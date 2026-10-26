"""
repayments.py: Loan repayment events (Stream 3).

A disbursed loan is repaid in equal monthly instalments by DebiCheck. How an
instalment is collected depends on the loan's delinquency bucket:

  - Current loans: a first strike on the due date at 0.77 x the merchant's state
    modifier, then up to three retries inside 10 days. Forced misses (a Delinquent or
    Churned merchant, or a collapse in GPV since disbursement) fail the attempt
    regardless of the DebiCheck draw.
  - Loans in arrears: an instalment still unpaid after its retries gets one month to be
    paid late; if it is still unpaid at month end the loan is 1-30 DPD (the matrix's
    Current to 1-30 step). From then it moves month by month through the Chain 4 DPD
    matrix, with the cure probability scaled by the merchant's state. Cure pays the arrears; staying or improving pays the oldest unpaid
    instalment; rolling forward leaves the new instalment unpaid too.
  - At 150 DPD (or a write-off draw from the 90+ row) the oldest unpaid instalment is
    written off and the loan ends.

Every collection attempt is one loan_repayments row. Outcomes feed the merchant's risk
score through Merchant.record_repayment_outcome() and apply_risk_delta().
"""
from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta
from typing import Any, Optional

import numpy as np

from simulator.config import load_causal_rules
from simulator.common import SAST, new_id
from simulator.payout import next_business_day
from simulator.state import Loan, risk_delta

_RULES = load_causal_rules()
_DEBI = _RULES["debicheck"]
_FORCED = _RULES["forced_missed"]
_DPD = _RULES["dpd"]
_CODES = _DEBI["response_codes"]

BUCKETS = ["current", "1-30", "31-60", "61-90", "90+"]
_MATRIX_KEY = {
    "current": "current", "1-30": "dpd_1_30", "31-60": "dpd_31_60",
    "61-90": "dpd_61_90", "90+": "dpd_90_plus",
}
_BUCKET_OF_KEY = {v: k for k, v in _MATRIX_KEY.items()}
_BUCKET_RANGE = {"1-30": (1, 30), "31-60": (31, 60), "61-90": (61, 90), "90+": (91, 149)}
_MONTH_STEP_DAYS = 30


# ---------------------------------------------------------------------------
# Dates, buckets, amounts
# ---------------------------------------------------------------------------

def bucket_of(days_past_due: int) -> str:
    if days_past_due <= 0:
        return "current"
    if days_past_due <= 30:
        return "1-30"
    if days_past_due <= 60:
        return "31-60"
    if days_past_due <= 90:
        return "61-90"
    return "90+"


def add_months(anchor: date, months: int) -> date:
    index = anchor.year * 12 + anchor.month - 1 + months
    year, month = divmod(index, 12)
    month += 1
    return date(year, month, min(anchor.day, calendar.monthrange(year, month)[1]))


def due_date(loan: Loan, installment: int) -> date:
    """Instalment k falls due k months after disbursement, on the next business day
    because DebiCheck does not clear on weekends or public holidays."""
    return next_business_day(add_months(loan.disbursed_at, installment))


def installment_amounts(loan: Loan, installment: int) -> tuple[int, int]:
    """(principal_due, interest_due) in cents. Equal parts; the last instalment absorbs
    the rounding remainder."""
    n = loan.term_months
    principal, fee = loan.principal_minor // n, loan.fee_minor // n
    if installment == n:
        principal = loan.principal_minor - (n - 1) * principal
        fee = loan.fee_minor - (n - 1) * fee
    return principal, fee


def start_schedule(loan: Loan, merchant: Any) -> None:
    """Initialise repayment bookkeeping when the loan is disbursed."""
    loan.fee_minor = int(round(loan.principal_minor * loan.total_cost_pct))
    loan.outstanding_balance_minor = loan.principal_minor + loan.fee_minor
    loan.outstanding_principal_minor = loan.principal_minor
    loan.baseline_gpv_30d_minor = merchant.gpv_30d_avg_minor()
    loan.next_installment = 1
    loan.next_due_date = due_date(loan, 1)
    loan.mandate_id = loan.mandate_id or new_id()
    loan.mandate_status = "ACCP"


# ---------------------------------------------------------------------------
# Probabilities
# ---------------------------------------------------------------------------

def _moved_acquirer(merchant: Any) -> bool:
    """A dormant merchant that did not close: its GPV has left, but it is still paying."""
    return merchant.dormant_since is not None and merchant.business_closed is False


def _state_modifier(merchant: Any) -> float:
    """DebiCheck success modifier by merchant state at the attempt. A Churned merchant
    has gone dark and never pays; states Causal Rules does not list count as neutral.
    A merchant that moved to another acquirer repays as a merchant in good standing,
    whatever state its vanished GPV has pushed it into."""
    if _moved_acquirer(merchant):
        return 1.0
    state = merchant.merchant_state
    if state == "churned":
        return 0.0
    return _DEBI["state_modifiers"].get(state, 1.0)


def forced_miss_probability(merchant: Any, loan: Loan) -> float:
    """Chance the attempt fails whatever the DebiCheck draw says (Causal Rules Chain 3)."""
    if _moved_acquirer(merchant):
        return 0.0
    if merchant.merchant_state == "churned" or merchant.business_closed:
        return _FORCED["churned_p"]
    p = 0.0
    if merchant.merchant_state == "delinquent":
        p += _FORCED["delinquent_p_increment"]
    baseline = loan.baseline_gpv_30d_minor
    if baseline > 0:
        decline = (baseline - merchant.gpv_30d_avg_minor()) / baseline
        if decline >= _FORCED["gpv_30d_decline_from_disbursement"]:
            p += _FORCED["gpv_decline_p_increment"]
    return min(p, 1.0)


def first_strike_probability(merchant: Any) -> float:
    return min(_DEBI["first_strike_base_success_rate"] * _state_modifier(merchant), 1.0)


def retry_probability(merchant: Any) -> float:
    return min(_DEBI["retry_attempt_success_rate"] * _state_modifier(merchant), 1.0)


def late_cure_probability(merchant: Any) -> float:
    """Chance an instalment that is still unpaid after its retries is paid before month end.

    The Chain 4 matrix counts a loan as 1-30 DPD only if it is still unpaid at month end:
    6% of current loans. After the retries, 11.5% of instalments are unpaid (for a
    neutral merchant), so about 48% must be paid in the rest of the month for 6% to
    remain. That share is scaled by the merchant's state like every other cure.
    """
    first = _DEBI["first_strike_base_success_rate"]
    retry = _DEBI["retry_attempt_success_rate"]
    unpaid_after_retries = (1 - first) * (1 - retry) ** _DEBI["max_retries"]
    month_end_share = _DPD["transition_matrix"]["current"]["dpd_1_30"]
    base_cure = max(0.0, 1.0 - month_end_share / unpaid_after_retries)
    return min(1.0, base_cure * _state_modifier(merchant))


def adjusted_matrix_row(bucket: str, modifier: float) -> dict[str, float]:
    """The Chain 4 row for `bucket` with the cure probability scaled by the merchant's
    state; the other outcomes share what remains in their original proportions."""
    raw = _DPD["transition_matrix"][_MATRIX_KEY[bucket]]
    cure = raw.get("current", 0.0)
    new_cure = min(1.0, cure * modifier)
    scale = (1.0 - new_cure) / (1.0 - cure) if cure < 1.0 else 0.0
    return {k: (new_cure if k == "current" else v * scale) for k, v in raw.items() if v > 0 or k == "current"}


def _sample(row: dict[str, float]) -> str:
    keys = list(row)
    cumulative = np.cumsum([row[k] for k in keys])
    index = int(np.searchsorted(cumulative, np.random.random() * cumulative[-1], side="right"))
    return keys[min(index, len(keys) - 1)]


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------

def _attempt_time(day: date) -> datetime:
    return datetime(
        day.year, day.month, day.day, int(np.random.randint(6, 9)),
        int(np.random.randint(0, 60)), int(np.random.randint(0, 60)), tzinfo=SAST,
    )


def _row(
    merchant: Any, loan: Loan, installment: int, due: date, day: date, *, paid: bool,
    retry_attempt: int = 0, dpd: int = 0, cured: bool = False, written_off: bool = False,
    status: Optional[str] = None, response: Optional[str] = None,
) -> dict:
    """Build one loan_repayments row and apply it to the loan's balances."""
    principal, interest = installment_amounts(loan, installment)
    total = principal + interest
    before = loan.outstanding_balance_minor
    if paid:
        loan.outstanding_balance_minor -= total
        loan.outstanding_principal_minor -= principal
    return {
        "repayment_id": new_id(),
        "loan_id": loan.loan_id,
        "merchant_id": merchant.merchant_id,
        "installment_number": installment,
        "due_date": due,
        "payment_ts": _attempt_time(day) if paid else None,
        "principal_due": principal,
        "interest_due": interest,
        "total_due": total,
        "amount_paid": total if paid else 0,
        "outstanding_before": before,
        "outstanding_after": loan.outstanding_balance_minor,
        "debit_method": "debicheck",
        "mandate_id": loan.mandate_id,
        "mandate_status": loan.mandate_status,
        "collection_status": status or ("success" if paid else "unpaid_nsf"),
        "response_code": response or (_CODES["success"] if paid else _CODES["insufficient_funds"]),
        "retry_attempt": retry_attempt,
        "days_past_due": dpd,
        "par_bucket": bucket_of(dpd),
        "is_cured": cured,
        "write_off_flag": written_off,
    }


# ---------------------------------------------------------------------------
# Closing a loan
# ---------------------------------------------------------------------------

def _repaid_in_full(merchant: Any, loan: Loan, sim_date: date) -> None:
    merchant.apply_risk_delta(risk_delta("loan_repaid_in_full"), sim_date)
    merchant.prior_loan_repaid = True
    merchant.has_active_loan = False
    merchant.active_loan = None


def _finished(loan: Loan) -> bool:
    return loan.next_installment > loan.term_months and not loan.unpaid_installments


def _write_off(merchant: Any, loan: Loan, sim_date: date) -> dict:
    """Write off the oldest unpaid instalment and end the loan."""
    oldest = loan.unpaid_installments[0]
    row = _row(
        merchant, loan, oldest, due_date(loan, oldest), sim_date, paid=False,
        dpd=max(loan.days_past_due, _DPD["write_off_dpd"]), written_off=True,
    )
    loan.is_written_off = True
    merchant.write_off(sim_date)
    merchant.has_active_loan = False
    merchant.active_loan = None
    return row


# ---------------------------------------------------------------------------
# Collecting one instalment
# ---------------------------------------------------------------------------

def _attempt_succeeds(merchant: Any, loan: Loan, probability: float) -> tuple[bool, bool]:
    """(collected, disputed). A forced miss fails before the DebiCheck draw; a collection
    that would have succeeded is disputed at the DebiCheck dispute rate."""
    if np.random.random() < forced_miss_probability(merchant, loan):
        return False, False
    if np.random.random() >= probability:
        return False, False
    if np.random.random() < _DEBI["dispute_rate"]:
        return False, True
    return True, False


def _failed_row(merchant: Any, loan: Loan, installment: int, due: date, day: date,
                disputed: bool, retry_attempt: int, dpd: int) -> dict:
    if disputed:
        merchant.apply_risk_delta(risk_delta("repayment_disputed_md06"), day)
        return _row(merchant, loan, installment, due, day, paid=False, retry_attempt=retry_attempt,
                    dpd=dpd, status="disputed", response=_CODES["disputed"])
    return _row(merchant, loan, installment, due, day, paid=False, retry_attempt=retry_attempt, dpd=dpd)


def _collect_current(merchant: Any, loan: Loan, installment: int, sim_date: date) -> list[dict]:
    """First strike on an instalment of a loan that is up to date."""
    due = loan.next_due_date
    collected, disputed = _attempt_succeeds(merchant, loan, first_strike_probability(merchant))
    if collected:
        row = _row(merchant, loan, installment, due, sim_date, paid=True)
        merchant.record_repayment_outcome(True, sim_date)
        return [row]
    # Retries on business days after the due date; the instalment is not yet counted as missed.
    loan.retrying_installment = installment
    loan.retry_dates = [
        next_business_day(due + timedelta(days=offset)) for offset in _DEBI["retry_day_offsets"]
    ][: _DEBI["max_retries"]]
    return [_failed_row(merchant, loan, installment, due, sim_date, disputed, retry_attempt=0, dpd=0)]


def _run_retry(merchant: Any, loan: Loan, sim_date: date) -> list[dict]:
    installment = loan.retrying_installment
    attempt = _DEBI["max_retries"] - len(loan.retry_dates) + 1
    loan.retry_dates.pop(0)
    due = due_date(loan, installment)
    dpd = (sim_date - due).days
    collected, disputed = _attempt_succeeds(merchant, loan, retry_probability(merchant))

    if collected:
        row = _row(merchant, loan, installment, due, sim_date, paid=True, retry_attempt=attempt,
                   dpd=dpd, cured=True)
        loan.retrying_installment, loan.retry_dates = None, []
        merchant.record_repayment_outcome(True, sim_date)
        if _finished(loan):
            _repaid_in_full(merchant, loan, sim_date)
        return [row]

    row = _failed_row(merchant, loan, installment, due, sim_date, disputed, attempt, dpd)
    if not loan.retry_dates:
        # Retries exhausted: the instalment is unpaid and the loan enters arrears.
        loan.retrying_installment = None
        loan.unpaid_installments.append(installment)
        loan.days_past_due = dpd
        loan.bucket = bucket_of(dpd)
        loan.late_recovery_due = True
        merchant.record_repayment_outcome(False, sim_date)
        merchant.apply_risk_delta(risk_delta("dpd_bucket_increase"), sim_date)
    return [row]


def _next_dpd(old: str, new: str, dpd: int) -> int:
    if new == "current":
        return 0
    if BUCKETS.index(new) > BUCKETS.index(old) or (new == "90+" and old == "90+"):
        return dpd + _MONTH_STEP_DAYS
    low, high = _BUCKET_RANGE[new]
    return int(np.random.randint(low, high + 1))


def _late_recovery(merchant: Any, loan: Loan, installment: Optional[int], sim_date: date) -> list[dict]:
    """The first monthly cycle after an instalment's retries failed. Either the instalment is
    paid late (the loan is current again) or it is still unpaid at month end, which is the
    Chain 4 Current to 1-30 transition, and the loan stays in the 1-30 bucket."""
    loan.late_recovery_due = False
    rows: list[dict] = []
    if np.random.random() < late_cure_probability(merchant):
        oldest = loan.unpaid_installments.pop(0)
        rows.append(_row(merchant, loan, oldest, due_date(loan, oldest), sim_date, paid=True,
                         dpd=loan.days_past_due, cured=True))
        merchant.record_repayment_outcome(True, sim_date)
        merchant.apply_risk_delta(risk_delta("dpd_bucket_cure"), sim_date)
        loan.bucket, loan.days_past_due = "current", 0
        if installment is not None:
            rows += _collect_current(merchant, loan, installment, sim_date)
        return rows

    oldest = loan.unpaid_installments[0]
    loan.days_past_due = min((sim_date - due_date(loan, oldest)).days, 30)
    loan.bucket = bucket_of(loan.days_past_due)
    if installment is not None:
        loan.unpaid_installments.append(installment)
        rows.append(_row(merchant, loan, installment, loan.next_due_date, sim_date, paid=False,
                         dpd=loan.days_past_due))
        merchant.record_repayment_outcome(False, sim_date)
    return rows


def _arrears_step(merchant: Any, loan: Loan, installment: Optional[int], sim_date: date) -> list[dict]:
    """A monthly cycle for a loan in arrears. `installment` is the instalment falling due
    now, or None once the term has ended and only arrears remain."""
    rows: list[dict] = []
    old_bucket, old_dpd = loan.bucket, loan.days_past_due
    outcome = _sample(adjusted_matrix_row(old_bucket, _state_modifier(merchant)))

    if outcome == "write_off":
        return [_write_off(merchant, loan, sim_date)]

    new_bucket = "current" if outcome == "current" else _BUCKET_OF_KEY[outcome]
    new_dpd = _next_dpd(old_bucket, new_bucket, old_dpd)
    if new_dpd >= _DPD["write_off_dpd"]:
        loan.days_past_due = new_dpd
        return [_write_off(merchant, loan, sim_date)]

    def pay_oldest(cured: bool) -> None:
        oldest = loan.unpaid_installments.pop(0)
        due = due_date(loan, oldest)
        rows.append(_row(merchant, loan, oldest, due, sim_date, paid=True, dpd=old_dpd, cured=cured))
        merchant.record_repayment_outcome(True, sim_date)

    if new_bucket == "current":
        while loan.unpaid_installments:
            pay_oldest(cured=True)
        if installment is not None:
            rows.append(_row(merchant, loan, installment, loan.next_due_date, sim_date, paid=True))
            merchant.record_repayment_outcome(True, sim_date)
        merchant.apply_risk_delta(risk_delta("dpd_bucket_cure") * BUCKETS.index(old_bucket), sim_date)
    else:
        # Staying in a bucket below 90+ or improving a bucket pays the oldest unpaid
        # instalment; rolling forward, or staying in 90+, pays nothing.
        old_index, new_index = BUCKETS.index(old_bucket), BUCKETS.index(new_bucket)
        if new_index < old_index or (new_index == old_index and new_bucket != "90+"):
            pay_oldest(cured=False)
        if installment is not None:
            loan.unpaid_installments.append(installment)
            rows.append(_row(merchant, loan, installment, loan.next_due_date, sim_date, paid=False, dpd=new_dpd))
            merchant.record_repayment_outcome(False, sim_date)
        step = BUCKETS.index(new_bucket) - BUCKETS.index(old_bucket)
        if step > 0:
            merchant.apply_risk_delta(risk_delta("dpd_bucket_increase") * step, sim_date)
        elif step < 0:
            merchant.apply_risk_delta(risk_delta("dpd_bucket_cure") * -step, sim_date)

    loan.bucket, loan.days_past_due = new_bucket, new_dpd
    return rows


# ---------------------------------------------------------------------------
# One merchant, one day
# ---------------------------------------------------------------------------

def simulate_repayments(merchant: Any, sim_date: date) -> list[dict]:
    """
    loan_repayments rows for the merchant's active loan on sim_date: any retry that falls
    due, and the instalment or arrears cycle due today. Returns [] when there is no loan.
    """
    loan: Optional[Loan] = merchant.active_loan
    if loan is None or loan.is_written_off or loan.next_due_date is None:
        return []

    rows: list[dict] = []
    if loan.retry_dates and loan.retry_dates[0] <= sim_date:
        rows += _run_retry(merchant, loan, sim_date)
        if merchant.active_loan is None:
            return rows

    if loan.next_due_date <= sim_date:
        k = loan.next_installment
        installment = k if k <= loan.term_months else None
        if loan.bucket == "current" and installment is not None:
            rows += _collect_current(merchant, loan, installment, sim_date)
        elif loan.unpaid_installments and loan.late_recovery_due:
            rows += _late_recovery(merchant, loan, installment, sim_date)
        elif loan.unpaid_installments:
            rows += _arrears_step(merchant, loan, installment, sim_date)
        if merchant.active_loan is not None:
            loan.next_installment = k + 1
            loan.next_due_date = due_date(loan, k + 1)
            if _finished(loan) and loan.retrying_installment is None:
                _repaid_in_full(merchant, loan, sim_date)
    return rows
