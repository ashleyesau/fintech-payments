"""tests/test_repayments.py: Phase 5.3 loan repayments."""
from collections import Counter, defaultdict
from datetime import date, timedelta

import numpy as np
import pytest

from simulator import repayments
from simulator.models import LoanRepayment
from simulator.repayments import (
    BUCKETS,
    add_months,
    adjusted_matrix_row,
    bucket_of,
    due_date,
    first_strike_probability,
    forced_miss_probability,
    late_cure_probability,
    installment_amounts,
    retry_probability,
    simulate_repayments,
    start_schedule,
)
from simulator.state import Loan, Merchant

DISBURSED = date(2025, 6, 2)  # a Monday


def merchant(state="stable", risk=10, gpv=100_000) -> Merchant:
    m = Merchant(
        archetype="restaurant", quality_percentile=0.3, onboarded_at=date(2025, 1, 1),
        merchant_state=state, risk_score=risk,
    )
    for i in range(90):
        m.record_day(date(2025, 3, 1) + timedelta(days=i), gpv, 5)
    return m


def with_loan(state="stable", principal=2_000_000, term=6, pct=0.07, **kwargs) -> Merchant:
    m = merchant(state, **kwargs)
    loan = Loan(
        application_id=f"app-{state}", merchant_id=m.merchant_id, principal_minor=principal,
        term_months=term, total_cost_pct=pct, disbursed_at=DISBURSED,
        outstanding_principal_minor=principal,
    )
    start_schedule(loan, m)
    m.active_loan, m.has_active_loan, m.loan_count = loan, True, 1
    return m


def rolls(monkeypatch, *values: float) -> None:
    """np.random.random returns these in order; running out is a test bug."""
    it = iter(values)
    monkeypatch.setattr(np.random, "random", lambda: next(it))


# One attempt draws: forced-miss roll, DebiCheck roll, dispute roll.
SUCCESS = (0.99, 0.0, 0.99)
NSF = (0.99, 0.99)
DISPUTE = (0.99, 0.0, 0.0)


def due_day(m: Merchant, installment: int = 1) -> date:
    return due_date(m.active_loan, installment)


# ---------------------------------------------------------------------------
# Dates, buckets, amounts, schedule
# ---------------------------------------------------------------------------

def test_add_months_clamps_to_month_end():
    assert add_months(date(2025, 1, 31), 1) == date(2025, 2, 28)
    assert add_months(date(2024, 1, 31), 1) == date(2024, 2, 29)
    assert add_months(date(2025, 11, 15), 3) == date(2026, 2, 15)


def test_due_date_moves_off_weekends_and_holidays():
    youth_day = Loan(application_id="a", disbursed_at=date(2025, 5, 16), term_months=6)
    assert add_months(youth_day.disbursed_at, 1) == date(2025, 6, 16)  # Youth Day, a Monday
    assert due_date(youth_day, 1) == date(2025, 6, 17)
    saturday = Loan(application_id="b", disbursed_at=date(2025, 7, 9), term_months=6)
    assert add_months(saturday.disbursed_at, 1) == date(2025, 8, 9)  # a Saturday and Women's Day
    assert due_date(saturday, 1) == date(2025, 8, 11)
    ordinary = Loan(application_id="c", disbursed_at=DISBURSED, term_months=6)
    assert due_date(ordinary, 1) == date(2025, 7, 2)  # a Wednesday, unchanged


def test_due_dates_anchor_on_the_disbursement_day_not_the_previous_due_date():
    loan = Loan(application_id="c", disbursed_at=date(2025, 1, 31), term_months=6)
    assert [add_months(loan.disbursed_at, k) for k in (1, 2, 3)] == [
        date(2025, 2, 28), date(2025, 3, 31), date(2025, 4, 30),
    ]


@pytest.mark.parametrize("dpd, bucket", [
    (0, "current"), (1, "1-30"), (30, "1-30"), (31, "31-60"), (60, "31-60"),
    (61, "61-90"), (90, "61-90"), (91, "90+"), (149, "90+"),
])
def test_bucket_boundaries(dpd, bucket):
    assert bucket_of(dpd) == bucket


def test_installments_sum_to_principal_and_fee():
    m = with_loan(principal=1_000_001, pct=0.07)
    loan = m.active_loan
    parts = [installment_amounts(loan, k) for k in range(1, 7)]
    assert sum(p for p, _ in parts) == 1_000_001
    assert sum(i for _, i in parts) == loan.fee_minor == 70_000
    assert parts[0] == (166_666, 11_666)
    assert parts[-1][0] == 1_000_001 - 5 * 166_666  # the last instalment takes the remainder


def test_start_schedule_sets_balance_mandate_and_first_due_date():
    m = with_loan(principal=2_000_000, term=6, pct=0.07)
    loan = m.active_loan
    assert loan.fee_minor == 140_000 and loan.outstanding_balance_minor == 2_140_000
    assert loan.outstanding_principal_minor == 2_000_000
    assert loan.next_installment == 1 and loan.next_due_date == due_date(loan, 1)
    assert loan.mandate_id and loan.mandate_status == "ACCP"
    assert loan.baseline_gpv_30d_minor == 100_000


# ---------------------------------------------------------------------------
# Probabilities
# ---------------------------------------------------------------------------

def test_first_strike_probability_follows_state_modifiers():
    expected = {"growing": 0.847, "stable": 0.77, "stressed": 0.539, "delinquent": 0.308}
    for state, p in expected.items():
        assert first_strike_probability(merchant(state)) == pytest.approx(p)
    assert first_strike_probability(merchant("churned")) == 0.0


def test_stressed_first_strike_is_20_to_30_points_below_stable():
    gap = first_strike_probability(merchant("stable")) - first_strike_probability(merchant("stressed"))
    assert 0.20 <= gap <= 0.30


def test_three_retries_recover_half_of_failures_for_a_stable_merchant():
    p = retry_probability(merchant("stable"))
    assert 1 - (1 - p) ** 3 == pytest.approx(0.50, abs=0.005)


def test_cumulative_collection_for_a_stable_merchant():
    p1 = first_strike_probability(merchant("stable"))
    p = retry_probability(merchant("stable"))
    assert p1 + (1 - p1) * (1 - (1 - p) ** 3) == pytest.approx(0.885, abs=0.005)


def test_forced_miss_probability():
    assert forced_miss_probability(merchant("churned"), with_loan().active_loan) == 1.0
    m = with_loan("delinquent")
    assert forced_miss_probability(m, m.active_loan) == pytest.approx(0.5)
    assert forced_miss_probability(with_loan("stable"), with_loan("stable").active_loan) == 0.0


def dormant(m: Merchant, closed: bool) -> Merchant:
    m.dormant_since, m.business_closed = date(2025, 5, 1), closed
    return m


def test_a_merchant_that_moved_acquirer_repays_as_if_in_good_standing():
    for state in ("stressed", "delinquent", "churned"):
        m = dormant(with_loan(state), closed=False)
        assert first_strike_probability(m) == pytest.approx(0.77)
        assert forced_miss_probability(m, m.active_loan) == 0.0


def test_a_merchant_that_closed_stops_paying_as_soon_as_it_goes_dormant():
    m = dormant(with_loan("stable"), closed=True)
    assert forced_miss_probability(m, m.active_loan) == 1.0


def test_dormant_merchant_with_undecided_closure_is_judged_by_state():
    m = with_loan("delinquent")
    m.dormant_since = date(2025, 5, 1)
    assert forced_miss_probability(m, m.active_loan) == pytest.approx(0.5)


def test_moved_acquirer_merchants_cure_arrears_like_healthy_ones():
    assert late_cure_probability(dormant(merchant("churned"), closed=False)) == pytest.approx(
        late_cure_probability(merchant("stable"))
    )


def test_gpv_collapse_since_disbursement_forces_misses():
    m = with_loan("stable", gpv=100_000)
    for i in range(30):
        m.record_day(date(2025, 6, 1) + timedelta(days=i), 40_000, 5)  # down 60%
    assert forced_miss_probability(m, m.active_loan) == pytest.approx(0.30)
    m.merchant_state = "delinquent"
    assert forced_miss_probability(m, m.active_loan) == pytest.approx(0.80)


def test_a_30_percent_dip_is_not_a_collapse():
    m = with_loan("stable", gpv=100_000)
    for i in range(30):
        m.record_day(date(2025, 6, 1) + timedelta(days=i), 70_000, 5)
    assert forced_miss_probability(m, m.active_loan) == 0.0


@pytest.mark.parametrize("bucket", ["1-30", "31-60", "61-90", "90+"])
def test_matrix_rows_still_sum_to_one_after_adjustment(bucket):
    for modifier in (0.0, 0.4, 0.7, 1.0, 1.1):
        assert sum(adjusted_matrix_row(bucket, modifier).values()) == pytest.approx(1.0)


def test_neutral_modifier_leaves_the_calibration_row_unchanged():
    row = adjusted_matrix_row("1-30", 1.0)
    assert row == pytest.approx({"current": 0.40, "dpd_1_30": 0.30, "dpd_31_60": 0.30})


def test_state_scales_cure_and_keeps_other_proportions():
    row = adjusted_matrix_row("1-30", 0.7)
    assert row["current"] == pytest.approx(0.28)
    assert row["dpd_1_30"] / row["dpd_31_60"] == pytest.approx(1.0)
    assert adjusted_matrix_row("1-30", 0.0)["current"] == 0.0


def test_the_90_plus_row_has_no_cure_to_scale():
    assert adjusted_matrix_row("90+", 0.4) == pytest.approx(
        {"dpd_61_90": 0.03, "dpd_90_plus": 0.94, "write_off": 0.03}
    )


# ---------------------------------------------------------------------------
# Instalment collection on a current loan
# ---------------------------------------------------------------------------

def test_nothing_happens_before_the_first_due_date():
    m = with_loan()
    assert simulate_repayments(m, due_day(m) - timedelta(days=1)) == []
    assert simulate_repayments(Merchant(), DISBURSED) == []


def test_first_strike_success_row(monkeypatch):
    m = with_loan()
    day = due_day(m)
    rolls(monkeypatch, *SUCCESS)
    (row,) = simulate_repayments(m, day)
    LoanRepayment(**row)
    loan = m.active_loan
    assert row["loan_id"] == loan.application_id and row["installment_number"] == 1
    assert row["due_date"] == day and row["retry_attempt"] == 0 and row["days_past_due"] == 0
    assert row["collection_status"] == "success" and row["response_code"] == "00"
    assert row["amount_paid"] == row["total_due"] == row["principal_due"] + row["interest_due"]
    assert row["payment_ts"] is not None and row["payment_ts"].date() == day
    assert row["outstanding_before"] == 2_140_000
    assert row["outstanding_after"] == 2_140_000 - row["total_due"] == loan.outstanding_balance_minor
    assert (row["debit_method"], row["mandate_status"], row["par_bucket"]) == ("debicheck", "ACCP", "current")
    assert row["mandate_id"] == loan.mandate_id
    assert (row["is_cured"], row["write_off_flag"]) == (False, False)
    assert m.risk_score == 10 - 2
    assert loan.next_installment == 2 and loan.next_due_date == due_date(loan, 2)


def test_first_strike_failure_schedules_three_retries_inside_ten_days(monkeypatch):
    m = with_loan()
    day = due_day(m)
    rolls(monkeypatch, *NSF)
    (row,) = simulate_repayments(m, day)
    LoanRepayment(**row)
    assert row["collection_status"] == "unpaid_nsf" and row["response_code"] == "AM04"
    assert row["amount_paid"] == 0 and row["payment_ts"] is None
    assert row["outstanding_after"] == row["outstanding_before"]
    loan = m.active_loan
    assert len(loan.retry_dates) == 3 and loan.retrying_installment == 1
    assert all(day < d <= day + timedelta(days=10) for d in loan.retry_dates)
    assert all(d.weekday() < 5 for d in loan.retry_dates)
    assert loan.retry_dates == sorted(loan.retry_dates)
    assert m.risk_score == 10  # a missed first strike is not yet a missed instalment
    assert loan.unpaid_installments == [] and loan.bucket == "current"


def test_a_retry_that_succeeds_cures_the_instalment(monkeypatch):
    m = with_loan()
    day = due_day(m)
    rolls(monkeypatch, *NSF)
    simulate_repayments(m, day)
    retry_day = m.active_loan.retry_dates[0]
    rolls(monkeypatch, *SUCCESS)
    (row,) = simulate_repayments(m, retry_day)
    LoanRepayment(**row)
    assert (row["retry_attempt"], row["collection_status"], row["is_cured"]) == (1, "success", True)
    assert row["days_past_due"] == (retry_day - day).days and row["par_bucket"] == "1-30"
    loan = m.active_loan
    assert loan.retry_dates == [] and loan.retrying_installment is None and loan.bucket == "current"
    assert loan.outstanding_balance_minor == 2_140_000 - row["total_due"]
    assert m.risk_score == 8


def test_exhausted_retries_put_the_loan_in_arrears(monkeypatch):
    m = with_loan()
    day = due_day(m)
    rolls(monkeypatch, *NSF)
    simulate_repayments(m, day)
    rows = []
    for retry_day in list(m.active_loan.retry_dates):
        rolls(monkeypatch, *NSF)
        rows += simulate_repayments(m, retry_day)
    for row in rows:
        LoanRepayment(**row)
    assert [r["retry_attempt"] for r in rows] == [1, 2, 3]
    assert all(r["collection_status"] == "unpaid_nsf" for r in rows)
    loan = m.active_loan
    assert loan.unpaid_installments == [1] and loan.bucket == "1-30"
    assert loan.days_past_due == rows[-1]["days_past_due"]
    assert loan.retrying_installment is None and loan.retry_dates == []
    assert m.risk_score == 10 + 8 + 10  # missed instalment, then the bucket increase


def test_a_collection_that_would_succeed_can_be_disputed(monkeypatch):
    m = with_loan()
    rolls(monkeypatch, *DISPUTE)
    (row,) = simulate_repayments(m, due_day(m))
    LoanRepayment(**row)
    assert row["collection_status"] == "disputed" and row["response_code"] == "MD06"
    assert row["amount_paid"] == 0
    assert m.risk_score == 10 + 15


def test_forced_miss_fails_the_attempt_without_a_debicheck_draw(monkeypatch):
    m = with_loan("delinquent")  # forced miss probability 0.5
    rolls(monkeypatch, 0.2)       # below 0.5: forced miss, no further rolls consumed
    (row,) = simulate_repayments(m, due_day(m))
    assert row["collection_status"] == "unpaid_nsf"


def test_a_churned_merchant_never_pays(monkeypatch):
    m = with_loan("churned")
    rolls(monkeypatch, 0.99)  # forced probability is 1.0, so 0.99 still misses
    (row,) = simulate_repayments(m, due_day(m))
    assert row["collection_status"] == "unpaid_nsf"


def test_paying_every_instalment_repays_the_loan_in_full(monkeypatch):
    m = with_loan(principal=1_000_000, term=3, pct=0.06, risk=40)
    rows = []
    for _ in range(3):
        rolls(monkeypatch, *SUCCESS)
        rows += simulate_repayments(m, m.active_loan.next_due_date)
    assert [r["installment_number"] for r in rows] == [1, 2, 3]
    assert sum(r["principal_due"] for r in rows) == 1_000_000
    assert sum(r["interest_due"] for r in rows) == 60_000
    assert rows[-1]["outstanding_after"] == 0
    assert m.active_loan is None and m.has_active_loan is False
    assert m.prior_loan_repaid is True and m.is_written_off is False
    # -2 per instalment, -5 for the third in a row, -10 for repaying in full.
    assert m.risk_score == 40 - 2 - 2 - 2 - 5 - 10


def test_repaying_a_loan_doubles_later_application_propensity(monkeypatch):
    from simulator.lending import application_probability

    m = with_loan(principal=1_000_000, term=1, pct=0.05)
    m.months_on_platform = 6
    day = date(2026, 3, 10)
    rolls(monkeypatch, *SUCCESS)
    simulate_repayments(m, due_day(m))
    assert m.prior_loan_repaid and m.has_active_loan is False

    reference = merchant()
    reference.months_on_platform = 6
    assert application_probability(m, day) == pytest.approx(
        1 - (1 - 2 * 0.04) ** (1 / 30)
    )
    assert application_probability(m, day) > application_probability(reference, day)


# ---------------------------------------------------------------------------
# Late recovery: the month after the retries fail
# ---------------------------------------------------------------------------

def retries_exhausted(monkeypatch, term=6) -> Merchant:
    """A loan whose instalment 1 failed its first strike and all three retries."""
    m = with_loan(term=term)
    rolls(monkeypatch, *NSF)
    simulate_repayments(m, due_day(m))
    for retry_day in list(m.active_loan.retry_dates):
        rolls(monkeypatch, *NSF)
        simulate_repayments(m, retry_day)
    return m


def test_late_cure_probability_leaves_six_percent_unpaid_at_month_end():
    stable = late_cure_probability(merchant("stable"))
    # 11.5% of instalments are unpaid after retries; 6% must remain at month end.
    assert 0.115 * (1 - stable) == pytest.approx(0.06, abs=0.001)
    assert late_cure_probability(merchant("growing")) == pytest.approx(stable * 1.10)
    assert late_cure_probability(merchant("stressed")) == pytest.approx(stable * 0.70)
    assert late_cure_probability(merchant("churned")) == 0.0


def test_exhausted_retries_flag_the_loan_for_late_recovery(monkeypatch):
    m = retries_exhausted(monkeypatch)
    assert m.active_loan.late_recovery_due is True
    assert m.active_loan.bucket == "1-30" and m.active_loan.unpaid_installments == [1]


def test_paying_late_clears_the_arrears_and_collects_the_new_instalment(monkeypatch):
    m = retries_exhausted(monkeypatch)
    loan = m.active_loan
    risk_before = m.risk_score
    rolls(monkeypatch, 0.0, *SUCCESS)  # late cure draw, then the new instalment's first strike
    rows = simulate_repayments(m, loan.next_due_date)
    for r in rows:
        LoanRepayment(**r)
    assert [(r["installment_number"], r["collection_status"], r["is_cured"]) for r in rows] == [
        (1, "success", True), (2, "success", False),
    ]
    assert loan.bucket == "current" and loan.days_past_due == 0 and loan.unpaid_installments == []
    assert loan.late_recovery_due is False and loan.next_installment == 3
    assert m.risk_score == risk_before - 2 - 5 - 2


def test_paying_late_still_leaves_the_new_instalment_exposed_to_failure(monkeypatch):
    m = retries_exhausted(monkeypatch)
    rolls(monkeypatch, 0.0, *NSF)
    rows = simulate_repayments(m, m.active_loan.next_due_date)
    assert [(r["installment_number"], r["collection_status"]) for r in rows] == [(1, "success"), (2, "unpaid_nsf")]
    assert m.active_loan.retrying_installment == 2 and m.active_loan.bucket == "current"


def test_still_unpaid_at_month_end_is_1_to_30_dpd_without_a_matrix_step(monkeypatch):
    m = retries_exhausted(monkeypatch)
    loan = m.active_loan
    rolls(monkeypatch, 0.99)
    rows = simulate_repayments(m, loan.next_due_date)
    assert [(r["installment_number"], r["collection_status"]) for r in rows] == [(2, "unpaid_nsf")]
    assert loan.unpaid_installments == [1, 2] and loan.bucket == "1-30"
    assert loan.late_recovery_due is False
    assert 28 <= loan.days_past_due <= 30 and rows[0]["days_past_due"] == loan.days_past_due


def test_late_recovery_after_the_term_has_no_new_instalment(monkeypatch):
    m = retries_exhausted(monkeypatch, term=1)
    loan = m.active_loan
    assert loan.next_installment == 2
    rolls(monkeypatch, 0.99)
    assert simulate_repayments(m, loan.next_due_date) == []
    assert loan.bucket == "1-30" and loan.late_recovery_due is False


def test_the_matrix_governs_the_cycle_after_late_recovery(monkeypatch):
    m = retries_exhausted(monkeypatch)
    loan = m.active_loan
    rolls(monkeypatch, 0.99)
    simulate_repayments(m, loan.next_due_date)
    force_outcome(monkeypatch, "dpd_31_60")
    simulate_repayments(m, loan.next_due_date)
    assert loan.bucket == "31-60" and loan.unpaid_installments == [1, 2, 3]


# ---------------------------------------------------------------------------
# Loans in arrears
# ---------------------------------------------------------------------------

def in_arrears(bucket="1-30", dpd=20, term=6, unpaid=(1,), state="stable") -> Merchant:
    """A loan whose instalments 1..len(unpaid) are unpaid, with instalment 2 now due."""
    m = with_loan(state, term=term)
    loan = m.active_loan
    loan.unpaid_installments = list(unpaid)
    loan.bucket, loan.days_past_due = bucket, dpd
    loan.next_installment = len(unpaid) + 1
    loan.next_due_date = due_date(loan, loan.next_installment)
    return m


def force_outcome(monkeypatch, key: str) -> None:
    monkeypatch.setattr(repayments, "_sample", lambda row: key)


def test_cure_pays_the_arrears_and_the_new_instalment(monkeypatch):
    m = in_arrears("1-30", 20)
    force_outcome(monkeypatch, "current")
    rows = simulate_repayments(m, m.active_loan.next_due_date)
    for r in rows:
        LoanRepayment(**r)
    assert [(r["installment_number"], r["is_cured"]) for r in rows] == [(1, True), (2, False)]
    assert all(r["collection_status"] == "success" for r in rows)
    loan = m.active_loan
    assert loan.bucket == "current" and loan.days_past_due == 0 and loan.unpaid_installments == []
    assert loan.next_installment == 3
    # Two instalments paid (-2 each) and one bucket recovered (-5).
    assert m.risk_score == 10 - 2 - 2 - 5


def test_staying_in_the_bucket_pays_the_oldest_and_misses_the_new_instalment(monkeypatch):
    m = in_arrears("1-30", 20)
    force_outcome(monkeypatch, "dpd_1_30")
    rows = simulate_repayments(m, m.active_loan.next_due_date)
    assert [(r["installment_number"], r["collection_status"]) for r in rows] == [(1, "success"), (2, "unpaid_nsf")]
    loan = m.active_loan
    assert loan.unpaid_installments == [2] and loan.bucket == "1-30"
    assert 1 <= loan.days_past_due <= 30 and rows[1]["days_past_due"] == loan.days_past_due
    assert rows[0]["is_cured"] is False


def test_rolling_forward_adds_a_second_unpaid_instalment(monkeypatch):
    m = in_arrears("1-30", 20)
    force_outcome(monkeypatch, "dpd_31_60")
    rows = simulate_repayments(m, m.active_loan.next_due_date)
    assert [(r["installment_number"], r["collection_status"]) for r in rows] == [(2, "unpaid_nsf")]
    loan = m.active_loan
    assert loan.unpaid_installments == [1, 2] and loan.bucket == "31-60" and loan.days_past_due == 50
    assert rows[0]["par_bucket"] == "31-60"
    assert m.risk_score == 10 + 8 + 10  # the miss and one bucket step


def test_improving_a_bucket_pays_the_oldest_and_lowers_risk(monkeypatch):
    m = in_arrears("31-60", 45, unpaid=(1, 2))
    force_outcome(monkeypatch, "dpd_1_30")  # not a key in this row, but sample is forced
    rows = simulate_repayments(m, m.active_loan.next_due_date)
    assert rows[0]["installment_number"] == 1 and rows[0]["collection_status"] == "success"
    assert m.active_loan.bucket == "1-30"


def test_90_plus_staying_keeps_ageing_by_a_month(monkeypatch):
    m = in_arrears("90+", 100, unpaid=(1, 2))
    force_outcome(monkeypatch, "dpd_90_plus")
    rows = simulate_repayments(m, m.active_loan.next_due_date)
    assert [r["collection_status"] for r in rows] == ["unpaid_nsf"]
    assert m.active_loan.days_past_due == 130 and m.active_loan.unpaid_installments == [1, 2, 3]


def test_reaching_150_dpd_writes_the_loan_off(monkeypatch):
    m = in_arrears("90+", 125, unpaid=(1, 2))
    force_outcome(monkeypatch, "dpd_90_plus")  # ageing 125 + 30 = 155 crosses the threshold
    rows = simulate_repayments(m, m.active_loan.next_due_date)
    assert len(rows) == 1
    LoanRepayment(**rows[0])
    row = rows[0]
    assert row["write_off_flag"] is True and row["installment_number"] == 1
    assert row["days_past_due"] >= 150 and row["par_bucket"] == "90+" and row["amount_paid"] == 0
    assert m.is_written_off and m.active_loan is None and m.has_active_loan is False
    assert m.risk_score >= 80


def test_the_matrix_can_write_a_loan_off_directly(monkeypatch):
    m = in_arrears("90+", 100, unpaid=(1, 2))
    force_outcome(monkeypatch, "write_off")
    (row,) = simulate_repayments(m, m.active_loan.next_due_date)
    assert row["write_off_flag"] is True and row["days_past_due"] == 150
    assert m.is_written_off and m.active_loan is None


def test_a_written_off_merchant_never_applies_again():
    from simulator.lending import eligibility_failures

    m = in_arrears("90+", 100, unpaid=(1, 2))
    m.write_off(date(2026, 1, 1))
    assert "prior_write_off" in eligibility_failures(m)


def test_arrears_after_the_term_ends_still_cure(monkeypatch):
    m = in_arrears("1-30", 20, term=2, unpaid=(1, 2))   # both instalments unpaid, term over
    loan = m.active_loan
    assert loan.next_installment == 3
    force_outcome(monkeypatch, "current")
    rows = simulate_repayments(m, loan.next_due_date)
    assert [r["installment_number"] for r in rows] == [1, 2]
    assert m.active_loan is None and m.prior_loan_repaid


def test_arrears_after_the_term_with_no_cure_emit_no_rows(monkeypatch):
    m = in_arrears("1-30", 20, term=1, unpaid=(1,))
    force_outcome(monkeypatch, "dpd_31_60")
    assert simulate_repayments(m, m.active_loan.next_due_date) == []
    assert m.active_loan.bucket == "31-60"


def test_outstanding_balance_never_goes_negative_or_up(monkeypatch):
    m = in_arrears("1-30", 20)
    force_outcome(monkeypatch, "current")
    before = m.active_loan.outstanding_balance_minor
    for r in simulate_repayments(m, m.active_loan.next_due_date):
        assert 0 <= r["outstanding_after"] <= r["outstanding_before"] <= before


# ---------------------------------------------------------------------------
# Population: Validation Targets collection bands
# ---------------------------------------------------------------------------

def population():
    np.random.seed(5)
    states = ["growing"] * 130 + ["stable"] * 200 + ["stressed"] * 50 + ["delinquent"] * 20
    merchants = [(s, with_loan(s)) for s in states]
    rows = defaultdict(list)
    for offset in range(540):
        day = DISBURSED + timedelta(days=offset)
        for state, m in merchants:
            for r in simulate_repayments(m, day):
                rows[(state, m.merchant_id)].append(r)
    return merchants, rows


@pytest.fixture(scope="module")
def population_run():
    return population()


def first_strikes(rows, states=None):
    return [
        r for (s, _), v in rows.items() if states is None or s in states
        for r in v if r["retry_attempt"] == 0 and r["days_past_due"] == 0 and not r["is_cured"]
    ]


def test_every_population_row_validates(population_run):
    _, rows = population_run
    ids = set()
    for v in rows.values():
        for r in v:
            LoanRepayment(**r)
            assert r["repayment_id"] not in ids
            ids.add(r["repayment_id"])
    assert len(ids) > 3000


def test_first_strike_success_rate_is_in_the_validation_band(population_run):
    _, rows = population_run
    attempts = first_strikes(rows)
    rate = sum(r["collection_status"] == "success" for r in attempts) / len(attempts)
    assert 0.72 <= rate <= 0.82


def test_retry_recovery_and_cumulative_collection_are_in_their_bands(population_run):
    _, rows = population_run
    failed = recovered = collected = total = 0
    for (state, _), v in rows.items():
        if state not in ("growing", "stable"):
            continue
        by_installment = defaultdict(list)
        for r in v:
            by_installment[r["installment_number"]].append(r)
        for rs in by_installment.values():
            first = [r for r in rs if r["retry_attempt"] == 0 and r["days_past_due"] == 0 and not r["is_cured"]]
            if not first:
                continue
            total += 1
            ok_first = first[0]["collection_status"] == "success"
            ok_retry = any(r["collection_status"] == "success" and 1 <= r["retry_attempt"] <= 3 for r in rs)
            collected += ok_first or ok_retry
            if not ok_first:
                failed += 1
                recovered += ok_retry
    assert 0.45 <= recovered / failed <= 0.55
    assert 0.85 <= collected / total <= 0.92


def test_weaker_merchant_states_collect_less(population_run):
    _, rows = population_run

    def rate(state):
        attempts = first_strikes(rows, {state})
        return sum(r["collection_status"] == "success" for r in attempts) / len(attempts)

    assert rate("growing") > rate("stable") > rate("stressed") > rate("delinquent")


def test_loans_end_and_merchants_are_marked(population_run):
    merchants, _ = population_run
    for _, m in merchants:
        assert m.active_loan is None  # 540 days outlasts a 6-month loan plus arrears
        assert m.has_active_loan is False
        assert m.prior_loan_repaid != m.is_written_off  # repaid in full or written off, never both


def test_risk_scores_stay_in_range_and_write_offs_hold_the_floor(population_run):
    merchants, _ = population_run
    for _, m in merchants:
        assert 0 <= m.risk_score <= 100
        if m.is_written_off:
            assert m.risk_score >= 80
