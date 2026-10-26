"""
lending.py: Loan application and disbursement events.

simulate_lending_day() is the lending counterpart of generator.simulate_day(): for one
merchant on one day it returns the loan_applications rows to INSERT and the
disbursement UPDATEs that fall due. Repayments are generated separately (Phase 5.3).

A loan is its application in V1, so a disbursed loan carries the application's id.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Optional

import numpy as np

from simulator.config import load_causal_rules, load_lending
from simulator.generator import SAST, new_id
from simulator.state import Loan

_RULES = load_causal_rules()
_ELIGIBILITY = _RULES["lending_eligibility"]
_PROPENSITY = _RULES["application_propensity"]
_BANDS = _RULES["loan_approval"]
_LEND = load_lending()


# ---------------------------------------------------------------------------
# Sampling helpers
# ---------------------------------------------------------------------------

def _choose(dist: dict) -> Any:
    keys = list(dist)
    weights = np.array(list(dist.values()), dtype=float)
    return keys[int(np.searchsorted(np.cumsum(weights), np.random.random() * weights.sum(), side="right"))]


def _daily(monthly_probability: float) -> float:
    return 1.0 - (1.0 - min(monthly_probability, 1.0)) ** (1.0 / 30.0)


def _uniform(bounds: list) -> float:
    return float(np.random.uniform(bounds[0], bounds[1]))


def _randint(bounds: list) -> int:
    return int(np.random.randint(bounds[0], bounds[1] + 1))


def _round_down(amount_minor: float) -> int:
    step = _LEND["amount_step_minor"]
    return int(amount_minor // step) * step


def _time_on(day: date, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=SAST)


# ---------------------------------------------------------------------------
# Who applies
# ---------------------------------------------------------------------------

def _in_market(merchant: Any) -> bool:
    """Merchants that are trading and could plausibly apply."""
    return (
        merchant.first_transaction_at is not None
        and merchant.dormant_since is None
        and merchant.merchant_state != "churned"
    )


def eligibility_failures(merchant: Any) -> list[str]:
    """Decline reasons for every Chain 3 gate the merchant fails; empty means eligible."""
    reasons = []
    if merchant.months_on_platform < _ELIGIBILITY["min_months_on_platform"]:
        reasons.append("insufficient_trading_history")
    if merchant.gpv_90d_minor() < _ELIGIBILITY["min_gpv_90d_minor"]:
        reasons.append("low_volume")
    if merchant.merchant_state not in _ELIGIBILITY["eligible_states"]:
        reasons.append(
            "adverse_merchant_state"
            if merchant.merchant_state in ("stressed", "delinquent")
            else "merchant_not_established"
        )
    if merchant.has_active_loan:
        reasons.append("existing_loan_outstanding")
    if merchant.is_written_off:
        reasons.append("prior_write_off")
    return reasons


def application_probability(merchant: Any, sim_date: date) -> float:
    """Chance that the merchant applies today. Zero outside the market or inside the cooldown."""
    if not _in_market(merchant):
        return 0.0
    last = merchant.last_application_date
    if last is not None and (sim_date - last).days < _LEND["application_cooldown_days"]:
        return 0.0

    if eligibility_failures(merchant):
        monthly = _LEND["ineligible_monthly_application_probability"]
    else:
        monthly = _PROPENSITY[merchant.merchant_state]
        if merchant.prior_loan_repaid:
            monthly *= _PROPENSITY["prior_loan_repaid_multiplier"]
    monthly *= _LEND["seasonal_application_multiplier"].get(sim_date.month, 1.0)
    return _daily(monthly)


# ---------------------------------------------------------------------------
# The application and its decision
# ---------------------------------------------------------------------------

def _bureau(merchant: Any) -> tuple[int, str]:
    cfg = _LEND["bureau"]
    score = (
        cfg["base"]
        + cfg["quality_weight"] * merchant.quality_percentile
        - cfg["risk_penalty"] * (merchant.risk_score - 10)
        + np.random.normal(0, cfg["noise_std"])
    )
    return int(max(100, min(999, round(score)))), _choose(cfg["mix"])


def _monthly_gpv(merchant: Any) -> float:
    return merchant.gpv_90d_minor() / 3.0


def _decide(merchant: Any, requested: int, term: int, bureau_score: int, fica_ok: bool, reasons: list[str]) -> dict:
    """Apply the decision rules and return decision fields. `reasons` already holds any
    eligibility failures; the result's `accepted` says whether funds will be disbursed."""
    automated = _LEND["actioned_by"]["automated"]
    outcome = {
        "decision": "declined", "actioned_by": automated, "decline_reasons": list(reasons),
        "approved_amount": 0, "approved_term_months": 0, "total_cost_percent": None, "accepted": False,
    }
    if reasons:
        return outcome
    if not fica_ok:
        outcome["decline_reasons"] = ["fica_failed"]
        return outcome
    if bureau_score < _LEND["bureau"]["floor"]:
        outcome["decline_reasons"] = ["low_bureau_score"]
        return outcome

    risk = merchant.risk_score
    amount = requested
    if risk <= _BANDS["auto_approve_max"]:
        outcome["decision"] = "approved"
    elif risk <= _BANDS["manual_review_max"]:
        outcome["actioned_by"] = _LEND["actioned_by"]["manual"]
        if np.random.random() >= _LEND["manual_review_approve_rate"]:
            outcome["decline_reasons"] = ["manual_review_declined"]
            return outcome
        outcome["decision"] = "approved"
    elif risk <= _BANDS["counter_offer_max"]:
        outcome["decision"] = "counter_offer"
        amount = requested * _BANDS["counter_offer_ratio"]
    else:
        outcome["decline_reasons"] = ["high_risk_score"]
        return outcome

    amount = _round_down(min(amount, _LEND["affordability_gpv_multiple"] * _monthly_gpv(merchant)))
    if amount < _LEND["min_approved_minor"]:
        outcome["decision"] = "declined"
        outcome["decline_reasons"] = ["low_affordability"]
        return outcome

    outcome["approved_amount"] = amount
    outcome["approved_term_months"] = term
    outcome["total_cost_percent"] = _LEND["total_cost_percent_by_term"][term]
    outcome["accepted"] = (
        True if outcome["decision"] == "approved"
        else bool(np.random.random() < _LEND["counter_offer_accept_rate"])
    )
    return outcome


def simulate_loan_application(merchant: Any, sim_date: date) -> Optional[dict]:
    """
    Probabilistically generate a loan_applications row for this merchant today.

    Returns the row to INSERT, or None if the merchant does not apply. On an accepted
    approval it also queues the disbursement on the merchant. Disbursement fields are
    null in the row; they are set later by the UPDATE from simulate_disbursements().
    """
    if np.random.random() >= application_probability(merchant, sim_date):
        return None

    merchant.last_application_date = sim_date
    reasons = eligibility_failures(merchant)

    declared = max(
        int(_monthly_gpv(merchant) * _uniform(_LEND["declared_revenue_factor"])),
        _LEND["declared_revenue_floor_minor"],
    )
    requested = int(declared * _uniform(_LEND["requested_revenue_multiple"]))
    requested = _round_down(max(_LEND["requested_floor_minor"], min(requested, _LEND["requested_ceiling_minor"])))
    term = int(_choose(_LEND["requested_term_mix"]))
    bureau_score, bureau = _bureau(merchant)
    fica_ok = np.random.random() >= _LEND["fica_fail_rate"]

    result = _decide(merchant, requested, term, bureau_score, fica_ok, reasons)

    submitted = _time_on(sim_date, int(np.random.randint(6, 22)), int(np.random.randint(0, 60)), int(np.random.randint(0, 60)))
    if result["actioned_by"] == _LEND["actioned_by"]["manual"]:
        latency = timedelta(hours=_uniform(_LEND["manual_review_hours"]))
    else:
        latency = timedelta(seconds=_randint(_LEND["decision_seconds"]))
    decided = submitted + latency

    short = merchant.merchant_id[:8]
    registered = np.random.random() < _LEND["business_registration_rate"]
    row = {
        "application_id": new_id(),
        "merchant_id": merchant.merchant_id,
        "applicant_user_id": f"user-{short}",
        "business_id": f"biz-{short}",
        "application_channel": _choose(_LEND["application_channel_mix"]),
        "submission_ts": submitted,
        "decision_ts": decided,
        "business_registration": (
            f"{np.random.randint(2005, 2025)}/{np.random.randint(1, 1_000_000):06d}/07" if registered else None
        ),
        "months_trading": merchant.months_on_platform + _randint(_LEND["prior_trading_months"]),
        "declared_monthly_revenue": declared,
        "requested_amount": requested,
        "requested_term_months": term,
        "owner_credit_score": bureau_score,
        "owner_bureau": bureau,
        "actioned_by": result["actioned_by"],
        "decision": result["decision"],
        "decline_reasons": result["decline_reasons"],
        "approved_amount": result["approved_amount"],
        "approved_term_months": result["approved_term_months"],
        "total_cost_percent": result["total_cost_percent"],
        "disbursed_at": None,
        "disbursement_amount_minor": None,
        "model_version_id": _LEND["model_version_id"],
        "fica_status": "pass" if fica_ok else "fail",
    }

    if result["accepted"]:
        merchant.scheduled_disbursements.append({
            "application_id": row["application_id"],
            "decided_at": decided,
            "due_date": decided.date() + timedelta(days=_randint(_LEND["disbursement_lag_days"])),
            "amount_minor": result["approved_amount"],
            "term_months": term,
            "total_cost_pct": result["total_cost_percent"],
        })
    return row


# ---------------------------------------------------------------------------
# Disbursement
# ---------------------------------------------------------------------------

def simulate_disbursements(merchant: Any, sim_date: date) -> list[dict]:
    """
    Disbursements due today. Each is an UPDATE of an existing loan_applications row
    (disbursed_at and disbursement_amount_minor), not a new row.

    Starting the loan also marks the merchant: an active Loan, has_active_loan true,
    loan_count incremented. No origination fee is deducted in V1, so the amount
    disbursed equals the approved amount.
    """
    due = [d for d in merchant.scheduled_disbursements if d["due_date"] <= sim_date]
    if not due:
        return []
    merchant.scheduled_disbursements = [d for d in merchant.scheduled_disbursements if d["due_date"] > sim_date]

    updates = []
    for item in due:
        landed = _time_on(item["due_date"], _randint(_LEND["disbursement_hours"]), int(np.random.randint(0, 60)))
        earliest = item["decided_at"] + timedelta(minutes=30)
        if landed < earliest:
            landed = earliest + timedelta(minutes=int(np.random.randint(0, 120)))
        updates.append({
            "application_id": item["application_id"],
            "disbursed_at": landed,
            "disbursement_amount_minor": item["amount_minor"],
        })
        merchant.active_loan = Loan(
            application_id=item["application_id"],
            merchant_id=merchant.merchant_id,
            principal_minor=item["amount_minor"],
            term_months=item["term_months"],
            total_cost_pct=item["total_cost_pct"],
            disbursed_at=landed.date(),
            outstanding_principal_minor=item["amount_minor"],
        )
        merchant.has_active_loan = True
        merchant.loan_count += 1
    return updates


def simulate_lending_day(merchant: Any, sim_date: date) -> dict[str, list[dict]]:
    """
    Lending events for one merchant on one day.

    Returns {"loan_applications": rows to INSERT, "loan_application_updates": disbursement
    UPDATEs}. Disbursements run first, so a loan disbursed today is already active when
    the merchant's eligibility is checked. Call after generator.simulate_day() so GPV
    includes today.
    """
    updates = simulate_disbursements(merchant, sim_date)
    application = simulate_loan_application(merchant, sim_date)
    return {
        "loan_applications": [application] if application else [],
        "loan_application_updates": updates,
    }
