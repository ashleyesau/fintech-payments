"""tests/test_lending.py: Phase 5.2 loan applications and disbursements."""
from collections import Counter
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pytest

from simulator import lending
from simulator.lending import (
    _decide,
    application_probability,
    eligibility_failures,
    simulate_disbursements,
    simulate_lending_day,
    simulate_loan_application,
)
from simulator.models import LoanApplication
from simulator.state import Merchant

TODAY = date(2026, 3, 10)  # a March day: no seasonal multiplier


def merchant(state="stable", risk=10, gpv_per_day=100_000, months=6, **kwargs) -> Merchant:
    """A trading merchant with 90 days of history, eligible by default (R90,000 GPV)."""
    m = Merchant(
        archetype="restaurant",
        quality_percentile=0.4,
        onboarded_at=date(2025, 6, 1),
        merchant_state=state,
        risk_score=risk,
        months_on_platform=months,
        **kwargs,
    )
    for i in range(90):
        m.record_day(date(2025, 12, 1) + timedelta(days=i), gpv_per_day, 5)
    return m


def force_random(monkeypatch, value: float) -> None:
    monkeypatch.setattr(np.random, "random", lambda: value)


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------

def test_default_merchant_is_eligible():
    assert eligibility_failures(merchant()) == []


@pytest.mark.parametrize("kwargs, reason", [
    ({"months": 2}, "insufficient_trading_history"),
    ({"gpv_per_day": 10_000}, "low_volume"),
    ({"state": "stressed"}, "adverse_merchant_state"),
    ({"state": "delinquent"}, "adverse_merchant_state"),
    ({"state": "new"}, "merchant_not_established"),
    ({"has_active_loan": True}, "existing_loan_outstanding"),
    ({"is_written_off": True}, "prior_write_off"),
])
def test_each_gate_has_a_decline_reason(kwargs, reason):
    assert eligibility_failures(merchant(**kwargs)) == [reason]


def test_failing_several_gates_lists_every_reason():
    reasons = eligibility_failures(merchant(months=1, state="stressed", has_active_loan=True))
    assert set(reasons) == {"insufficient_trading_history", "adverse_merchant_state", "existing_loan_outstanding"}


# ---------------------------------------------------------------------------
# Propensity
# ---------------------------------------------------------------------------

def daily(monthly: float) -> float:
    return 1 - (1 - monthly) ** (1 / 30)


def test_monthly_propensity_by_state_becomes_a_daily_probability():
    assert application_probability(merchant(state="stable"), TODAY) == pytest.approx(daily(0.04))
    assert application_probability(merchant(state="growing"), TODAY) == pytest.approx(daily(0.08))


def test_a_repaid_prior_loan_doubles_propensity():
    m = merchant(state="stable", prior_loan_repaid=True)
    assert application_probability(m, TODAY) == pytest.approx(daily(0.08))


def test_october_and_november_double_propensity():
    for month in (10, 11):
        assert application_probability(merchant(), date(2026, month, 10)) == pytest.approx(daily(0.08))
    assert application_probability(merchant(), date(2026, 9, 10)) == pytest.approx(daily(0.04))


def test_ineligible_merchants_apply_at_the_ineligible_rate():
    m = merchant(state="stressed")
    assert application_probability(m, TODAY) == pytest.approx(daily(0.04))


def test_merchants_outside_the_market_do_not_apply():
    assert application_probability(Merchant(), TODAY) == 0.0  # never traded
    dormant = merchant()
    dormant.dormant_since = date(2026, 1, 1)
    assert application_probability(dormant, TODAY) == 0.0
    assert application_probability(merchant(state="churned"), TODAY) == 0.0


def test_cooldown_blocks_a_second_application():
    m = merchant(last_application_date=TODAY - timedelta(days=29))
    assert application_probability(m, TODAY) == 0.0
    m.last_application_date = TODAY - timedelta(days=30)
    assert application_probability(m, TODAY) > 0.0


# ---------------------------------------------------------------------------
# Decision bands
# ---------------------------------------------------------------------------

def decide(m, requested=2_000_000, term=6, bureau=700, fica_ok=True):
    return _decide(m, requested, term, bureau, fica_ok, eligibility_failures(m))


def test_low_risk_is_approved_automatically(monkeypatch):
    force_random(monkeypatch, 0.5)
    out = decide(merchant(risk=30))
    assert (out["decision"], out["actioned_by"], out["accepted"]) == ("approved", "automated_engine", True)
    assert out["approved_amount"] == 2_000_000
    assert out["approved_term_months"] == 6 and out["total_cost_percent"] == 0.07


def test_manual_review_band_goes_to_an_analyst(monkeypatch):
    force_random(monkeypatch, 0.1)  # below the 60% approve rate
    approved = decide(merchant(risk=31))
    assert (approved["decision"], approved["actioned_by"]) == ("approved", "manual_review")
    force_random(monkeypatch, 0.9)
    declined = decide(merchant(risk=55))
    assert declined["decision"] == "declined" and declined["actioned_by"] == "manual_review"
    assert declined["decline_reasons"] == ["manual_review_declined"]


def test_counter_offer_band_offers_sixty_percent(monkeypatch):
    force_random(monkeypatch, 0.1)
    out = decide(merchant(risk=56), requested=2_000_000)
    assert out["decision"] == "counter_offer" and out["approved_amount"] == 1_200_000
    assert out["accepted"] is True
    force_random(monkeypatch, 0.9)
    assert decide(merchant(risk=75))["accepted"] is False


def test_high_risk_is_declined_automatically(monkeypatch):
    force_random(monkeypatch, 0.5)
    out = decide(merchant(risk=76))
    assert (out["decision"], out["actioned_by"], out["decline_reasons"]) == (
        "declined", "automated_engine", ["high_risk_score"],
    )
    assert out["approved_amount"] == 0 and out["approved_term_months"] == 0 and out["total_cost_percent"] is None


def test_ineligible_merchant_is_declined_with_its_gate_reasons(monkeypatch):
    force_random(monkeypatch, 0.5)
    out = decide(merchant(state="stressed", risk=5))
    assert out["decision"] == "declined" and out["decline_reasons"] == ["adverse_merchant_state"]


def test_failed_kyc_and_low_bureau_score_decline(monkeypatch):
    force_random(monkeypatch, 0.5)
    assert decide(merchant(), fica_ok=False)["decline_reasons"] == ["fica_failed"]
    assert decide(merchant(), bureau=399)["decline_reasons"] == ["low_bureau_score"]
    assert decide(merchant(), bureau=400)["decision"] == "approved"


def test_approval_is_capped_at_three_times_monthly_gpv(monkeypatch):
    force_random(monkeypatch, 0.5)
    # R90,000 over 90 days is R30,000 a month, so the cap is R90,000.
    out = decide(merchant(), requested=50_000_000)
    assert out["approved_amount"] == 9_000_000


def test_approved_amounts_round_down_to_whole_rands_of_100(monkeypatch):
    force_random(monkeypatch, 0.5)
    assert decide(merchant(), requested=1_234_567)["approved_amount"] == 1_230_000


def test_too_small_an_approval_is_declined_for_affordability(monkeypatch):
    force_random(monkeypatch, 0.5)
    # A counter-offer is 60% of the request: 60% of R5,000 is R3,000, under the R5,000 floor.
    out = decide(merchant(risk=60), requested=500_000)
    assert out["decision"] == "declined" and out["decline_reasons"] == ["low_affordability"]


# ---------------------------------------------------------------------------
# The row
# ---------------------------------------------------------------------------

def applications(monkeypatch, per_merchant=60, seed=21, **kwargs):
    """Applications from 40 merchants with risk scores across the range. The daily roll is
    forced to succeed so every call yields a row."""
    monkeypatch.setattr(lending, "application_probability", lambda m, d: 1.0)
    np.random.seed(seed)
    rows = []
    for _ in range(40):
        m = merchant(risk=int(np.random.randint(0, 100)), **kwargs)
        for d in range(per_merchant):
            rows.append(simulate_loan_application(m, date(2026, 1, 1) + timedelta(days=d)))
            m.has_active_loan = False
            m.scheduled_disbursements.clear()
    return rows


def test_rows_validate_against_the_model(monkeypatch):
    rows = applications(monkeypatch)
    assert len(rows) == 2400
    for row in rows:
        LoanApplication(**row)


def test_row_shape_by_decision(monkeypatch):
    for row in applications(monkeypatch):
        assert row["disbursed_at"] is None and row["disbursement_amount_minor"] is None
        assert row["decision_ts"] > row["submission_ts"]
        assert row["requested_amount"] >= 1_000_000
        if row["decision"] == "declined":
            assert row["decline_reasons"] and row["approved_amount"] == 0 and row["total_cost_percent"] is None
        else:
            assert row["decline_reasons"] == [] and row["approved_amount"] >= 500_000
            assert row["approved_term_months"] == row["requested_term_months"]
        if row["decision"] == "counter_offer":
            assert row["approved_amount"] <= row["requested_amount"] * 0.60


def test_identifiers_are_stable_per_merchant(monkeypatch):
    monkeypatch.setattr(lending, "application_probability", lambda m, d: 1.0)
    m = merchant()
    np.random.seed(2)
    rows = [simulate_loan_application(m, date(2026, 1, 1) + timedelta(days=i)) for i in range(20)]
    assert len(rows) == 20
    assert len({r["applicant_user_id"] for r in rows}) == 1 and len({r["business_id"] for r in rows}) == 1
    assert len({r["application_id"] for r in rows}) == len(rows)


def test_manual_review_takes_hours_and_automated_decisions_take_seconds(monkeypatch):
    rows = applications(monkeypatch, per_merchant=100, seed=4)
    manual = [r for r in rows if r["actioned_by"] == "manual_review"]
    auto = [r for r in rows if r["actioned_by"] == "automated_engine"]
    assert manual and auto
    assert all(timedelta(hours=1) <= r["decision_ts"] - r["submission_ts"] <= timedelta(hours=10) for r in manual)
    assert all(r["decision_ts"] - r["submission_ts"] <= timedelta(seconds=180) for r in auto)


def test_application_sets_the_cooldown_and_returns_none_otherwise(monkeypatch):
    m = merchant()
    force_random(monkeypatch, 0.9999)
    assert simulate_loan_application(m, TODAY) is None
    assert m.last_application_date is None
    force_random(monkeypatch, 0.0)
    assert simulate_loan_application(m, TODAY) is not None
    assert m.last_application_date == TODAY


# ---------------------------------------------------------------------------
# Disbursement
# ---------------------------------------------------------------------------

def approved_application(seed=1):
    """Draw applications until one is approved and queued for disbursement."""
    np.random.seed(seed)
    m = merchant(risk=5)
    for d in range(2000):
        row = simulate_loan_application(m, date(2026, 1, 1) + timedelta(days=d))
        if row and row["decision"] == "approved":
            return m, row, date(2026, 1, 1) + timedelta(days=d)
    raise AssertionError("no approved application drawn")


def test_approved_application_queues_a_disbursement_within_three_days():
    m, row, day = approved_application()
    assert len(m.scheduled_disbursements) == 1
    item = m.scheduled_disbursements[0]
    assert item["application_id"] == row["application_id"] and item["amount_minor"] == row["approved_amount"]
    decided = row["decision_ts"].date()
    assert decided <= item["due_date"] <= decided + timedelta(days=3)


def test_declined_and_refused_counter_offers_are_not_disbursed(monkeypatch):
    m = merchant(risk=90)
    force_random(monkeypatch, 0.0)
    row = simulate_loan_application(m, TODAY)
    assert row["decision"] == "declined" and m.scheduled_disbursements == []
    refused = merchant(risk=60)
    # The first random call is the application roll (0.0); every later call refuses the
    # counter-offer.
    calls = iter([0.0] + [0.9] * 40)
    monkeypatch.setattr(np.random, "random", lambda: next(calls))
    row = simulate_loan_application(refused, TODAY)
    assert row["decision"] == "counter_offer" and refused.scheduled_disbursements == []


def test_nothing_is_disbursed_before_the_due_date():
    m, row, day = approved_application()
    due = m.scheduled_disbursements[0]["due_date"]
    assert simulate_disbursements(m, due - timedelta(days=1)) == []
    assert m.has_active_loan is False and m.active_loan is None


def test_disbursement_is_an_update_of_the_existing_row_and_starts_the_loan():
    m, row, day = approved_application()
    due = m.scheduled_disbursements[0]["due_date"]
    updates = simulate_disbursements(m, due)
    assert len(updates) == 1
    update = updates[0]
    assert set(update) == {"application_id", "disbursed_at", "disbursement_amount_minor"}
    assert update["application_id"] == row["application_id"]
    assert update["disbursement_amount_minor"] == row["approved_amount"]
    assert update["disbursed_at"] >= row["decision_ts"] + timedelta(minutes=30)
    assert update["disbursed_at"].date() >= due

    loan = m.active_loan
    assert loan.loan_id == loan.application_id == row["application_id"]
    assert (loan.principal_minor, loan.outstanding_principal_minor) == (row["approved_amount"], row["approved_amount"])
    assert loan.term_months == row["requested_term_months"] and loan.total_cost_pct == row["total_cost_percent"]
    assert loan.disbursed_at == update["disbursed_at"].date()
    assert m.has_active_loan is True and m.loan_count == 1
    assert m.scheduled_disbursements == []
    assert simulate_disbursements(m, due + timedelta(days=1)) == []


def test_merchant_with_a_loan_is_ineligible_for_another():
    m, row, day = approved_application()
    simulate_disbursements(m, m.scheduled_disbursements[0]["due_date"])
    assert eligibility_failures(m) == ["existing_loan_outstanding"]


def test_lending_day_runs_disbursements_before_applications():
    m, row, day = approved_application()
    due = m.scheduled_disbursements[0]["due_date"]
    out = simulate_lending_day(m, due)
    assert [u["application_id"] for u in out["loan_application_updates"]] == [row["application_id"]]
    assert out["loan_applications"] == []  # cooldown after the application
    assert m.has_active_loan


# ---------------------------------------------------------------------------
# Population: Validation Targets approval and seasonality bands
# ---------------------------------------------------------------------------

def population():
    np.random.seed(11)

    def build(kind: str) -> Merchant:
        m = merchant(gpv_per_day=10_000 if kind == "lowvol" else 100_000)
        m.quality_percentile = float(np.random.beta(1.5, 4.0))
        m.merchant_state = {"elig_g": "growing", "stressed": "stressed"}.get(kind, "stable")
        if kind == "young":
            m.months_on_platform = 1
        if kind == "loan":
            m.has_active_loan = True
        # Risk score distribution from Validation Targets: 62 / 20 / 12 / 6 percent.
        r = np.random.random()
        low, high = (0, 30) if r < 0.62 else (31, 55) if r < 0.82 else (56, 75) if r < 0.94 else (76, 100)
        m.risk_score = int(np.random.randint(low, high + 1))
        return m

    kinds = ["elig_g"] * 150 + ["elig_s"] * 150 + ["stressed"] * 50 + ["young"] * 50 + ["lowvol"] * 50 + ["loan"] * 50
    merchants = [(k, build(k)) for k in kinds]
    rows = []
    for d in range(365):
        day = date(2026, 1, 1) + timedelta(days=d)
        for kind, m in merchants:
            row = simulate_loan_application(m, day)
            if row:
                rows.append((kind, day, row))
                m.scheduled_disbursements.clear()
    return rows


@pytest.fixture(scope="module")
def population_rows():
    return population()


def test_gross_and_net_approval_rates_sit_in_their_bands(population_rows):
    eligible = [r for k, _, r in population_rows if k.startswith("elig")]
    approved_all = [r for _, _, r in population_rows if r["decision"] == "approved"]
    approved_eligible = [r for r in eligible if r["decision"] == "approved"]
    gross = len(approved_all) / len(population_rows)
    net = len(approved_eligible) / len(eligible)
    assert 0.35 <= gross <= 0.55
    assert 0.55 <= net <= 0.75
    assert gross < net


def test_october_november_volume_is_one_and_a_half_to_two_and_a_half_times_average(population_rows):
    by_month = Counter(day.month for _, day, _ in population_rows)
    average = len(population_rows) / 12
    assert 1.5 <= (by_month[10] + by_month[11]) / 2 / average <= 2.5


def test_ineligible_applications_are_always_declined(population_rows):
    for kind, _, row in population_rows:
        if not kind.startswith("elig"):
            assert row["decision"] == "declined" and row["decline_reasons"]
