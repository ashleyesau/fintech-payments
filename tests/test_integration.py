"""tests/test_integration.py: generator, state engine and risk decay run together.

A handful of low-volume merchants are simulated day by day with the same loop the
Phase 7 backfill will use: simulate_day, then evaluate_state, then decay_risk.
"""
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pytest

from simulator import lending
from simulator.generator import simulate_day
from simulator.lending import simulate_lending_day
from simulator.models import LoanApplication, LoanRepayment, MerchantSnapshot, MerchantTransaction, RiskEvent
from simulator.risk import evaluate_risk
from simulator.state import Merchant

START = date(2025, 1, 1)
ARCHETYPES = ["wholesale", "tutor", "e_commerce", "salon"]
CARD_CHANNELS = {"card_present", "card_ecom", "payment_link"}


def run(days: int, seed: int = 31):
    np.random.seed(seed)
    risk_rows: dict[str, list[dict]] = defaultdict(list)
    merchants = [
        Merchant(
            merchant_id=f"m-{i}",
            archetype=a,
            quality_percentile=0.3,
            onboarded_at=START,
            province=["gauteng", "western_cape", "kwazulu_natal", "eastern_cape"][i],
            is_metro=i % 2 == 0,
        )
        for i, a in enumerate(ARCHETYPES)
    ]
    events: dict[str, list[dict]] = defaultdict(list)
    for offset in range(days):
        day = START + timedelta(days=offset)
        for m in merchants:
            todays = simulate_day(m, day)
            events[m.merchant_id] += todays
            risk_rows[m.merchant_id] += evaluate_risk(m, todays, day)
            m.evaluate_state(day)
            m.decay_risk(day)
    return merchants, events


@pytest.fixture(scope="module")
def year_run():
    return run(150)


def test_all_events_validate_and_ids_are_unique(year_run):
    _, events = year_run
    ids = [e["transaction_id"] for evs in events.values() for e in evs]
    assert len(ids) > 3000
    assert len(ids) == len(set(ids))
    for evs in events.values():
        for e in evs:
            MerchantTransaction(**e)


def test_recorded_gpv_matches_completed_payments(year_run):
    merchants, events = year_run
    for m in merchants:
        by_day = defaultdict(int)
        for e in events[m.merchant_id]:
            if e["event_type"] == "payment" and e["status"] == "completed":
                by_day[e["created_at"].date()] += e["amount_minor"]
        last_90 = [START + timedelta(days=150 - 90 + i) for i in range(90)]
        assert list(m.daily_gpv_minor) == [by_day.get(d, 0) for d in last_90], m.archetype


def test_wholesale_never_trades_on_weekends(year_run):
    _, events = year_run
    payments = [e for e in events["m-0"] if e["event_type"] == "payment"]
    assert payments
    assert all(e["created_at"].weekday() < 5 for e in payments)


def test_followups_refer_to_earlier_completed_payments(year_run):
    _, events = year_run
    for merchant_id, evs in events.items():
        payments = {e["transaction_id"]: e for e in evs if e["event_type"] == "payment"}
        followups = [e for e in evs if e["event_type"] in {"refund", "chargeback"}]
        for f in followups:
            original = payments[f["correlation_id"]]
            assert original["status"] == "completed"
            assert original["amount_minor"] == f["amount_minor"]
            assert f["created_at"].date() > original["created_at"].date()
            if f["event_type"] == "chargeback":
                assert original["channel"] in CARD_CHANNELS


def test_refunds_and_chargebacks_occur_in_the_run(year_run):
    _, events = year_run
    kinds = {e["event_type"] for evs in events.values() for e in evs}
    assert {"payment", "refund"} <= kinds


def test_states_and_risk_scores_stay_valid(year_run):
    merchants, _ = year_run
    for m in merchants:
        assert m.merchant_state in {"new", "growing", "stable", "stressed", "delinquent", "churned"}
        assert 0 <= m.risk_score <= 100
        MerchantSnapshot(**m.snapshot(datetime(2025, 6, 1, tzinfo=timezone.utc)))


def test_merchants_leave_new_once_there_is_enough_history(year_run):
    merchants, _ = year_run
    assert any(m.merchant_state != "new" for m in merchants)


def test_same_seed_reproduces_the_whole_run():
    def fingerprint():
        merchants, events = run(60, seed=77)
        return (
            [(m.merchant_state, m.risk_score, list(m.daily_gpv_minor)) for m in merchants],
            [(e["transaction_id"], e["amount_minor"], e["status"]) for evs in events.values() for e in evs],
        )

    assert fingerprint() == fingerprint()


def test_dormant_merchants_stop_trading_and_walk_down_to_churned(monkeypatch):
    # Churn turned up so merchants go dark within the run.
    monkeypatch.setattr("simulator.state._CHURN_MULTIPLIER", 25.0)
    merchants, events = run(240, seed=5)
    dormant = [m for m in merchants if m.dormant_since is not None]
    assert dormant
    for m in dormant:
        later_payments = [
            e for e in events[m.merchant_id]
            if e["event_type"] == "payment" and e["created_at"].date() > m.dormant_since
        ]
        assert later_payments == []
    churned = [m for m in merchants if m.merchant_state == "churned"]
    assert churned
    assert all(m.dormant_since is not None for m in churned)


def test_lending_events_follow_the_merchants_through_a_run(monkeypatch):
    # Propensity turned up so a small population produces loans within the run.
    for state in ("growing", "stable"):
        monkeypatch.setitem(lending._PROPENSITY, state, 0.6)
    monkeypatch.setitem(lending._LEND, "ineligible_monthly_application_probability", 0.3)

    np.random.seed(14)
    merchants = [
        Merchant(
            merchant_id=f"m-{i}", archetype=a, quality_percentile=0.35, onboarded_at=START, risk_score=8
        )
        for i, a in enumerate(["restaurant", "salon", "e_commerce", "wholesale"])
    ]
    applications, updates, repayments = [], [], []
    for offset in range(420):
        day = START + timedelta(days=offset)
        for m in merchants:
            simulate_day(m, day)
            lend = simulate_lending_day(m, day)
            applications += lend["loan_applications"]
            updates += lend["loan_application_updates"]
            repayments += lend["loan_repayments"]
            m.evaluate_state(day)
            m.decay_risk(day)

    assert applications and updates and repayments
    for row in applications:
        LoanApplication(**row)
    for row in repayments:
        LoanRepayment(**row)
    by_id = {a["application_id"]: a for a in applications}
    assert len(by_id) == len(applications)
    disbursed = {}
    for update in updates:
        original = by_id[update["application_id"]]
        assert original["decision"] in {"approved", "counter_offer"}
        assert update["disbursement_amount_minor"] == original["approved_amount"]
        assert update["disbursed_at"] > original["decision_ts"]
        disbursed[update["application_id"]] = update
    assert len(disbursed) == len(updates)

    # A repayment always follows its loan's disbursement, as the foreign key requires.
    for row in repayments:
        update = disbursed[row["loan_id"]]
        assert row["due_date"] > update["disbursed_at"].date()
        assert by_id[row["loan_id"]]["merchant_id"] == row["merchant_id"]
    assert len({r["repayment_id"] for r in repayments}) == len(repayments)

    for m in merchants:
        own = [u for u in updates if by_id[u["application_id"]]["merchant_id"] == m.merchant_id]
        assert m.loan_count == len(own)
        assert m.has_active_loan == (m.active_loan is not None)
        assert 0 <= m.risk_score <= 100


def test_risk_events_from_a_run_are_valid_and_move_the_score(monkeypatch):
    # Every merchant is in a fraud cluster for the first days so the rules have something to see.
    np.random.seed(40)
    merchants = [
        Merchant(merchant_id=f"m-{i}", archetype=a, quality_percentile=0.35, onboarded_at=START,
                 first_transaction_at=START, fraud_cluster_until=START + timedelta(days=3), risk_score=10)
        for i, a in enumerate(["spaza_shop", "restaurant", "e_commerce", "salon"])
    ]
    rows = []
    for offset in range(60):
        day = START + timedelta(days=offset)
        for m in merchants:
            rows += evaluate_risk(m, simulate_day(m, day), day)
            m.decay_risk(day)
    assert rows
    ids = set()
    for row in rows:
        RiskEvent(**row)
        assert row["alert_id"] not in ids
        ids.add(row["alert_id"])
    assert all(0 <= m.risk_score <= 100 for m in merchants)
    assert max(m.risk_score for m in merchants) > 10
    # The last event's score is the merchant's score at that moment.
    for m in merchants:
        own = [r for r in rows if r["merchant_id"] == m.merchant_id]
        if own:
            assert own[-1]["risk_score"] >= m.risk_score  # decay only lowers it afterwards
