"""tests/test_risk.py: Phase 5.4 fraud clusters and Chain 5 risk rules."""
from collections import Counter
from datetime import date, datetime, timedelta

import numpy as np
import pytest

from simulator import risk
from simulator.common import SAST
from simulator.generator import _ticket_minor, simulate_day, simulate_transaction
from simulator.models import RiskEvent
from simulator.risk import cluster_active, evaluate_risk, maybe_start_fraud_cluster
from simulator.state import Merchant

DAY = date(2025, 6, 10)


def merchant(archetype="restaurant", state="stable", risk_score=10, **kwargs) -> Merchant:
    m = Merchant(
        archetype=archetype, merchant_state=state, risk_score=risk_score,
        onboarded_at=date(2025, 1, 1), first_transaction_at=date(2025, 1, 5), **kwargs,
    )
    m.baseline_daily_tx = 100.0
    return m


def at(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=SAST)


def payment(n: int, day=DAY, hour=12, channel="card_present", status="completed", amount=10_000, **extra) -> dict:
    return {
        "transaction_id": f"p-{n}", "event_type": "payment", "status": status, "channel": channel,
        "amount_minor": amount, "created_at": at(day, hour, n % 60),
        "otp_requested_at": None, "otp_entered_at": None, **extra,
    }


def follow_up(kind: str, n: int, day=DAY) -> dict:
    return {
        "transaction_id": f"{kind}-{n}", "event_type": kind, "status": "completed",
        "channel": "card_present", "amount_minor": 5_000, "created_at": at(day, 15, n % 60),
        "otp_requested_at": None, "otp_entered_at": None,
    }


def prefill(m: Merchant, days: int, completed=100, card=60, refunds=0, chargebacks=0) -> None:
    for _ in range(days):
        m.risk_history.append((completed, card, refunds, chargebacks))


def rules(rows: list[dict]) -> list[str]:
    return [r["rule_type"] for r in rows]


# ---------------------------------------------------------------------------
# Fraud clusters
# ---------------------------------------------------------------------------

def test_cluster_active_covers_the_start_and_end_days():
    m = merchant()
    assert not cluster_active(m, DAY)
    m.fraud_cluster_until = DAY
    assert cluster_active(m, DAY) and not cluster_active(m, DAY + timedelta(days=1))


def test_cluster_start_hazard_is_the_archetype_fraud_rate_per_month(monkeypatch):
    # E-commerce fraud_rate 1% a month: just under the daily hazard hits, just over misses.
    daily = 1 - (1 - 0.01) ** (1 / 30)
    m = merchant("e_commerce")
    monkeypatch.setattr(np.random, "random", lambda: daily * 0.999)
    assert maybe_start_fraud_cluster(m, DAY) is True
    m2 = merchant("e_commerce")
    monkeypatch.setattr(np.random, "random", lambda: daily * 1.001)
    assert maybe_start_fraud_cluster(m2, DAY) is False


def test_a_cluster_lasts_two_to_five_days(monkeypatch):
    monkeypatch.setattr(np.random, "random", lambda: 0.0)
    lengths = set()
    for seed in range(40):
        np.random.seed(seed)
        m = merchant()
        maybe_start_fraud_cluster(m, DAY)
        lengths.add((m.fraud_cluster_until - DAY).days + 1)
    assert lengths <= {2, 3, 4, 5} and len(lengths) > 1


@pytest.mark.parametrize("change", [
    {"first_transaction_at": None},
    {"dormant_since": DAY - timedelta(days=5)},
    {"merchant_state": "churned"},
    {"fraud_cluster_until": DAY + timedelta(days=2)},
])
def test_clusters_do_not_start_for_these_merchants(monkeypatch, change):
    monkeypatch.setattr(np.random, "random", lambda: 0.0)
    m = merchant()
    for key, value in change.items():
        setattr(m, key, value)
    before = m.fraud_cluster_until
    assert maybe_start_fraud_cluster(m, DAY) is False
    assert m.fraud_cluster_until == before


def sample_day(m: Merchant, n_days=30, seed=3):
    np.random.seed(seed)
    out = []
    for d in range(n_days):
        out += simulate_day(m, date(2025, 6, 1) + timedelta(days=d))
    return out


@pytest.mark.slow
def test_a_cluster_multiplies_payment_volume():
    def payments(cluster: bool):
        m = merchant("restaurant", quality_percentile=0.3)
        if cluster:
            m.fraud_cluster_until = date(2025, 7, 30)
        np.random.seed(5)
        events = []
        for d in range(20):
            events += simulate_day(m, date(2025, 6, 1) + timedelta(days=d))
        return len([e for e in events if e["event_type"] == "payment"])

    assert payments(True) / payments(False) == pytest.approx(3.0, rel=0.25)


def test_baseline_daily_tx_excludes_the_cluster_multiplier():
    calm, busy = merchant("restaurant", quality_percentile=0.3), merchant("restaurant", quality_percentile=0.3)
    busy.fraud_cluster_until = DAY
    np.random.seed(5)
    simulate_day(calm, DAY)
    np.random.seed(5)
    simulate_day(busy, DAY)
    assert busy.baseline_daily_tx == calm.baseline_daily_tx > 0


def test_cluster_payments_include_oversized_tickets():
    np.random.seed(9)
    tickets = [_ticket_minor("restaurant", DAY, in_cluster=True) for _ in range(4000)]
    large = [t for t in tickets if t > 5 * 150 * 100]
    assert 0.07 <= len(large) / len(tickets) <= 0.13
    assert all(6 * 150 * 100 <= t <= 10 * 150 * 100 for t in large)
    normal = [_ticket_minor("restaurant", DAY) for _ in range(4000)]
    assert not [t for t in normal if t > 5 * 150 * 100]


def test_cluster_payments_carry_elevated_risk_scores():
    m = merchant("restaurant")
    m.fraud_cluster_until = DAY
    np.random.seed(2)
    scores = [simulate_transaction(m, DAY)["risk_score"] for _ in range(300)]
    assert all(60 <= s <= 95 for s in scores)
    calm = merchant("restaurant")
    ordinary = [simulate_transaction(calm, DAY) for _ in range(300)]
    # A fraud-blocked decline scores 80 to 99 anyway; every other payment stays low.
    assert max(t["risk_score"] for t in ordinary if t["decline_code"] != "59") < 60


@pytest.mark.slow
def test_cluster_eft_attempts_often_fail_at_the_otp_step():
    def otp_failure_share(cluster: bool) -> float:
        m = merchant("wholesale")
        if cluster:
            m.fraud_cluster_until = DAY
        np.random.seed(4)
        rows = [simulate_transaction(m, DAY) for _ in range(6000)]
        eft = [r for r in rows if r["channel"] == "instant_eft" and r["login_succeeded_at"]]
        failed = [r for r in eft if r["otp_requested_at"] and not r["otp_entered_at"]]
        return len(failed) / len(eft)

    assert otp_failure_share(False) == pytest.approx(0.15, abs=0.04)
    assert otp_failure_share(True) == pytest.approx(0.65, abs=0.05)


def test_cluster_follow_ups_are_far_more_likely():
    from simulator.generator import _schedule_followups

    def follow_ups(cluster: bool) -> Counter:
        m = merchant("e_commerce")
        if cluster:
            m.fraud_cluster_until = DAY
        np.random.seed(6)
        txn = {"transaction_id": "t", "channel": "card_ecom", "payment_method": "visa", "amount_minor": 1}
        for _ in range(4000):
            _schedule_followups(m, txn, DAY)
        return Counter(f["kind"] for f in m.scheduled_followups)

    calm, bad = follow_ups(False), follow_ups(True)
    assert bad["refund"] / calm["refund"] == pytest.approx(8.0, rel=0.25)
    assert bad["chargeback"] / calm["chargeback"] == pytest.approx(10.0, rel=0.35)


# ---------------------------------------------------------------------------
# Rule evaluation: quiet days
# ---------------------------------------------------------------------------

def test_a_quiet_day_fires_nothing_and_advances_the_history():
    m = merchant()
    events = [payment(n) for n in range(5)]
    assert evaluate_risk(m, events, DAY) == []
    assert list(m.risk_history) == [(5, 5, 0, 0)]
    assert evaluate_risk(m, [], DAY + timedelta(days=1)) == []
    assert len(m.risk_history) == 2


def test_history_counts_completed_card_payments_refunds_and_chargebacks():
    m = merchant()
    events = [
        payment(1), payment(2, channel="instant_eft"), payment(3, status="failed"),
        follow_up("refund", 1), follow_up("refund", 2), follow_up("chargeback", 1),
    ]
    evaluate_risk(m, events, DAY)
    assert list(m.risk_history) == [(2, 1, 2, 1)]


# ---------------------------------------------------------------------------
# Refund spike and its escalation
# ---------------------------------------------------------------------------

def spaza_with_busy_week(risk_score=10) -> Merchant:
    m = merchant("spaza_shop", risk_score=risk_score)
    prefill(m, 6, completed=100, card=60, refunds=2)  # 600 completed, 12 refunds
    return m


def test_a_refund_spike_adds_ten_and_links_to_the_last_refund():
    m = spaza_with_busy_week()
    events = [payment(1)] + [follow_up("refund", n) for n in range(10)]
    (row,) = evaluate_risk(m, events, DAY)
    RiskEvent(**row)
    assert row["rule_type"] == "refund_spike" and row["risk_score_delta"] == 10
    assert row["entity_type"] == "transaction" and row["entity_id"] == "refund-9"
    assert row["risk_score"] == m.risk_score == 20
    assert row["action"] == "allow" and row["action_by"] == "system_auto"
    assert row["action_ts"] > events[-1]["created_at"]
    assert row["resolved_ts"] is None and row["outcome"] is None
    assert m.escalation_until is None
    assert m.last_adverse_event_date == DAY


def test_a_spike_above_the_manual_review_score_is_flagged_and_opens_the_window():
    m = spaza_with_busy_week(risk_score=50)
    (row,) = evaluate_risk(m, [follow_up("refund", n) for n in range(10)], DAY)
    assert row["risk_score"] == 60 and row["action"] == "flag_review"
    assert m.escalation_until == DAY + timedelta(days=30)


def test_ordinary_refund_levels_do_not_fire():
    m = merchant("spaza_shop")
    prefill(m, 6, completed=100, card=60, refunds=0)
    assert evaluate_risk(m, [payment(1)] + [follow_up("refund", n) for n in range(2)], DAY) == []


def test_a_high_window_without_a_refund_today_does_not_fire():
    m = spaza_with_busy_week()
    assert evaluate_risk(m, [payment(1)], DAY) == []


def test_refund_spike_cooldown_is_seven_days():
    m = spaza_with_busy_week()
    burst = [follow_up("refund", n) for n in range(10)]
    assert rules(evaluate_risk(m, burst, DAY)) == ["refund_spike"]
    assert evaluate_risk(m, burst, DAY + timedelta(days=3)) == []
    assert rules(evaluate_risk(m, burst, DAY + timedelta(days=7))) == ["refund_spike"]


def test_a_chargeback_after_a_flagged_spike_escalates_and_stresses_a_stable_merchant():
    m = spaza_with_busy_week(risk_score=50)
    evaluate_risk(m, [follow_up("refund", n) for n in range(10)], DAY)
    later = DAY + timedelta(days=12)
    (row,) = evaluate_risk(m, [follow_up("chargeback", 1, later)], later)
    RiskEvent(**row)
    assert row["rule_type"] == "chargeback_accumulation" and row["rule_id"] == "rule-refund-chargeback-escalation"
    assert row["risk_score_delta"] == 20 and row["risk_score"] == 80
    assert m.merchant_state == "stressed" and m.escalation_until is None


def test_escalation_leaves_other_states_alone():
    m = spaza_with_busy_week(risk_score=50)
    m.merchant_state = "growing"
    evaluate_risk(m, [follow_up("refund", n) for n in range(10)], DAY)
    later = DAY + timedelta(days=5)
    assert rules(evaluate_risk(m, [follow_up("chargeback", 1, later)], later)) == ["chargeback_accumulation"]
    assert m.merchant_state == "growing"


def test_a_chargeback_after_the_window_does_not_escalate():
    m = spaza_with_busy_week(risk_score=50)
    evaluate_risk(m, [follow_up("refund", n) for n in range(10)], DAY)
    late = DAY + timedelta(days=31)
    assert evaluate_risk(m, [follow_up("chargeback", 1, late)], late) == []
    assert m.merchant_state == "stable"


def test_unflagged_spike_opens_no_escalation_window():
    m = spaza_with_busy_week(risk_score=10)
    evaluate_risk(m, [follow_up("refund", n) for n in range(10)], DAY)
    later = DAY + timedelta(days=2)
    assert evaluate_risk(m, [follow_up("chargeback", 1, later)], later) == []
    assert m.merchant_state == "stable"


# ---------------------------------------------------------------------------
# Chargeback accumulation and the VAMP ratio
# ---------------------------------------------------------------------------

def test_chargeback_accumulation_needs_count_and_a_rate_above_baseline():
    m = merchant("spaza_shop")  # fraud rate 0.1%: 3000 card payments in 90 days predicts 3
    prefill(m, 89, completed=40, card=34, chargebacks=0)
    for day in range(10):
        m.risk_history[day] = (40, 34, 0, 1)  # 10 chargebacks spread over the window
    (row,) = evaluate_risk(m, [follow_up("chargeback", 1)], DAY)
    RiskEvent(**row)
    assert row["rule_type"] == "chargeback_accumulation" and row["risk_score_delta"] == 20
    assert row["rule_id"] == "rule-chargeback-90d" and row["action"] == "flag_review"


def test_a_few_chargebacks_for_an_ecommerce_merchant_are_normal():
    m = merchant("e_commerce")  # 1% fraud rate: about 10 chargebacks expected over 1,000 card payments
    prefill(m, 89, completed=14, card=11, chargebacks=0)
    for day in range(5):
        m.risk_history[day] = (14, 11, 0, 1)
    assert evaluate_risk(m, [follow_up("chargeback", 1)], DAY) == []


def test_chargeback_accumulation_cooldown_is_ninety_days():
    m = merchant("spaza_shop")
    prefill(m, 89, completed=40, card=34)
    for day in range(10):
        m.risk_history[day] = (40, 34, 0, 1)
    assert rules(evaluate_risk(m, [follow_up("chargeback", 1)], DAY)) == ["chargeback_accumulation"]
    assert evaluate_risk(m, [follow_up("chargeback", 2)], DAY + timedelta(days=60)) == []


def test_scheme_warning_fires_above_a_one_and_a_half_percent_vamp_ratio():
    m = merchant("e_commerce")
    prefill(m, 29, completed=14, card=14)  # about 406 card payments in 30 days
    for day in range(12):
        m.risk_history[day] = (14, 14, 0, 1)  # 12 chargebacks, plus today's
    (row,) = [r for r in evaluate_risk(m, [follow_up("chargeback", 1)], DAY) if r["rule_type"] == "scheme_warning"]
    RiskEvent(**row)
    assert row["risk_score_delta"] == 25 and row["entity_type"] == "merchant"
    assert row["entity_id"] == m.merchant_id


def test_scheme_warning_needs_card_volume():
    m = merchant("e_commerce")
    prefill(m, 5, completed=5, card=5)  # 30 card payments
    for day in range(5):
        m.risk_history[day] = (5, 5, 0, 1)
    assert "scheme_warning" not in rules(evaluate_risk(m, [follow_up("chargeback", 1)], DAY))


# ---------------------------------------------------------------------------
# Velocity, large transactions, OTP abuse
# ---------------------------------------------------------------------------

def test_velocity_breach_above_the_hourly_mean_plus_sigma():
    # Restaurant at 100 a day: hour 12 expects 11.6, so the 5-sigma line is about 28.6.
    m = merchant("restaurant")
    (row,) = evaluate_risk(m, [payment(n, hour=12) for n in range(40)], DAY)
    RiskEvent(**row)
    assert row["rule_type"] == "velocity_breach" and row["risk_score_delta"] == 12
    assert row["entity_id"] == "p-39" and row["action"] == "flag_review"
    assert evaluate_risk(merchant("restaurant"), [payment(n, hour=12) for n in range(25)], DAY) == []


def test_velocity_ignores_tiny_hours():
    m = merchant("restaurant")
    m.baseline_daily_tx = 1.0
    assert evaluate_risk(m, [payment(n, hour=12) for n in range(7)], DAY) == []
    assert rules(evaluate_risk(merchant("restaurant"), [payment(n) for n in range(0)], DAY)) == []


def test_velocity_uses_the_baseline_not_todays_inflated_volume():
    m = merchant("restaurant")
    m.baseline_daily_tx = 400.0  # a very busy normal day
    assert evaluate_risk(m, [payment(n, hour=12) for n in range(40)], DAY) == []


def test_large_transaction_is_five_times_the_mean_ticket():
    m = merchant("restaurant")  # mean ticket R150, so the line is R750
    assert evaluate_risk(m, [payment(1, amount=75_000)], DAY) == []
    (row,) = evaluate_risk(m, [payment(2, amount=75_001)], DAY)
    RiskEvent(**row)
    assert row["rule_type"] == "large_txn_flag" and row["risk_score_delta"] == 5
    assert row["entity_id"] == "p-2"


def test_large_transaction_fires_once_a_day():
    m = merchant("restaurant")
    rows = evaluate_risk(m, [payment(1, amount=200_000), payment(2, amount=300_000)], DAY)
    assert rules(rows) == ["large_txn_flag"]
    assert evaluate_risk(m, [payment(3, amount=200_000)], DAY) == []


def otp_failure(n: int, hour=10) -> dict:
    return payment(n, hour=hour, channel="instant_eft", status="expired",
                   otp_requested_at=at(DAY, hour, n % 60), otp_entered_at=None)


def test_three_otp_failures_in_an_hour_is_abuse_and_blocks():
    m = merchant("wholesale")
    (row,) = evaluate_risk(m, [otp_failure(n) for n in range(3)], DAY)
    RiskEvent(**row)
    assert row["rule_type"] == "otp_abuse" and row["risk_score_delta"] == 8 and row["action"] == "block"


def test_otp_failures_spread_across_hours_or_below_three_do_not_count():
    m = merchant("wholesale")
    assert evaluate_risk(m, [otp_failure(0, hour=9), otp_failure(1, hour=10), otp_failure(2, hour=11)], DAY) == []
    assert evaluate_risk(m, [otp_failure(0), otp_failure(1)], DAY) == []


def test_otp_entries_that_completed_are_not_failures():
    m = merchant("wholesale")
    done = [payment(n, hour=10, channel="instant_eft", otp_requested_at=at(DAY, 10), otp_entered_at=at(DAY, 10, 1))
            for n in range(5)]
    assert evaluate_risk(m, done, DAY) == []


def test_several_rules_can_fire_on_the_same_day_and_stack():
    m = merchant("restaurant")
    events = [payment(n, hour=12) for n in range(40)] + [payment(99, amount=200_000)]
    assert set(rules(evaluate_risk(m, events, DAY))) == {"velocity_breach", "large_txn_flag"}
    assert m.risk_score == 10 + 12 + 5


def test_risk_score_is_capped_at_100():
    m = merchant("restaurant", risk_score=95)
    (row,) = evaluate_risk(m, [payment(1, amount=200_000)], DAY)
    assert row["risk_score"] == 100 == m.risk_score


# ---------------------------------------------------------------------------
# A cluster end to end
# ---------------------------------------------------------------------------

def run_cluster(seed=7, days=120, archetype="e_commerce"):
    np.random.seed(seed)
    m = merchant(archetype, quality_percentile=0.35, merchant_id="m-cluster")
    m.fraud_cluster_until = DAY + timedelta(days=2)  # a three-day cluster starting today
    rows = []
    for d in range(days):
        day = DAY + timedelta(days=d)
        events = simulate_day(m, day)
        rows += evaluate_risk(m, events, day)
        m.decay_risk(day)
    return m, rows


def test_a_cluster_produces_the_same_day_rules_and_a_higher_risk_score():
    # A Spaza shop is busy enough for the hourly velocity rule to see a tripled day.
    m, rows = run_cluster(archetype="spaza_shop", days=30)
    assert rows
    for r in rows:
        RiskEvent(**r)
    first_week = [r for r in rows if r["action_ts"].date() <= DAY + timedelta(days=6)]
    assert {"large_txn_flag", "velocity_breach"} <= set(rules(first_week))
    assert max(r["risk_score"] for r in rows) > 25


def test_cluster_chargebacks_arrive_later_and_can_trip_the_slower_rules():
    m, rows = run_cluster()
    later = [r for r in rows if r["action_ts"].date() > DAY + timedelta(days=14)]
    assert {"refund_spike", "chargeback_accumulation"} & set(rules(later))


def test_the_same_seed_gives_the_same_risk_events():
    def fingerprint():
        m, rows = run_cluster(seed=11, days=60)
        return [(r["alert_id"], r["rule_type"], r["risk_score"]) for r in rows]

    assert fingerprint() == fingerprint()
