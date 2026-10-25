"""tests/test_state.py: Chain 2 state transitions and Chain 4 risk score rules."""
from datetime import date, timedelta

import numpy as np
import pytest

from simulator.state import Merchant, risk_delta

START = date(2025, 1, 1)


@pytest.fixture
def always(monkeypatch):
    """Every probability roll succeeds."""
    monkeypatch.setattr(np.random, "random", lambda: 0.0)


@pytest.fixture
def never(monkeypatch):
    """Every probability roll fails."""
    monkeypatch.setattr(np.random, "random", lambda: 0.999999)


def build(history, state="new", onboarded_days_ago=None) -> Merchant:
    """Merchant with one transaction per day carrying the given daily GPV values.
    The last recorded day is START + len(history) - 1."""
    m = Merchant(archetype="spaza_shop", merchant_state=state)
    m.onboarded_at = START if onboarded_days_ago is None else START - timedelta(days=onboarded_days_ago)
    for i, gpv in enumerate(history):
        m.record_day(START + timedelta(days=i), gpv, 1 if gpv > 0 else 0)
    return m


def today(m: Merchant) -> date:
    return START + timedelta(days=m.trading_days - 1)


# ---------------------------------------------------------------------------
# Construction and bookkeeping
# ---------------------------------------------------------------------------

def test_quality_percentile_drawn_at_creation():
    np.random.seed(1)
    values = {Merchant().quality_percentile for _ in range(20)}
    assert len(values) > 1
    assert all(0.0 < v < 1.0 for v in values)


def test_effective_tx_rate_uses_quality_multiplier():
    m = Merchant(archetype="spaza_shop", quality_percentile=0.5)
    assert m.effective_tx_rate() == pytest.approx(80.0 * (0.4 + 1.6 * 0.5))


def test_nothing_recorded_before_first_transaction():
    m = Merchant()
    m.record_day(START, 0, 0)
    assert len(m.daily_gpv_minor) == 0
    assert m.trading_days == 0
    m.record_day(START + timedelta(days=1), 5000, 3)
    assert m.first_transaction_at == START + timedelta(days=1)
    assert list(m.daily_gpv_minor) == [5000]


def test_history_buffer_capped_at_60_days():
    m = build([100] * 75)
    assert len(m.daily_gpv_minor) == 60
    assert m.trading_days == 75


def test_days_without_txn_counts_and_resets():
    m = build([100, 0, 0, 0])
    assert m.days_without_txn == 3
    m.record_day(START + timedelta(days=4), 100, 1)
    assert m.days_without_txn == 0


def test_stress_modifier_by_state():
    assert Merchant(merchant_state="growing").stress_modifier() == 1.05
    assert Merchant(merchant_state="stressed").stress_modifier() == 0.80
    assert Merchant(merchant_state="churned").stress_modifier() == 0.0


# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------

def test_no_transition_before_30_trading_days(always):
    m = build([100] * 29)
    assert m.evaluate_state(today(m)) is None
    assert m.merchant_state == "new"


def test_no_metrics_between_day_30_and_60(always):
    # Strong growth shape, but only 45 days exist, so no prior window.
    m = build([100] * 15 + [1000] * 30, onboarded_days_ago=0)
    assert m.trading_days == 45
    assert m.evaluate_state(today(m)) is None
    assert m.merchant_state == "new"


# ---------------------------------------------------------------------------
# new
# ---------------------------------------------------------------------------

def test_new_to_growing_at_day_60(always):
    m = build([100] * 30 + [150] * 30)
    assert m.evaluate_state(today(m)) == "growing"
    assert m.merchant_state == "growing"


def test_new_to_growing_roll_can_fail(never):
    m = build([100] * 30 + [150] * 30)
    assert m.evaluate_state(today(m)) is None
    assert m.merchant_state == "new"


def test_new_to_stable_needs_three_months_on_platform(always):
    flat = [100] * 60
    young = build(flat)  # 59 days on platform at evaluation: 1 month
    assert young.evaluate_state(today(young)) is None
    older = build(flat, onboarded_days_ago=40)  # 99 days: 3 months
    assert older.evaluate_state(today(older)) == "stable"


# ---------------------------------------------------------------------------
# growing and stable
# ---------------------------------------------------------------------------

def test_stable_to_stressed_on_decline(always):
    m = build([1000] * 30 + [500] * 30, state="stable")
    assert m.evaluate_state(today(m)) == "stressed"


def test_growing_to_stressed_on_decline(always):
    m = build([1000] * 30 + [500] * 30, state="growing")
    assert m.evaluate_state(today(m)) == "stressed"


def test_decline_below_threshold_does_not_stress(always):
    # c = 0.8p gives decline (p - c) / (p + c) of about 11%.
    m = build([1000] * 30 + [800] * 30, state="stable")
    assert m.evaluate_state(today(m)) is None


def test_baseline_includes_latest_window():
    # A 33% drop is the point where the 60-day decline reaches 20%.
    just_under = build([1000] * 30 + [680] * 30, state="stable")
    just_over = build([1000] * 30 + [660] * 30, state="stable")
    _, decline_under, _ = just_under._gpv_metrics()
    _, decline_over, _ = just_over._gpv_metrics()
    assert decline_under < 0.20 < decline_over


def test_stable_to_growing_needs_two_months_of_growth(always):
    m = build([100] * 30 + [150] * 30, state="stable")
    for _ in range(59):
        assert m.evaluate_state(today(m)) is None
    assert m.days_growth_high == 59
    assert m.evaluate_state(today(m)) == "growing"


def test_growing_to_stable_after_two_months_below_threshold(always):
    m = build([100] * 60, state="growing")
    for _ in range(59):
        assert m.evaluate_state(today(m)) is None
    assert m.days_growth_low == 59
    assert m.evaluate_state(today(m)) == "stable"


def test_transition_resets_month_counters(always):
    m = build([100] * 60, state="growing")
    for _ in range(60):
        m.evaluate_state(today(m))
    assert m.merchant_state == "stable"
    assert m.days_growth_low == 0 and m.days_growth_high == 0
    assert m.days_in_state == 0


# ---------------------------------------------------------------------------
# One transition per cycle, priority
# ---------------------------------------------------------------------------

def test_one_transition_per_cycle(always):
    # Decline is severe enough for Delinquent, but Stable can only reach Stressed.
    m = build([1000] * 30 + [100] * 30, state="stable")
    assert m.evaluate_state(today(m)) == "stressed"
    assert m.evaluate_state(today(m)) == "delinquent"


def test_failed_roll_does_not_fall_through_to_lower_priority(never):
    m = build([1000] * 30 + [500] * 30, state="stable")
    assert m.evaluate_state(today(m)) is None
    assert m.merchant_state == "stable"


# ---------------------------------------------------------------------------
# stressed, delinquent
# ---------------------------------------------------------------------------

def test_stressed_to_delinquent_on_deep_decline(always):
    m = build([1000] * 30 + [200] * 30, state="stressed")
    assert m.evaluate_state(today(m)) == "delinquent"


def test_stressed_to_delinquent_on_missed_repayment(always):
    m = build([1000] * 30 + [700] * 30, state="stressed")
    m.record_repayment_outcome(False, today(m))
    assert m.missed_repayment_flag
    assert m.evaluate_state(today(m)) == "delinquent"


def test_missed_repayment_flag_ignored_unless_stressed():
    m = build([100] * 60, state="stable")
    m.record_repayment_outcome(False, today(m))
    assert not m.missed_repayment_flag


def test_missed_repayment_flag_is_consumed(never):
    m = build([1000] * 30 + [700] * 30, state="stressed")
    m.record_repayment_outcome(False, today(m))
    m.evaluate_state(today(m))
    assert not m.missed_repayment_flag


def test_stressed_to_stable_on_recovery(always):
    m = build([700] * 30 + [1000] * 30, state="stressed")
    assert m.evaluate_state(today(m)) == "stable"


def test_stressed_stays_stressed_without_recovery(always):
    m = build([1000] * 30 + [650] * 30, state="stressed")
    assert m.evaluate_state(today(m)) is None


def test_delinquent_to_churned_after_30_dark_days(always):
    m = build([1000] * 60, state="delinquent")
    for i in range(30):
        m.record_day(START + timedelta(days=60 + i), 0, 0)
    assert m.days_without_txn == 30
    assert m.evaluate_state(today(m)) == "churned"


def test_delinquent_stays_while_still_trading(always):
    m = build([1000] * 60, state="delinquent")
    assert m.evaluate_state(today(m)) is None


def test_churned_is_terminal(always):
    m = build([0] * 60, state="churned")
    assert m.evaluate_state(today(m)) is None


# ---------------------------------------------------------------------------
# Chain 4: risk score
# ---------------------------------------------------------------------------

def test_repayment_success_lowers_risk():
    m = Merchant(risk_score=20)
    m.record_repayment_outcome(True, START)
    assert m.risk_score == 20 + risk_delta("repayment_successful")


def test_repayment_miss_raises_risk_and_resets_streak():
    m = Merchant(risk_score=20, consecutive_successful_repayments=2)
    m.record_repayment_outcome(False, START)
    assert m.risk_score == 28
    assert m.consecutive_successful_repayments == 0
    assert m.last_adverse_event_date == START


def test_third_consecutive_success_gets_bonus():
    m = Merchant(risk_score=30)
    for _ in range(3):
        m.record_repayment_outcome(True, START)
    assert m.risk_score == 30 - 2 - 2 - 2 - 5


def test_risk_score_clamped_to_0_and_100():
    low = Merchant(risk_score=1)
    low.apply_risk_delta(-10, START)
    assert low.risk_score == 0
    high = Merchant(risk_score=95)
    high.apply_risk_delta(20, START)
    assert high.risk_score == 100


def test_decay_one_point_per_30_day_period():
    m = Merchant(risk_score=40, onboarded_at=START)
    m.decay_risk(START + timedelta(days=29))
    assert m.risk_score == 40
    m.decay_risk(START + timedelta(days=30))
    assert m.risk_score == 39
    # Same period: no second decay.
    m.decay_risk(START + timedelta(days=31))
    assert m.risk_score == 39
    m.decay_risk(START + timedelta(days=60))
    assert m.risk_score == 38


def test_decay_accelerates_after_90_clean_days():
    m = Merchant(risk_score=40, onboarded_at=START)
    for day in (30, 60):
        m.decay_risk(START + timedelta(days=day))
    assert m.risk_score == 38
    m.decay_risk(START + timedelta(days=90))
    assert m.risk_score == 35


def test_adverse_event_restarts_decay_clock():
    m = Merchant(risk_score=40, onboarded_at=START)
    m.apply_risk_delta(10, START + timedelta(days=20))
    m.decay_risk(START + timedelta(days=30))
    assert m.risk_score == 50
    m.decay_risk(START + timedelta(days=50))
    assert m.risk_score == 49


def test_decay_floor_is_zero():
    m = Merchant(risk_score=0, onboarded_at=START)
    m.decay_risk(START + timedelta(days=30))
    assert m.risk_score == 0


def test_write_off_sets_permanent_floor():
    m = Merchant(risk_score=10, onboarded_at=START)
    m.write_off(START)
    assert m.is_written_off
    assert m.risk_score == 80
    for day in range(30, 400, 30):
        m.decay_risk(START + timedelta(days=day))
    assert m.risk_score == 80


def test_written_off_merchant_cannot_cure_below_floor():
    m = Merchant(risk_score=10, onboarded_at=START)
    m.write_off(START)
    m.apply_risk_delta(-20, START)
    assert m.risk_score == 80
