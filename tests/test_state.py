"""tests/test_state.py: Chain 2 state transitions and Chain 4 risk score rules."""
from datetime import date, timedelta

import numpy as np
import pytest

from simulator.state import Loan, Merchant, risk_delta

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


def test_history_buffer_capped_at_90_days():
    m = build([100] * 120)
    assert len(m.daily_gpv_minor) == 90
    assert m.trading_days == 120


def test_gpv_90d_sums_the_buffer():
    assert build([100] * 40).gpv_90d_minor() == 4000
    # Only the latest 90 days count.
    assert build([1000] * 30 + [100] * 90).gpv_90d_minor() == 9000


def test_state_metrics_use_only_the_latest_60_days():
    # 30 old days of 5000 must not leak into the 60-day windows.
    m = build([5000] * 30 + [1000] * 30 + [500] * 30)
    growth, decline, current = m._gpv_metrics()
    assert current == 500
    assert growth == pytest.approx(-0.5)
    assert decline == pytest.approx((750 - 500) / 750)


def test_stressed_recovery_uses_latest_60_days_with_a_longer_buffer(always):
    # Latest 60: 700 then 1000, which recovers. The old 90-day-ago spike must be ignored.
    m = build([9000] * 30 + [700] * 30 + [1000] * 30, state="stressed")
    assert m.evaluate_state(today(m)) == "stable"


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


def test_delinquent_does_not_churn_while_still_trading(always):
    # GPV is still weak, so no recovery either; only 30 dark days churn a merchant.
    m = build([1000] * 30 + [650] * 30, state="delinquent")
    assert m.evaluate_state(today(m)) is None


def test_new_merchant_that_goes_dark_can_churn(always):
    # No path out of New by GPV decline exists in Chain 2, so a dark merchant needs this.
    m = build([1000] * 30, state="new")
    for i in range(30, 60):
        m.record_day(START + timedelta(days=i), 0, 0)
    assert m.days_without_txn == 30
    assert m.evaluate_state(today(m)) == "churned"


def test_new_merchant_still_trading_does_not_churn(always):
    m = build([100] * 60, state="new")
    assert m.evaluate_state(today(m)) != "churned"


def test_delinquent_merchant_with_recovered_gpv_steps_back_to_stressed(always):
    m = build([700] * 30 + [1000] * 30, state="delinquent")
    assert m.evaluate_state(today(m)) == "stressed"


def test_delinquent_stays_while_gpv_has_not_recovered(always):
    m = build([1000] * 30 + [650] * 30, state="delinquent")
    assert m.evaluate_state(today(m)) is None


def test_an_arrears_loan_does_not_block_a_delinquent_merchants_recovery(always):
    from simulator.state import Loan

    m = build([700] * 30 + [1000] * 30, state="delinquent")
    m.active_loan = Loan(application_id="a", unpaid_installments=[1])
    assert m.evaluate_state(today(m)) == "stressed"


def test_dormant_delinquent_merchant_does_not_recover(always):
    m = build([1000] * 60, state="delinquent")
    for i in range(60, 75):
        m.record_day(START + timedelta(days=i), 0, 0)
    assert m.evaluate_state(today(m)) is None  # GPV is falling, and 30 dark days not yet reached


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


# ---------------------------------------------------------------------------
# Loan identity
# ---------------------------------------------------------------------------

def test_loan_id_is_application_id():
    loan = Loan(application_id="app-1")
    assert loan.loan_id == "app-1"


def test_loan_generates_one_id_when_none_given():
    loan = Loan()
    assert loan.application_id and loan.loan_id == loan.application_id


# ---------------------------------------------------------------------------
# Dormancy (monthly churn hazard)
# ---------------------------------------------------------------------------

def test_dormancy_needs_a_first_transaction():
    m = Merchant(archetype="spaza_shop", onboarded_at=START)
    assert m.maybe_go_dormant(START) is False
    assert m.dormant_since is None


def test_dormancy_hits_when_the_roll_succeeds(always):
    m = build([100] * 5)
    assert m.maybe_go_dormant(today(m)) is True
    assert m.dormant_since == today(m)


def test_a_dormant_merchant_closed_or_moved_acquirer(monkeypatch):
    # First roll: dormancy hit; second: closure draw against dormant_closure_share (0.25).
    for closure_roll, closed in ((0.0, True), (0.9, False)):
        rolls = iter([0.0, closure_roll])
        monkeypatch.setattr(np.random, "random", lambda: next(rolls))
        m = build([100] * 5)
        assert m.maybe_go_dormant(today(m)) is True
        assert m.business_closed is closed


def test_closure_is_undecided_until_the_merchant_goes_dormant():
    assert build([100] * 5).business_closed is None


def test_dormancy_misses_when_the_roll_fails(never):
    m = build([100] * 5)
    assert m.maybe_go_dormant(today(m)) is False
    assert m.dormant_since is None


def test_dormant_merchant_is_not_rolled_again(always):
    m = build([100] * 5)
    m.maybe_go_dormant(today(m))
    first = m.dormant_since
    assert m.maybe_go_dormant(today(m) + timedelta(days=1)) is False
    assert m.dormant_since == first


# ---------------------------------------------------------------------------
# Transition probabilities are daily hazards (Phase 8, state snapping)
# ---------------------------------------------------------------------------

def test_a_window_probability_compounds_over_30_days():
    from simulator.state import daily_hazard

    for p in (0.30, 0.65, 0.85):
        assert 1 - (1 - daily_hazard(p)) ** 30 == pytest.approx(p)
    assert daily_hazard(0.0) == 0.0


def test_a_met_condition_rolls_the_daily_hazard_not_the_window_probability(monkeypatch):
    # Stable to Stressed is 0.65 over a window. A roll of 0.5 used to pass it on the
    # day the condition appeared; now it needs a roll under the daily hazard (about 0.034).
    from simulator.state import daily_hazard

    hazard = daily_hazard(0.65)
    history = [1000] * 30 + [500] * 30
    monkeypatch.setattr(np.random, "random", lambda: 0.5)
    m = build(history, state="stable")
    assert m.evaluate_state(today(m)) is None
    monkeypatch.setattr(np.random, "random", lambda: hazard * 0.999)
    m = build(history, state="stable")
    assert m.evaluate_state(today(m)) == "stressed"
    monkeypatch.setattr(np.random, "random", lambda: hazard * 1.001)
    m = build(history, state="stable")
    assert m.evaluate_state(today(m)) is None


def test_the_missed_repayment_trigger_is_rolled_once_at_its_full_probability(monkeypatch):
    # A single event, not a condition that persists, so it is not spread over a window.
    history = [1000] * 30 + [700] * 30
    monkeypatch.setattr(np.random, "random", lambda: 0.89)
    m = build(history, state="stressed")
    m.record_repayment_outcome(False, today(m))
    assert m.evaluate_state(today(m)) == "delinquent"
    monkeypatch.setattr(np.random, "random", lambda: 0.91)
    m = build(history, state="stressed")
    m.record_repayment_outcome(False, today(m))
    assert m.evaluate_state(today(m)) is None


def _busiest_week_share(count=400, seed=3):
    """400 Stable merchants whose daily GPV falls 40% on the same day. Returns the largest
    share of them that first become Stressed in any one week."""
    from collections import Counter

    np.random.seed(seed)
    merchants = [build([1000] * 60, state="stable") for _ in range(count)]
    day = today(merchants[0])
    first = {}
    for offset in range(1, 61):
        day += timedelta(days=1)
        for i, m in enumerate(merchants):
            m.record_day(day, 600, 1)
            if i not in first and m.evaluate_state(day) == "stressed":
                first[i] = offset
    weeks = Counter((offset - 1) // 7 for offset in first.values())
    return max(weeks.values()) / count


@pytest.mark.slow
def test_a_seasonal_drop_does_not_move_most_merchants_in_one_week(monkeypatch):
    assert _busiest_week_share() < 0.30
    # The control: the same drop with the old daily roll (no hazard) moves nearly all of
    # them in one week, which is the failure mode.
    monkeypatch.setattr("simulator.state.daily_hazard", lambda p: p)
    assert _busiest_week_share() > 0.90


def test_daily_hazard_compounds_to_the_monthly_probability(monkeypatch):
    # A roll just under the daily hazard hits, just over misses, and 30 days of that
    # hazard add up to monthly_churn_prob.
    from simulator.archetypes import get_archetype

    monthly = get_archetype("spaza_shop").monthly_churn_prob
    daily = 1 - (1 - monthly) ** (1 / 30)

    monkeypatch.setattr(np.random, "random", lambda: daily * 0.999)
    assert build([100] * 5).maybe_go_dormant(START) is True
    monkeypatch.setattr(np.random, "random", lambda: daily * 1.001)
    assert build([100] * 5).maybe_go_dormant(START) is False
    assert 1 - (1 - daily) ** 30 == pytest.approx(monthly)


@pytest.mark.slow
def test_dormancy_rate_over_a_year_matches_archetype():
    np.random.seed(8)
    gone = 0
    n = 3000
    for _ in range(n):
        m = build([100] * 2, onboarded_days_ago=0)
        m.archetype = "e_commerce"  # 5% a month
        for d in range(360):
            if m.maybe_go_dormant(START + timedelta(days=d)):
                gone += 1
                break
    assert gone / n == pytest.approx(1 - 0.95 ** 12, abs=0.03)


def test_dormant_merchant_walks_down_to_churned(always):
    # A merchant that has stopped trading reaches Churned through the normal chain.
    m = build([1000] * 60, state="stable")
    m.dormant_since = START
    seen = []
    for i in range(60, 200):
        m.record_day(START + timedelta(days=i), 0, 0)
        moved = m.evaluate_state(START + timedelta(days=i))
        if moved:
            seen.append(moved)
        if m.merchant_state == "churned":
            break
    assert seen == ["stressed", "delinquent", "churned"]


# ---------------------------------------------------------------------------
# merchants dimension snapshot
# ---------------------------------------------------------------------------

def test_snapshot_validates_against_the_merchants_model():
    from datetime import datetime, timezone

    from simulator.models import MerchantSnapshot

    m = build([100] * 5, state="growing")
    m.province, m.is_metro, m.risk_score, m.loan_count, m.has_active_loan = "western_cape", False, 22, 1, True
    at = datetime(2025, 3, 1, 23, 59, tzinfo=timezone.utc)
    row = m.snapshot(at)
    MerchantSnapshot(**row)
    assert row["merchant_state"] == "growing" and row["risk_score"] == 22
    assert row["loan_count"] == 1 and row["has_active_loan"] is True
    assert row["onboarded_at"] == datetime(2025, 1, 1, tzinfo=timezone.utc)
    assert row["first_transaction_at"] == datetime(2025, 1, 1, tzinfo=timezone.utc)
    assert row["snapshot_at"] == at


def test_snapshot_before_first_transaction_has_no_first_transaction_time():
    from datetime import datetime, timezone

    row = Merchant(archetype="spaza_shop", onboarded_at=START).snapshot(datetime(2025, 1, 2, tzinfo=timezone.utc))
    assert row["first_transaction_at"] is None


# ---------------------------------------------------------------------------
# Identifiers from the seeded RNG
# ---------------------------------------------------------------------------

def test_merchant_ids_come_from_the_seeded_rng():
    np.random.seed(5)
    first = [Merchant().merchant_id for _ in range(5)]
    np.random.seed(5)
    second = [Merchant().merchant_id for _ in range(5)]
    assert first == second
    assert len(set(first)) == 5


def test_loan_fallback_id_comes_from_the_seeded_rng():
    np.random.seed(5)
    first = Loan().loan_id
    np.random.seed(5)
    assert Loan().loan_id == first
    assert first != ""


# ---------------------------------------------------------------------------
# Activation and abandoning onboarding
# ---------------------------------------------------------------------------

def test_a_merchant_without_an_activation_date_can_trade_from_onboarding():
    m = Merchant(archetype="spaza_shop", onboarded_at=START)
    assert m.can_trade(START) and m.can_trade(START + timedelta(days=400))


def test_a_merchant_cannot_trade_before_its_activation_date():
    m = Merchant(archetype="spaza_shop", onboarded_at=START, activation_date=START + timedelta(days=3))
    assert not m.can_trade(START + timedelta(days=2))
    assert m.can_trade(START + timedelta(days=3))


def test_a_never_activator_never_trades():
    m = Merchant(archetype="spaza_shop", onboarded_at=START, never_activates=True)
    assert not any(m.can_trade(START + timedelta(days=d)) for d in (0, 30, 400))


def test_a_never_activator_churns_on_the_abandonment_day_and_not_before():
    m = Merchant(archetype="spaza_shop", onboarded_at=START, never_activates=True)
    assert m.abandon_onboarding(START + timedelta(days=29)) is False
    assert m.merchant_state == "new"
    assert m.abandon_onboarding(START + timedelta(days=30)) is True
    assert m.merchant_state == "churned" and m.churned_at == START + timedelta(days=30)
    assert m.abandon_onboarding(START + timedelta(days=31)) is False  # once only


def test_abandonment_window_is_a_parameter():
    m = Merchant(archetype="spaza_shop", onboarded_at=START, never_activates=True)
    assert m.abandon_onboarding(START + timedelta(days=9), days=10) is False
    assert m.abandon_onboarding(START + timedelta(days=10), days=10) is True


def test_an_activator_is_never_abandoned():
    m = Merchant(archetype="spaza_shop", onboarded_at=START, activation_date=START + timedelta(days=40))
    assert m.abandon_onboarding(START + timedelta(days=100)) is False
    assert m.merchant_state == "new"


def test_a_never_activator_generates_no_trading_days_and_stays_new_until_abandoned():
    m = Merchant(archetype="spaza_shop", onboarded_at=START, never_activates=True)
    for d in range(30):
        day = START + timedelta(days=d)
        assert not m.can_trade(day)
        assert m.evaluate_state(day) is None
    assert m.trading_days == 0 and m.merchant_state == "new"


def test_churning_through_the_state_chain_records_the_churn_date(always):
    m = build([1000] * 60, state="delinquent")
    for i in range(30):
        m.record_day(START + timedelta(days=60 + i), 0, 0)
    assert m.evaluate_state(today(m)) == "churned"
    assert m.churned_at == today(m)


def test_other_transitions_leave_churned_at_empty(always):
    m = build([1000] * 30 + [650] * 30, state="stable")
    assert m.evaluate_state(today(m)) == "stressed"
    assert m.churned_at is None


# ---------------------------------------------------------------------------
# Snapshot window for churned merchants
# ---------------------------------------------------------------------------

CHURN_DAY = date(2025, 6, 1)


def churned_merchant(**kwargs) -> Merchant:
    m = Merchant(archetype="spaza_shop", onboarded_at=START, merchant_state="churned",
                 churned_at=CHURN_DAY, **kwargs)
    return m


def test_snapshots_continue_for_merchants_that_have_not_churned():
    assert Merchant(archetype="spaza_shop", onboarded_at=START).snapshot_due(date(2030, 1, 1))


def test_a_churned_merchant_is_snapshotted_through_the_window_and_not_after():
    m = churned_merchant()
    assert m.snapshot_due(CHURN_DAY)
    assert m.snapshot_due(CHURN_DAY + timedelta(days=30))
    assert not m.snapshot_due(CHURN_DAY + timedelta(days=31))


def test_the_snapshot_window_is_a_parameter():
    m = churned_merchant()
    assert m.snapshot_due(CHURN_DAY + timedelta(days=7), window_days=7)
    assert not m.snapshot_due(CHURN_DAY + timedelta(days=8), window_days=7)


def test_a_churned_merchant_with_an_active_loan_keeps_its_snapshots():
    m = churned_merchant(has_active_loan=True)
    assert m.snapshot_due(CHURN_DAY + timedelta(days=400))


def test_the_window_restarts_when_the_loan_ends():
    ended = CHURN_DAY + timedelta(days=100)
    m = churned_merchant(has_active_loan=False, loan_closed_on=ended)
    assert m.snapshot_due(CHURN_DAY + timedelta(days=99))  # inside the loan, still due
    assert m.snapshot_due(ended + timedelta(days=30))
    assert not m.snapshot_due(ended + timedelta(days=31))


def test_a_loan_that_ended_before_the_churn_does_not_move_the_window():
    m = churned_merchant(loan_closed_on=CHURN_DAY - timedelta(days=50))
    assert not m.snapshot_due(CHURN_DAY + timedelta(days=31))


def test_a_churned_merchant_with_no_churn_date_keeps_its_snapshots():
    assert Merchant(archetype="spaza_shop", merchant_state="churned").snapshot_due(date(2030, 1, 1))
