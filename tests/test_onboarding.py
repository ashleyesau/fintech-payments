"""tests/test_onboarding.py: the onboarding ramp, the archetype mix draw, activation and
replacement onboarding."""
from collections import Counter
from datetime import date, timedelta

import numpy as np
import pytest

from simulator.archetypes import ARCHETYPES
from simulator.config import load_calibration
from simulator.onboarding import (
    RAMP_DAYS,
    draw_activation,
    new_merchant,
    onboard_day,
    ramp_count,
    ramp_target,
    sample_archetype,
)

START = date(2025, 1, 1)


# ---------------------------------------------------------------------------
# The ramp
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("count", [7, 100, 1000, 1234])
def test_the_ramp_onboards_exactly_the_merchant_count_over_six_months(count):
    daily = [ramp_count(d, count) for d in range(RAMP_DAYS)]
    assert sum(daily) == count
    assert all(n >= 0 for n in daily)
    assert ramp_target(RAMP_DAYS - 1, count) == count


@pytest.mark.parametrize("count", [100, 1000])
def test_the_ramp_follows_the_monthly_shares(count):
    cumulative = 0.0
    for month in range(1, 7):
        cumulative += load_calibration()["onboarding_ramp"][f"month_{month}_pct"]
        assert ramp_target(month * 30 - 1, count) == round(count * cumulative)


def test_a_quarter_of_a_run_onboards_the_first_three_months_share():
    assert ramp_target(89, 1000) == 530  # 10% + 18% + 25% = 53%


def test_nothing_is_onboarded_by_the_ramp_after_month_six_or_before_the_start():
    assert ramp_count(RAMP_DAYS, 1000) == 0 and ramp_count(RAMP_DAYS + 90, 1000) == 0
    assert ramp_target(-1, 1000) == 0


def test_ramp_counts_do_not_use_the_rng():
    np.random.seed(1)
    first = [ramp_count(d, 500) for d in range(RAMP_DAYS)]
    np.random.seed(2)
    assert [ramp_count(d, 500) for d in range(RAMP_DAYS)] == first


def test_the_busiest_month_is_the_third():
    monthly = [sum(ramp_count(d, 1000) for d in range(m * 30, m * 30 + 30)) for m in range(6)]
    assert monthly == [100, 180, 250, 200, 150, 120]


# ---------------------------------------------------------------------------
# onboard_day and replacement
# ---------------------------------------------------------------------------

def test_onboard_day_follows_the_ramp():
    np.random.seed(3)
    for d in (0, 10, 100):
        merchants = onboard_day(START + timedelta(days=d), START, 1000)
        assert len(merchants) == ramp_count(d, 1000)
        assert all(m.onboarded_at == START + timedelta(days=d) for m in merchants)


def test_replacements_are_ignored_during_the_ramp():
    np.random.seed(3)
    day = START + timedelta(days=RAMP_DAYS - 1)
    assert len(onboard_day(day, START, 1000, replacements=5)) == ramp_count(RAMP_DAYS - 1, 1000)


def test_from_month_seven_each_churned_merchant_is_replaced_one_for_one():
    np.random.seed(3)
    first_day = START + timedelta(days=RAMP_DAYS)
    assert len(onboard_day(first_day, START, 1000, replacements=3)) == 3
    assert len(onboard_day(first_day + timedelta(days=50), START, 1000, replacements=0)) == 0


def test_a_whole_run_onboards_the_ramp_plus_replacements():
    np.random.seed(3)
    total = 0
    replaced = 0
    for d in range(365):
        replacements = 2 if d % 10 == 0 else 0
        total += len(onboard_day(START + timedelta(days=d), START, 200, replacements))
        if d >= RAMP_DAYS:
            replaced += replacements
    assert total == 200 + replaced


def test_the_same_seed_gives_the_same_merchants():
    np.random.seed(8)
    first = onboard_day(START + timedelta(days=40), START, 1000)
    np.random.seed(8)
    assert onboard_day(START + timedelta(days=40), START, 1000) == first
    assert len({m.merchant_id for m in first}) == len(first)


# ---------------------------------------------------------------------------
# One merchant
# ---------------------------------------------------------------------------

def test_a_new_merchant_has_its_creation_fields():
    np.random.seed(4)
    m = new_merchant(START)
    assert m.archetype in ARCHETYPES
    assert m.onboarded_at == START and m.merchant_state == "new"
    assert 0.0 < m.quality_percentile < 1.0
    assert m.province and isinstance(m.is_metro, bool)
    assert m.first_transaction_at is None and m.trading_days == 0


def test_an_activator_trades_from_its_activation_date_and_not_before():
    np.random.seed(4)
    activators = [m for m in (new_merchant(START) for _ in range(50)) if not m.never_activates]
    assert activators
    for m in activators:
        assert m.activation_date > START
        assert not m.can_trade(m.activation_date - timedelta(days=1))
        assert m.can_trade(m.activation_date)


def test_a_never_activator_has_no_activation_date_and_is_abandoned_at_day_30():
    np.random.seed(4)
    never = [m for m in (new_merchant(START) for _ in range(50)) if m.never_activates]
    assert never
    for m in never:
        assert m.activation_date is None and not m.can_trade(START + timedelta(days=100))
        assert m.abandon_onboarding(START + timedelta(days=29)) is False
        assert m.abandon_onboarding(START + timedelta(days=30)) is True


# ---------------------------------------------------------------------------
# The draws, over a large seeded sample
# ---------------------------------------------------------------------------

SAMPLE = 6000


@pytest.fixture(scope="module")
def population():
    np.random.seed(21)
    return [new_merchant(START) for _ in range(SAMPLE)]


def test_archetype_shares_follow_the_mix(population):
    counts = Counter(m.archetype for m in population)
    for name, share in load_calibration()["archetype_mix"].items():
        assert counts[name] / SAMPLE == pytest.approx(share, abs=0.02), name


def test_province_follows_the_archetype_distribution(population):
    from simulator.archetypes import _PROVINCE_DISTRIBUTIONS

    spaza = [m for m in population if m.archetype == "spaza_shop"]
    counts = Counter(m.province for m in spaza)
    for province, share in _PROVINCE_DISTRIBUTIONS["spaza_shop"].items():
        assert counts[province] / len(spaza) == pytest.approx(share / sum(_PROVINCE_DISTRIBUTIONS["spaza_shop"].values()), abs=0.05), province


def test_quality_percentile_follows_the_beta_distribution(population):
    quality = np.array([m.quality_percentile for m in population])
    assert quality.mean() == pytest.approx(1.5 / 5.5, abs=0.01)


def test_about_two_thirds_of_merchants_ever_transact(population):
    activating = sum(not m.never_activates for m in population) / SAMPLE
    assert activating == pytest.approx(load_calibration()["lifecycle"]["activation_share"], abs=0.02)


def test_activation_meets_the_seven_day_and_median_targets(population):
    delays = [(m.activation_date - m.onboarded_at).days for m in population if not m.never_activates]
    within_week = sum(d <= 7 for d in delays) / SAMPLE
    assert 0.55 <= within_week <= 0.75  # Validation Targets: active within 7 days
    assert 2 <= float(np.median(delays)) <= 5  # Validation Targets: median days to first transaction
    assert min(delays) >= 1 and max(delays) <= 30


def test_nine_in_ten_activators_start_in_the_first_week_and_the_rest_in_the_late_range(population):
    delays = [(m.activation_date - m.onboarded_at).days for m in population if not m.never_activates]
    assert sum(d <= 7 for d in delays) / len(delays) == pytest.approx(0.90, abs=0.02)
    low, high = load_calibration()["lifecycle"]["activation_delay"]["late_days"]
    late = [d for d in delays if d > 7]
    assert late and min(late) >= low and max(late) <= high


def test_draw_activation_reports_never_activators_with_no_delay():
    np.random.seed(5)
    draws = [draw_activation() for _ in range(200)]
    assert all((delay is None) == never for never, delay in draws)


def test_sample_archetype_only_returns_archetypes_in_the_mix():
    np.random.seed(6)
    assert {sample_archetype() for _ in range(300)} <= set(load_calibration()["archetype_mix"])
