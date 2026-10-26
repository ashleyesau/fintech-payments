"""tests/test_calibration_config.py: onboarding mix and lifecycle parameters in
calibration.yaml, checked against the archetype list and the Validation Targets bands
they were chosen to meet."""
import pytest

from simulator.archetypes import ARCHETYPES
from simulator.config import load_calibration


@pytest.fixture(scope="module")
def calibration() -> dict:
    return load_calibration()


@pytest.fixture(scope="module")
def lifecycle(calibration) -> dict:
    return calibration["lifecycle"]


def test_archetype_mix_covers_every_archetype_and_nothing_else(calibration):
    assert set(calibration["archetype_mix"]) == set(ARCHETYPES)


def test_archetype_mix_shares_are_positive_and_sum_to_one(calibration):
    shares = calibration["archetype_mix"].values()
    assert all(share > 0 for share in shares)
    assert sum(shares) == pytest.approx(1.0)


def test_onboarding_ramp_covers_the_whole_merchant_count(calibration):
    assert sum(calibration["onboarding_ramp"].values()) == pytest.approx(1.0)


def test_mix_keeps_platform_monthly_churn_inside_its_band(calibration):
    """Validation Targets: platform blended monthly churn 2-5%."""
    mix = calibration["archetype_mix"]
    churn = sum(share * ARCHETYPES[name].monthly_churn_prob for name, share in mix.items())
    assert 0.02 <= churn <= 0.05


def test_mix_keeps_platform_refund_rate_inside_its_band(calibration):
    """Validation Targets: platform refund rate 0.5-3.0%, weighted by transactions."""
    mix = calibration["archetype_mix"]
    count = refunds = 0.0
    for name, share in mix.items():
        a = ARCHETYPES[name]
        daily = share * a.tx_per_day * len(a.open_days) / 7 * sum(a.monthly_seasonal_multipliers) / 12
        count += daily
        refunds += daily * a.refund_rate
    assert 0.005 <= refunds / count <= 0.03


def test_activation_day_weights_are_a_distribution_over_seven_days(lifecycle):
    weights = lifecycle["activation_delay"]["day_weights"]
    assert len(weights) == 7
    assert all(w >= 0 for w in weights)
    assert sum(weights) == pytest.approx(1.0)


def test_late_activation_range_starts_after_the_first_week(lifecycle):
    low, high = lifecycle["activation_delay"]["late_days"]
    assert 8 <= low <= high


def test_activation_meets_the_seven_day_target(lifecycle):
    """Validation Targets: 55-75% of onboarded merchants active within 7 days."""
    within_week = lifecycle["activation_share"] * lifecycle["activation_delay"]["within_7_days_share"]
    assert 0.55 <= within_week <= 0.75


def test_median_days_to_first_transaction_is_inside_its_band(lifecycle):
    """Validation Targets: median 2-5 days, taken over merchants that activate."""
    delay = lifecycle["activation_delay"]
    cumulative = 0.0
    median = None
    for day, weight in enumerate(delay["day_weights"], start=1):
        cumulative += weight * delay["within_7_days_share"]
        if cumulative >= 0.5:
            median = day
            break
    assert median is not None
    assert 2 <= median <= 5


def test_activation_leaves_about_a_third_never_transacting(lifecycle):
    assert 0.30 <= 1 - lifecycle["activation_share"] <= 0.36


def test_retention_with_activation_sits_inside_the_m6_and_m12_bands(calibration, lifecycle):
    """Survival from the mix's monthly churn, times the share that ever transacts.

    Validation Targets: logo retention 45-65% at M6 and 30-50% at M12, counting every
    onboarded merchant. A back-of-envelope check; Phase 8 measures it on a run.
    """
    mix = calibration["archetype_mix"]
    churn = sum(share * ARCHETYPES[name].monthly_churn_prob for name, share in mix.items())
    activation = lifecycle["activation_share"]
    assert 0.45 <= activation * (1 - churn) ** 6 <= 0.65
    assert 0.30 <= activation * (1 - churn) ** 12 <= 0.50


def test_abandonment_and_snapshot_windows_are_positive_whole_days(lifecycle):
    assert isinstance(lifecycle["onboarding_abandonment_days"], int)
    assert lifecycle["onboarding_abandonment_days"] > 0
    assert isinstance(lifecycle["churned_snapshot_days"], int)
    assert lifecycle["churned_snapshot_days"] > 0
