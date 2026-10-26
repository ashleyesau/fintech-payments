"""tests/test_archetype_data.py: archetypes.yaml against its source tables.

The Causal Dependency Rules Chain 1 tables are copied here as the authority for
spaza_shop, restaurant, e_commerce and wholesale. Simulator Design section 3 is the
authority for ticket, rate and churn parameters.
"""
import numpy as np
import pytest

from simulator.archetypes import ARCHETYPES, get_archetype, risk_index, sample_risk_baseline

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# Causal Rules, Chain 1: weekday weights (Mon..Sun).
CAUSAL_WEEKDAY = {
    "spaza_shop": [0.90, 0.95, 1.00, 1.00, 1.10, 1.15, 0.90],
    "restaurant": [0.70, 0.75, 0.85, 0.90, 1.10, 1.30, 1.30],
    "e_commerce": [1.00, 1.00, 1.05, 1.10, 1.15, 1.05, 0.90],
    "wholesale": [1.10, 1.10, 1.10, 1.10, 1.10, 0.00, 0.00],
}

# Causal Rules, Chain 1: hourly weights (hour 0..23). The doc says they sum to 1.0 per
# archetype; the printed table does not (1.04, 1.21, 1.18), so only the shape is used.
CAUSAL_HOURLY = {
    "spaza_shop": [0, 0, 0, 0, 0, 0, .03, .06, .08, .08, .07, .07, .08, .08, .07, .07, .08, .08, .07, .05, .03, .02, .01, .01],
    "restaurant": [0, 0, 0, 0, 0, 0, 0, .02, .04, .05, .06, .08, .14, .13, .07, .06, .06, .07, .10, .12, .10, .07, .03, .01],
    "e_commerce": [.02, .02, .02, .02, .02, .02, .02, .03, .04, .06, .07, .08, .08, .08, .08, .08, .07, .07, .07, .07, .06, .05, .03, .02],
    "wholesale": [0, 0, 0, 0, 0, 0, .05, .10, .15, .15, .12, .10, .08, .07, .06, .05, .04, .03, 0, 0, 0, 0, 0, 0],
}

# Causal Rules, Chain 1: monthly seasonal modifiers (Jan..Dec).
CAUSAL_SEASONAL = {
    "spaza_shop": [1.02, 0.95, 0.98, 0.97, 0.98, 0.99, 0.99, 1.00, 0.99, 1.00, 1.05, 1.18],
    "restaurant": [0.95, 0.97, 1.00, 1.00, 0.98, 0.92, 0.93, 0.96, 1.00, 1.05, 1.08, 1.20],
    "e_commerce": [0.92, 0.94, 0.97, 0.95, 0.98, 0.97, 0.96, 0.99, 0.98, 1.00, 1.16, 1.30],
    "wholesale": [0.90, 0.95, 1.00, 0.98, 1.00, 0.98, 0.97, 1.00, 1.02, 1.05, 1.08, 0.85],
}

# Simulator Design section 3: (ticket, std, tx/day, refund, fraud, monthly churn).
DESIGN_TABLE = {
    "spaza_shop": (50, 20, 80, 0.005, 0.001, 0.02),
    "restaurant": (150, 50, 60, 0.015, 0.003, 0.03),
    "salon": (200, 70, 30, 0.010, 0.002, 0.025),
    "grocery": (100, 30, 120, 0.008, 0.001, 0.015),
    "liquor_store": (250, 100, 40, 0.020, 0.005, 0.04),
    "fuel_station": (80, 20, 150, 0.002, 0.0005, 0.01),
    "e_commerce": (500, 150, 20, 0.050, 0.010, 0.05),
    "wholesale": (1500, 500, 10, 0.001, 0.001, 0.005),
    "salon_low_cost": (80, 20, 35, 0.005, 0.001, 0.02),
    "tutor": (300, 100, 5, 0.100, 0.002, 0.03),
    "seasonal_retail": (400, 200, 25, 0.050, 0.005, 0.07),
}


@pytest.mark.parametrize("name", CAUSAL_WEEKDAY)
def test_weekday_weights_keep_causal_rules_proportions(name):
    # Weights are normalised to mean 1.0 over open days, so compare ratios.
    raw = np.array(CAUSAL_WEEKDAY[name])
    loaded = np.array([get_archetype(name).weekday_weights[d] for d in DAYS])
    open_days = raw > 0
    assert list(loaded > 0) == list(open_days)
    assert loaded[open_days] / raw[open_days] == pytest.approx(
        np.full(open_days.sum(), loaded[open_days][0] / raw[open_days][0])
    )


@pytest.mark.parametrize("name", CAUSAL_HOURLY)
def test_hourly_weights_keep_causal_rules_shape(name):
    expected = np.array(CAUSAL_HOURLY[name], dtype=float)
    loaded = np.array(get_archetype(name).hourly_weights)
    assert len(loaded) == 24
    assert loaded / loaded.sum() == pytest.approx(expected / expected.sum())


@pytest.mark.parametrize("name", CAUSAL_SEASONAL)
def test_seasonal_multipliers_match_causal_rules(name):
    assert list(get_archetype(name).monthly_seasonal_multipliers) == pytest.approx(CAUSAL_SEASONAL[name])


@pytest.mark.parametrize("name", DESIGN_TABLE)
def test_parameters_match_simulator_design_table(name):
    a = get_archetype(name)
    ticket, std, tx, refund, fraud, churn = DESIGN_TABLE[name]
    assert (a.mean_ticket_zar, a.ticket_std_zar, a.tx_per_day) == (ticket, std, tx)
    assert (a.refund_rate, a.fraud_rate, a.monthly_churn_prob) == pytest.approx((refund, fraud, churn))


def test_every_archetype_is_covered_by_the_design_table():
    assert set(DESIGN_TABLE) == set(ARCHETYPES)


# Validation Targets, transaction distribution checks.

def test_spaza_saturday_is_20_to_30_percent_above_monday():
    w = get_archetype("spaza_shop").weekday_weights
    assert 1.20 <= w["Sat"] / w["Mon"] <= 1.30


def test_wholesale_trades_no_weekend_days():
    w = get_archetype("wholesale").weekday_weights
    assert w["Sat"] == 0.0 and w["Sun"] == 0.0


def _share(name: str, hours: range) -> float:
    h = np.array(get_archetype(name).hourly_weights)
    return float(h[list(hours)].sum() / h.sum())


def test_restaurant_lunch_and_dinner_shares():
    assert 0.20 <= _share("restaurant", range(12, 14)) <= 0.28
    assert 0.25 <= _share("restaurant", range(18, 21)) <= 0.35


def test_hourly_distribution_is_not_flat():
    # Validation Targets: platform-wide hourly distribution must fail a uniform test.
    for name in ARCHETYPES:
        h = np.array(get_archetype(name).hourly_weights, dtype=float)
        assert (h == 0).any() or h.max() / h.min() > 3


def test_ecommerce_evening_share_sits_in_the_amended_validation_band():
    # Causal Rules' hourly table gives 21-24% for 18:00-22:00; Validation Targets said
    # 25-35%. Causal Rules ranks higher, so the Validation band was amended to 21-24%
    # (Phase 8, with a note on that page). This guards the table against drifting out of it.
    share = _share("e_commerce", range(18, 22))
    assert 0.21 <= share <= 0.24


# ---------------------------------------------------------------------------
# Baseline risk score (Phase 8): archetype risk index and the Validation distribution
# ---------------------------------------------------------------------------

def test_risk_index_ranks_archetypes_by_their_refund_fraud_and_churn_rates():
    index = {name: risk_index(name) for name in ARCHETYPES}
    assert all(0.0 <= v <= 1.0 for v in index.values())
    ranked = sorted(index, key=index.get)
    assert set(ranked[:2]) == {"fuel_station", "wholesale"}
    assert set(ranked[-2:]) == {"e_commerce", "seasonal_retail"}
    assert index["spaza_shop"] < index["restaurant"] < index["liquor_store"]


# Risk events add to a score and decay a point a month, so the end-of-run distribution is
# the baseline plus an excess. Percentiles of (score - baseline) over 1,030 active merchants
# from three 500-merchant, 15-month runs, session 6 (median 0, mean 6.5).
EXCESS_PERCENTILES = [(0, 0), (50, 0), (60, 0), (70, 4), (80, 10), (90, 22), (95, 35), (99, 68), (100, 94)]


def test_baseline_plus_risk_events_lands_in_the_validation_buckets():
    """Validation Targets: 0-30 55 to 70%, 31-55 15 to 25%, 56-75 8 to 15%, 76-100 3 to 8%
    of active merchants. A run ends with each merchant's baseline plus whatever risk events
    have added and not yet decayed, so the mix-weighted baseline draw plus the measured
    excess has to sit inside those bands. The baseline alone is lighter at the top (about
    72 / 18 / 7 / 3%) because the events fill the upper tiers."""
    from simulator.config import load_calibration

    np.random.seed(5)
    mix = load_calibration()["archetype_mix"]
    names = list(mix)
    shares = np.array([mix[n] for n in names]) / sum(mix.values())
    baselines = np.array([sample_risk_baseline(str(np.random.choice(names, p=shares))) for _ in range(20000)])
    assert baselines.min() >= 0 and baselines.max() <= 100
    pct, value = zip(*EXCESS_PERCENTILES)
    excess = np.interp(np.random.random(len(baselines)) * 100, pct, value)
    scores = np.minimum(100, baselines + excess)
    buckets = [
        (scores <= 30).mean(),
        ((scores > 30) & (scores <= 55)).mean(),
        ((scores > 55) & (scores <= 75)).mean(),
        (scores > 75).mean(),
    ]
    assert 0.55 <= buckets[0] <= 0.70
    assert 0.15 <= buckets[1] <= 0.25
    assert 0.08 <= buckets[2] <= 0.15
    assert 0.03 <= buckets[3] <= 0.08


def test_riskier_archetypes_draw_higher_baselines():
    np.random.seed(6)
    wholesale = np.mean([sample_risk_baseline("wholesale") for _ in range(3000)])
    ecommerce = np.mean([sample_risk_baseline("e_commerce") for _ in range(3000)])
    assert ecommerce > wholesale + 25
