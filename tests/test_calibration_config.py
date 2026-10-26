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


# ---------------------------------------------------------------------------
# Instant EFT funnel: Simulator Design's values, and the amended Validation bands
# ---------------------------------------------------------------------------

def test_eft_funnel_is_simulator_designs_and_sits_in_the_amended_bands():
    """Simulator Design gives the Ozow funnel as 95, 90, 85, 92 and 99% and the OTP-stage
    split as 70/20/10. They multiply to 66.2% completion and put 37.9% of failures at the
    OTP step. Validation Targets said 70 to 83% and 40 to 60% (Ozow One-Blink benchmarks),
    which the sourced funnel cannot reach, so the bands were amended to 62 to 70% and 34
    to 42% (Phase 8), as the take-rate and write-off bands were. If a funnel value is
    retuned, this fails and the band note on the Validation Targets page needs a look."""
    from simulator.config import load_payments

    payments = load_payments()
    funnel = payments["eft_funnel"]
    assert funnel == {"bank_selected": 0.95, "login_succeeded": 0.90, "otp_entered": 0.85,
                      "authorized": 0.92, "completed": 0.99}
    assert payments["eft_otp_failure_split"] == {"abandoned": 0.70, "expired": 0.20, "failed": 0.10}

    reach_login = funnel["bank_selected"] * funnel["login_succeeded"]
    reach_otp_entered = reach_login * funnel["otp_entered"]
    reach_authorized = reach_otp_entered * funnel["authorized"]
    completion = reach_authorized * funnel["completed"]
    failures = {
        "bank": 1 - funnel["bank_selected"],
        "login": funnel["bank_selected"] * (1 - funnel["login_succeeded"]),
        "otp": reach_login * (1 - funnel["otp_entered"]),
        "authorize": reach_otp_entered * (1 - funnel["authorized"]),
        "complete": reach_authorized * (1 - funnel["completed"]),
    }
    assert 0.62 <= completion <= 0.70
    assert 0.34 <= failures["otp"] / sum(failures.values()) <= 0.42
    assert completion + sum(failures.values()) == pytest.approx(1.0)


def test_fraud_event_rate_follows_the_archetype_rates_and_the_amended_band():
    """The platform fraud event rate (chargebacks over completed payments) is the archetype
    fraud rates weighted by the payments each archetype produces, and only card channels
    can charge back. That is 0.15%, under Validation Targets' original 0.2% floor even though
    the same rates weighted by merchant count give 0.30%: the high-volume archetypes (Spaza,
    Grocery) carry the lowest rates and a fifth of payments are not card. The floor was
    amended to 0.1% (Phase 8). Raising E-commerce from 10 to 12% of the mix moved this by
    about 0.001 points; reaching 0.2% would need E-commerce at about 13% of payments."""
    from simulator.archetypes import ARCHETYPES
    from simulator.config import load_payments

    mix = load_calibration()["archetype_mix"]
    channels = load_payments()["channel_mix"]
    weights, rate = 0.0, 0.0
    for name, share in mix.items():
        a = ARCHETYPES[name]
        payments = share * a.tx_per_day * (len(a.open_days) / 7) * sum(a.monthly_seasonal_multipliers) / 12
        card = sum(v for k, v in channels[name].items() if k in ("card_present", "card_ecom", "payment_link"))
        weights += payments
        rate += payments * a.fraud_rate * card
    by_payments = rate / weights
    by_merchants = sum(share * ARCHETYPES[n].fraud_rate for n, share in mix.items())
    assert 0.001 <= by_payments <= 0.015
    assert by_payments < 0.002 < by_merchants   # why the original floor could not hold


# ---------------------------------------------------------------------------
# Bands amended in Phase 8 because the sourced parameters cannot reach them
# ---------------------------------------------------------------------------

def _weights():
    """Expected GPV per onboarded merchant by archetype: share x tx per day x open-day
    fraction x mean ticket (the same weighting as the take-rate guard)."""
    from simulator.archetypes import ARCHETYPES

    mix = load_calibration()["archetype_mix"]
    return {
        n: share * ARCHETYPES[n].tx_per_day * (len(ARCHETYPES[n].open_days) / 7) * ARCHETYPES[n].mean_ticket_zar
        for n, share in mix.items()
    }


def test_january_against_december_follows_the_modifiers_and_the_amended_band():
    """December runs 25 to 35% above the annual average and January's count and ticket
    modifiers multiply to 0.91, so January is at most 0.91 / 1.25 = 0.73 of December
    before any seasonal table, and 0.91 / 1.35 = 0.67 at the other end. Weighted by the
    archetype seasonal tables (Jan over Dec 0.72) it is about 0.65. Validation Targets'
    original 10 to 20% below (0.80 to 0.90) cannot hold with the other two bands; the band
    was amended to 27 to 40% below (Phase 8)."""
    from simulator.archetypes import ARCHETYPES

    cal = load_calibration()
    modifiers = cal["seasonal"]["january_tx_count_modifier"] * cal["seasonal"]["january_ticket_size_modifier"]
    assert 0.67 <= modifiers / 1.35 < modifiers / 1.25 <= 0.73
    assert modifiers / 1.25 < 0.80            # why the original band could not hold

    weights = _weights()
    jan = sum(w * ARCHETYPES[n].monthly_seasonal_multipliers[0] for n, w in weights.items())
    dec = sum(w * ARCHETYPES[n].monthly_seasonal_multipliers[11] for n, w in weights.items())
    expected = modifiers * jan / dec - 1
    assert -0.40 <= expected <= -0.27


def test_black_friday_follows_the_ticket_lifts_and_the_amended_band():
    """Causal Rules lifts tickets after the cap on three archetypes only (E-commerce 2.5,
    Spaza 1.3, Restaurant 1.1). Weighted by GPV that is 1.30 on the day, and November
    runs about 1.15 above an average month, so about 1.49 against an average Friday.
    The original 2.0 to 2.5x needs lifts Causal Rules does not give the other eight
    archetypes; the band was amended to 1.3 to 1.9x (Phase 8): 1.49 expected, and a single
    day's ratio ranged 1.45 to 1.81 across seeds."""
    from simulator.archetypes import ARCHETYPES

    cal = load_calibration()
    lifts = cal["seasonal"]["black_friday_ticket_multipliers"]
    default = cal["seasonal"]["black_friday_ticket_default"]
    weights = _weights()
    total = sum(weights.values())
    day_lift = sum(w * lifts.get(n, default) for n, w in weights.items()) / total
    nov = sum(w * ARCHETYPES[n].monthly_seasonal_multipliers[10] for n, w in weights.items()) / total
    year = sum(w * sum(ARCHETYPES[n].monthly_seasonal_multipliers) / 12 for n, w in weights.items()) / total
    expected = day_lift * nov / year
    assert 1.4 <= expected <= 1.7
    assert day_lift < 1.4 and expected < 2.0   # why the original band could not hold


def test_first_payment_default_follows_the_matrix_and_the_amended_band():
    """For a neutral merchant an instalment is still unpaid a month after its due date
    with the probability of the matrix's Current to 1-30 step (6%), by construction of the
    late-cure probability. Any state mix adds to that (Stressed and Delinquent merchants cure
    and recover less), so full runs read 7 to 10%. Validation Targets' 3 to 6% sits below the
    matrix it was meant to agree with; the band was amended to 5 to 10% (Phase 8)."""
    from simulator.repayments import first_strike_probability, late_cure_probability, retry_probability
    from simulator.state import Merchant

    m = Merchant(merchant_state="stable")
    from simulator.config import load_causal_rules

    retries = load_causal_rules()["debicheck"]["max_retries"]
    unpaid_after_retries = (1 - first_strike_probability(m)) * (1 - retry_probability(m)) ** retries
    neutral = unpaid_after_retries * (1 - late_cure_probability(m))
    assert neutral == pytest.approx(0.06, abs=0.003)
    assert 0.05 <= neutral <= 0.10


def test_approved_share_in_the_low_risk_bucket_follows_manual_review_and_the_amended_band():
    """Applications from the 0-30 tier are approved automatically; the 31-55 tier goes to
    manual review, which approves 60% (lending.yaml), and those count as approved. At the
    middle of Validation Targets' risk buckets (62.5% and 20%) the approved share from 0-30
    is 62.5 / (62.5 + 20 x 0.6) = 84%. The original "90% or more" cannot hold with a 60%
    manual approval rate; the band was amended to 75% or more (Phase 8)."""
    import yaml
    from pathlib import Path

    rate = yaml.safe_load((Path(__file__).parent.parent / "config" / "lending.yaml").read_text())["manual_review_approve_rate"]
    share = 0.625 / (0.625 + 0.20 * rate)
    assert 0.75 <= share < 0.90


def test_automation_and_net_approval_follow_the_risk_tiers_and_the_amended_bands():
    """Manual review is the 31-55 risk tier, which Validation Targets put at 15 to 25% of
    merchants, so automation can be no higher than 75 to 85% (100% less that tier), not the
    65 to 80% it was given. Approved decisions over all tiers at the middle of the buckets is
    (62.5 + 20 x 0.6) / 100 = 74.5%, and applicants who are eligible are Growing or Stable
    merchants, who sit lower in risk than the platform, so the net approval band's top edge
    was lifted from 75 to 80% (Phase 8)."""
    import yaml
    from pathlib import Path

    rate = yaml.safe_load((Path(__file__).parent.parent / "config" / "lending.yaml").read_text())["manual_review_approve_rate"]
    manual_share_low, manual_share_high = 0.15, 0.25
    assert 1 - manual_share_high == pytest.approx(0.75)
    assert 1 - manual_share_low == pytest.approx(0.85)
    net_at_mid_buckets = 0.625 + 0.20 * rate
    assert 0.55 <= net_at_mid_buckets <= 0.80
    assert net_at_mid_buckets < 0.75 < 0.80
