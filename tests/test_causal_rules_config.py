"""Checks that config/causal_rules.yaml is internally consistent and matches
the values in Causal Dependency Rules that Phase 4 and 5 depend on."""
import pytest

from simulator.config import load_causal_rules

STATES = ["new", "growing", "stable", "stressed", "delinquent", "churned"]


@pytest.fixture(scope="module")
def rules() -> dict:
    return load_causal_rules()


def test_stress_modifier_covers_every_state(rules):
    assert set(rules["state_transitions"]["stress_modifier"]) == set(STATES)


def test_transition_probabilities_are_valid(rules):
    for name, p in rules["state_transitions"]["probabilities"].items():
        assert 0.0 < p <= 1.0, name


def test_transition_priority_order(rules):
    assert rules["state_transitions"]["priority"] == [
        "churned", "delinquent", "stressed", "growing", "stable",
    ]


def test_debicheck_state_modifiers(rules):
    mods = rules["debicheck"]["state_modifiers"]
    assert mods == {"growing": 1.10, "stable": 1.00, "stressed": 0.70, "delinquent": 0.40}


def test_dpd_rows_sum_to_one(rules):
    for bucket, row in rules["dpd"]["transition_matrix"].items():
        assert sum(row.values()) == pytest.approx(1.0), bucket


def test_loan_approval_bands_are_contiguous(rules):
    a = rules["loan_approval"]
    assert a["manual_review_min"] == a["auto_approve_max"] + 1
    assert a["counter_offer_min"] == a["manual_review_max"] + 1
    assert a["auto_decline_min"] == a["counter_offer_max"] + 1


def test_risk_decay_is_per_period_not_daily(rules):
    decay = rules["risk_score"]["decay"]
    assert decay["period_days"] == 30
    assert decay["points_per_period"] == 1
    assert decay["sustained_good_days"] == 90
    assert decay["sustained_points_per_period"] == 3


def test_fraud_trigger_deltas(rules):
    deltas = {k: v["risk_score_delta"] for k, v in rules["fraud_triggers"].items()}
    assert deltas == {
        "refund_spike": 10,
        "velocity_breach": 12,
        "chargeback_accumulation": 20,
        "scheme_warning": 25,
        "large_txn_flag": 5,
        "otp_abuse": 8,
    }
