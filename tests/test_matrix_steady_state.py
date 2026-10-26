"""tests/test_matrix_steady_state.py: portfolio quality implied by the DPD matrix.

Solves the Chain 4 matrix in causal_rules.yaml to steady state, with a loan that reaches
90+ DPD ageing 30 days a month and written off at 150 DPD (about two months in 90+). It is
a fast guard on the configuration; scripts/portfolio_check.py is the live check.

The Validation Targets write-off band was 3-7%. It cannot hold alongside the PAR90 band
under this matrix, so it was amended to 15-25% gross (see DECISION_LOG).
"""
import pytest

from simulator.config import load_causal_rules

MONTHS_IN_90_PLUS = 2  # entering at 91-120 DPD, written off once a further 60 days have passed


def steady_state() -> dict[str, float]:
    m = load_causal_rules()["dpd"]["transition_matrix"]
    current = 1.0
    b1 = m["current"]["dpd_1_30"] * current / (1 - m["dpd_1_30"]["dpd_1_30"])
    b2 = m["dpd_1_30"]["dpd_31_60"] * b1 / (1 - m["dpd_31_60"]["dpd_31_60"])
    b3 = m["dpd_31_60"]["dpd_61_90"] * b2 / (1 - m["dpd_61_90"]["dpd_61_90"])
    enter_90_plus = m["dpd_61_90"]["dpd_90_plus"] * b3  # per month
    b4 = enter_90_plus * MONTHS_IN_90_PLUS
    total = current + b1 + b2 + b3 + b4
    return {
        "par30": (b2 + b3 + b4) / total,
        "par60": (b3 + b4) / total,
        "par90": b4 / total,
        "annual_write_off": 12 * enter_90_plus / total,
    }


def test_matrix_gives_par_inside_the_validation_bands():
    s = steady_state()
    assert 0.05 <= s["par30"] <= 0.10
    assert 0.03 <= s["par60"] <= 0.07
    assert 0.02 <= s["par90"] <= 0.05


def test_matrix_write_off_flow_sits_in_the_amended_band():
    assert 0.15 <= steady_state()["annual_write_off"] <= 0.25


def test_the_original_write_off_band_contradicts_the_par90_band():
    # A 5% annual flow with the same 90+ dwell would give a PAR90 below its 2% floor.
    s = steady_state()
    par90_at_5_percent_flow = s["par90"] * 0.05 / s["annual_write_off"]
    assert par90_at_5_percent_flow < 0.02


def test_steady_state_values():
    s = steady_state()
    assert s["par30"] == pytest.approx(0.082, abs=0.003)
    assert s["annual_write_off"] == pytest.approx(0.196, abs=0.005)
