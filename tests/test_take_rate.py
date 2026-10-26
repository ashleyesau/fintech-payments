"""tests/test_take_rate.py: platform take rates from the fee schedule.

Weights each archetype by expected daily GPV (tx_per_day x mean ticket). Validation
Targets originally put gross take at 2.5-3.2%; the Simulator Design rate card cannot
reach that (instant EFT is capped at R3 and PayShap is R1 flat), so the band was
amended to 1.9-2.4%. Net take is unchanged at 0.8-1.4%.
"""
from datetime import date

import numpy as np
import pytest

from simulator.archetypes import ARCHETYPES
from simulator.generator import simulate_transaction
from simulator.state import Merchant

pytestmark = pytest.mark.slow

SAMPLES_PER_ARCHETYPE = 2000


def gpv_weighted_take_rates() -> tuple[float, float]:
    np.random.seed(2)
    weighted_gross = weighted_net = total_weight = 0.0
    for name, archetype in ARCHETYPES.items():
        merchant = Merchant(archetype=name, quality_percentile=0.27, onboarded_at=date(2025, 1, 1))
        volume = gross = net = 0
        for _ in range(SAMPLES_PER_ARCHETYPE):
            txn = simulate_transaction(merchant, date(2025, 5, 14))
            if txn["status"] != "completed":
                continue
            volume += txn["amount_minor"]
            net += txn["acquirer_markup_minor"]
            gross += txn["interchange_fee_minor"] + txn["scheme_fee_minor"] + txn["acquirer_markup_minor"]
        weight = archetype.tx_per_day * archetype.mean_ticket_zar
        weighted_gross += weight * gross / volume
        weighted_net += weight * net / volume
        total_weight += weight
    return weighted_gross / total_weight, weighted_net / total_weight


def test_blended_take_rates_sit_inside_the_validation_bands():
    gross, net = gpv_weighted_take_rates()
    assert 0.019 <= gross <= 0.024
    assert 0.008 <= net <= 0.014
