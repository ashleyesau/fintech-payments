"""
archetypes.py — Merchant archetype definitions and base parameters.

Each Archetype dataclass holds the calibrated parameters for one merchant
category. These are the inputs to the event intensity function in Chain 1
of the causal dependency rules.

Phase 3 work: populate with the full 10-archetype table from the design docs,
including weekday weights, hourly weights, and seasonal modifiers.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class Archetype:
    name: str
    mean_ticket_zar: float        # mean transaction value in rands
    ticket_std_zar: float         # std dev of ticket size
    tx_per_day: float             # base transactions per open day
    open_days: list[str]          # weekdays open, e.g. ["Mon","Tue",...,"Sat"]
    refund_rate: float            # fraction of completed txns that generate a refund
    fraud_rate: float             # baseline fraud event probability per txn
    monthly_churn_prob: float     # probability merchant churns each month
    mcc: str                      # ISO 18245 merchant category code
    bank_weights: Dict[str, float] = field(default_factory=dict)  # bank → weight


# ---------------------------------------------------------------------------
# Archetype registry — Phase 3 will fill in all 10 entries
# ---------------------------------------------------------------------------

ARCHETYPES: Dict[str, Archetype] = {
    # Placeholder — replace with calibrated values in Phase 3
    "spaza_shop": Archetype(
        name="spaza_shop",
        mean_ticket_zar=50.0,
        ticket_std_zar=20.0,
        tx_per_day=80.0,
        open_days=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat"],
        refund_rate=0.005,
        fraud_rate=0.001,
        monthly_churn_prob=0.02,
        mcc="5411",
        bank_weights={"FNB": 0.40, "ABSA": 0.30, "Capitec": 0.20, "Nedbank": 0.10},
    ),
    # TODO Phase 3: restaurant, salon, grocery, liquor_store, fuel_station,
    #               e_commerce, wholesale, salon_low_cost, tutor, seasonal_retail
}


def get_archetype(name: str) -> Archetype:
    if name not in ARCHETYPES:
        raise ValueError(f"Unknown archetype: {name!r}. Available: {list(ARCHETYPES)}")
    return ARCHETYPES[name]
