"""
generator.py: Core event generation functions.

simulate_day() is the main entry point: given a merchant and a calendar date,
it returns the list of events that merchant would generate on that day.

Phase 5 work: implement the full event generation logic: channel-dependent
state machines, fee calculation from the SARB interchange schedule, loan
application propensity, DebiCheck repayment attempts.
"""
from __future__ import annotations

from datetime import date
from typing import Any


def simulate_day(merchant: Any, sim_date: date) -> list[dict]:
    """
    Generate all events for one merchant on one simulated day.

    Returns a list of event dicts ready for Postgres insert. Each dict
    maps directly to a table row per the Event Schemas contract.

    Phase 5 implementation.
    """
    raise NotImplementedError("Phase 5")


def simulate_transaction(merchant: Any, sim_date: date) -> dict:
    """
    Generate a single payment event for a merchant.

    Selects channel (card_present / card_ecom / instant_eft / payshap),
    runs the appropriate state machine, calculates fees from the SARB
    interchange schedule, and returns a merchant_transactions row.

    Phase 5 implementation.
    """
    raise NotImplementedError("Phase 5")


def simulate_loan_application(merchant: Any, sim_date: date) -> dict | None:
    """
    Probabilistically generate a loan application event.

    Returns a loan_applications row dict, or None if the merchant does not
    apply this day. Eligibility gates from Chain 3 of the causal rules.

    Phase 5 implementation.
    """
    raise NotImplementedError("Phase 5")
