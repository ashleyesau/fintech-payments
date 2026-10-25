"""
risk.py — Fraud and risk rule evaluation.

Evaluates Chain 5 of the causal dependency rules: detects transaction
anomalies (refund spikes, velocity breaches, chargeback accumulation),
emits risk_event records, and updates the merchant's running risk score.

Phase 5 work: implement the full rule set with the thresholds from the
Causal Dependency Rules doc.
"""
from __future__ import annotations

from typing import Any


def evaluate_risk(merchant: Any, recent_events: list[dict]) -> list[dict]:
    """
    Check recent events against the fraud rule set.

    Returns a list of risk_event dicts (may be empty) and mutates
    merchant.risk_score via the delta table from Chain 5.

    Phase 5 implementation.
    """
    raise NotImplementedError("Phase 5")
