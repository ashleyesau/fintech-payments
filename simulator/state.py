"""
state.py: Stateful domain objects: Merchant, Loan, DebitMandate.

These classes hold the in-memory state that evolves across simulated days.
State is never read from Postgres during a run; it lives here and is
checkpointed to disk at the end of each session.

Phase 4 work: implement the full Chain 2 state machine (GPV-based transitions),
quality_percentile assignment, and risk score decay logic.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date
from typing import Optional


# ---------------------------------------------------------------------------
# Merchant
# ---------------------------------------------------------------------------

MerchantState = str  # "new" | "growing" | "stable" | "stressed" | "delinquent" | "churned"


@dataclass
class Merchant:
    merchant_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    archetype: str = ""
    province: str = "gauteng"
    is_metro: bool = True

    # Lifecycle
    onboarded_at: Optional[date] = None
    first_transaction_at: Optional[date] = None
    merchant_state: MerchantState = "new"
    months_on_platform: int = 0

    # Quality tier (drawn from Beta(1.5, 4.0) at creation, fixed for lifetime)
    quality_percentile: float = 0.5

    # Risk
    risk_score: int = 10          # 0–100, higher = riskier
    loan_count: int = 0
    has_active_loan: bool = False

    # Internal counters (not persisted to Postgres)
    consecutive_good_days: int = 0
    days_without_txn: int = 0

    def effective_tx_rate(self) -> float:
        """Base tx rate adjusted for quality tier. Chain 1 multiplier."""
        from simulator.archetypes import get_archetype
        archetype = get_archetype(self.archetype)
        return archetype.tx_per_day * (0.4 + 1.6 * self.quality_percentile)


# ---------------------------------------------------------------------------
# Loan
# ---------------------------------------------------------------------------

@dataclass
class Loan:
    loan_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    merchant_id: str = ""
    application_id: str = ""

    principal_minor: int = 0       # disbursed amount in cents
    term_months: int = 6
    total_cost_pct: float = 0.07   # NCA fee as fraction

    disbursed_at: Optional[date] = None
    outstanding_principal_minor: int = 0
    days_past_due: int = 0
    is_written_off: bool = False

    mandate_id: Optional[str] = None
    mandate_status: str = "PDNG"   # ACCP | RJCT | PDNG | CANC


# ---------------------------------------------------------------------------
# Checkpoint helpers: Phase 7 will implement full serialisation
# ---------------------------------------------------------------------------

def save_checkpoint(merchants: list[Merchant], path: str = "checkpoint.json") -> None:
    """Persist merchant states to disk. Phase 7 implementation."""
    raise NotImplementedError("Phase 7")


def load_checkpoint(path: str = "checkpoint.json") -> list[Merchant]:
    """Load merchant states from disk. Phase 7 implementation."""
    raise NotImplementedError("Phase 7")
