"""
payout.py: Settlement and payout scheduling logic.

Calculates settlement_date for each completed transaction (T+1 or T+2 for
card, same-day for instant EFT) and groups transactions into settlement
batches. Full merchant_payouts stream is V2 scope; in V1 settlement_date
and batch_id ride inline on merchant_transactions.

Phase 5 work: implement settlement date calculation using
calendar.get_sa_public_holidays() for business days.
"""
from __future__ import annotations

from datetime import date


def settlement_date(completed_at: date, channel: str) -> date:
    """
    Calculate the settlement date for a completed transaction.

    - instant_eft / payshap: same day
    - card_present / card_ecom / payment_link: T+1 business day (T+2 for ecom)

    Phase 5 implementation: currently returns a placeholder.
    """
    raise NotImplementedError("Phase 5")


def settlement_batch_id(merchant_id: str, settle_date: date) -> str:
    return f"batch-{settle_date.isoformat()}-{merchant_id[:8]}"
