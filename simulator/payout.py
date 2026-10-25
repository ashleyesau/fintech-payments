"""
payout.py: Settlement and payout scheduling logic.

Calculates settlement_date for each completed transaction (T+1 or T+2 for
card, same-day for instant EFT) and groups transactions into settlement
batches. Full merchant_payouts stream is V2 scope; in V1 settlement_date
and batch_id ride inline on merchant_transactions.

Phase 5 work: implement settlement date calculation respecting the SA
public holiday calendar.
"""
from __future__ import annotations

from datetime import date


# SA public holidays (static list, extend as needed)
SA_PUBLIC_HOLIDAYS_2025_2026: set[date] = {
    date(2025, 1, 1),   # New Year's Day
    date(2025, 3, 21),  # Human Rights Day
    date(2025, 4, 18),  # Good Friday
    date(2025, 4, 21),  # Family Day
    date(2025, 4, 28),  # Freedom Day observed
    date(2025, 5, 1),   # Workers' Day
    date(2025, 6, 16),  # Youth Day
    date(2025, 8, 9),   # National Women's Day
    date(2025, 9, 24),  # Heritage Day
    date(2025, 12, 16), # Day of Reconciliation
    date(2025, 12, 25), # Christmas Day
    date(2025, 12, 26), # Day of Goodwill
    date(2026, 1, 1),   # New Year's Day
}


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
