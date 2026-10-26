"""
payout.py: Settlement date and batch id for completed transactions.

Card payments settle a fixed number of business days after completion; instant
rails settle the same day. Full merchant_payouts stream is V2 scope; in V1
settlement_date and batch_id ride inline on merchant_transactions.
"""
from __future__ import annotations

from datetime import date, timedelta

from simulator.calendar import get_sa_public_holidays
from simulator.config import load_fees


def _is_business_day(d: date) -> bool:
    return d.weekday() < 5 and d not in get_sa_public_holidays(d.year)


def next_business_day(d: date) -> date:
    """d itself if it is a business day, otherwise the next one."""
    while not _is_business_day(d):
        d += timedelta(days=1)
    return d


def settlement_date(completed_on: date, channel: str) -> date:
    """
    Settlement date for a transaction completed on completed_on.

    Channels with a lag of zero days in fees.yaml (instant EFT, PayShap) settle on the
    same calendar day, weekends included. Others advance by that many business days,
    skipping weekends and SA public holidays.
    """
    days = load_fees()["settlement_days"][channel]
    settled = completed_on
    while days > 0:
        settled += timedelta(days=1)
        if _is_business_day(settled):
            days -= 1
    return settled


def settlement_batch_id(merchant_id: str, settle_date: date) -> str:
    return f"batch-{settle_date.isoformat()}-{merchant_id[:8]}"
