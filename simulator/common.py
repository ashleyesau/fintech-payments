"""
common.py: helpers shared by the event generators.

Kept apart from generator.py so that risk.py, lending.py and repayments.py can use them
without importing the transaction generator.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone

import numpy as np

# South Africa has no daylight saving; archetype hourly weights are local time.
SAST = timezone(timedelta(hours=2))


def new_id() -> str:
    """UUID4 drawn from numpy's RNG so a seeded run reproduces its identifiers."""
    return str(uuid.UUID(bytes=np.random.bytes(16), version=4))


def end_of_day(day: date) -> datetime:
    """The moment the daily merchants snapshot is taken: 23:59 SAST."""
    return datetime(day.year, day.month, day.day, 23, 59, tzinfo=SAST)
