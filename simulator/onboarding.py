"""
onboarding.py: who joins the platform and when.

The main loop calls onboard_day() once per simulated day. A merchant is created with
everything that is fixed at onboarding: archetype (from the mix), province, metro flag,
quality percentile, id, and its activation (whether it ever transacts, and from which
day).

Counts are deterministic. The ramp in calibration.yaml spreads the merchant count over
the first six 30-day months; from month 7 each merchant that becomes Churned is replaced
by one new merchant the same day. The module keeps no state of its own, so a resumed run
only needs the run's start date and merchant count (carried in the checkpoint's extra).
Everything random comes from numpy's global RNG, in a fixed order, so a seeded or
checkpoint-restored run reproduces.
"""
from __future__ import annotations

from datetime import date, timedelta
from math import floor

import numpy as np

from simulator.archetypes import sample_metro, sample_province, sample_quality_percentile
from simulator.common import new_id
from simulator.config import load_calibration
from simulator.state import Merchant

# "Months" are 30 days throughout the state engine, so the ramp uses the same unit.
MONTH_DAYS = 30
RAMP_MONTHS = 6
RAMP_DAYS = RAMP_MONTHS * MONTH_DAYS


def _ramp_fractions() -> list[float]:
    ramp = load_calibration()["onboarding_ramp"]
    return [ramp[f"month_{k}_pct"] for k in range(1, RAMP_MONTHS + 1)]


def ramp_target(day_index: int, merchant_count: int) -> int:
    """Merchants onboarded by the end of day `day_index` (0 is the run's first day).

    Each month's share is spread evenly over its 30 days, and the running total is
    rounded, so the six months add up to exactly `merchant_count`.
    """
    if day_index < 0:
        return 0
    if day_index >= RAMP_DAYS:
        return merchant_count
    fractions = _ramp_fractions()
    month, day_in_month = divmod(day_index, MONTH_DAYS)
    cumulative = sum(fractions[:month]) + fractions[month] * (day_in_month + 1) / MONTH_DAYS
    return min(merchant_count, floor(merchant_count * cumulative + 0.5))


def ramp_count(day_index: int, merchant_count: int) -> int:
    """Merchants the ramp onboards on day `day_index`."""
    return ramp_target(day_index, merchant_count) - ramp_target(day_index - 1, merchant_count)


def sample_archetype() -> str:
    """An archetype drawn from the archetype_mix shares."""
    mix = load_calibration()["archetype_mix"]
    names = list(mix)
    shares = np.array([mix[name] for name in names], dtype=float)
    return str(np.random.choice(names, p=shares / shares.sum()))


def draw_activation() -> tuple[bool, int | None]:
    """(never_activates, delay_days). Delay is days after onboarding that the merchant
    may first transact; None for a merchant that never will.

    Of the merchants that activate, most start inside a week, with day weights from the
    config; the rest start uniformly between the late bounds.
    """
    lifecycle = load_calibration()["lifecycle"]
    if np.random.random() >= lifecycle["activation_share"]:
        return True, None
    delay = lifecycle["activation_delay"]
    if np.random.random() < delay["within_7_days_share"]:
        weights = np.array(delay["day_weights"], dtype=float)
        cumulative = np.cumsum(weights / weights.sum())
        index = int(np.searchsorted(cumulative, np.random.random(), side="right"))
        return False, min(index, len(weights) - 1) + 1
    low, high = delay["late_days"]
    return False, int(np.random.randint(low, high + 1))


def new_merchant(onboarded_at: date) -> Merchant:
    """Create one merchant. The draws run in a fixed order: archetype, province, metro
    flag, quality percentile, id, activation."""
    archetype = sample_archetype()
    province = sample_province(archetype)
    is_metro = sample_metro(province)
    quality = sample_quality_percentile()
    merchant_id = new_id()
    never_activates, delay_days = draw_activation()
    return Merchant(
        merchant_id=merchant_id,
        archetype=archetype,
        province=province,
        is_metro=is_metro,
        quality_percentile=quality,
        onboarded_at=onboarded_at,
        never_activates=never_activates,
        activation_date=None if delay_days is None else onboarded_at + timedelta(days=delay_days),
    )


def onboard_day(day: date, start_date: date, merchant_count: int, replacements: int = 0) -> list[Merchant]:
    """The merchants that join on `day`.

    `replacements` is the number of merchants that became Churned today. They are
    replaced one for one, but only once the ramp is over: during the ramp the count is
    fixed by the schedule.
    """
    day_index = (day - start_date).days
    count = ramp_count(day_index, merchant_count)
    if day_index >= RAMP_DAYS:
        count += replacements
    return [new_merchant(day) for _ in range(count)]
