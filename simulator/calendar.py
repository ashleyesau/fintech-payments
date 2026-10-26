"""
simulator/calendar.py: SA public holiday calendar and intensity multiplier.

Exports:
  get_sa_public_holidays(year) -> dict[date, str]
  get_black_friday(year) -> date
  get_intensity_multiplier(sim_date, archetype_name, stress_modifier=1.0,
                           province=None, is_metro=True) -> float
  get_tx_count_modifier(sim_date) -> float
  get_ticket_size_modifier(sim_date, archetype_name) -> float
"""
from __future__ import annotations

from datetime import date, timedelta
from functools import lru_cache
from typing import Optional

from dateutil.easter import easter, EASTER_WESTERN

from simulator.config import load_calibration

# Fixed-date SA public holidays per the Public Holidays Act (Act 36 of 1994).
_FIXED_HOLIDAYS: list[tuple[int, int, str]] = [
    (1, 1, "New Year's Day"),
    (3, 21, "Human Rights Day"),
    (4, 27, "Freedom Day"),
    (5, 1, "Workers' Day"),
    (6, 16, "Youth Day"),
    (8, 9, "National Women's Day"),
    (9, 24, "Heritage Day"),
    (12, 16, "Day of Reconciliation"),
    (12, 25, "Christmas Day"),
    (12, 26, "Day of Goodwill"),
]


@lru_cache(maxsize=10)
def get_sa_public_holidays(year: int) -> dict[date, str]:
    """
    Return all SA public holidays for the given year, keyed by date.

    Includes 10 fixed-date holidays, Easter-derived Good Friday and Family Day
    (Easter Monday), and Sunday Rule additions (if a holiday falls on a Sunday,
    the following Monday is also a public holiday).
    """
    holidays: dict[date, str] = {}

    for month, day, name in _FIXED_HOLIDAYS:
        holidays[date(year, month, day)] = name

    easter_sunday = easter(year, method=EASTER_WESTERN)
    holidays[easter_sunday - timedelta(days=2)] = "Good Friday"
    holidays[easter_sunday + timedelta(days=1)] = "Family Day"

    # Sunday Rule: holiday on Sunday -> following Monday also a public holiday.
    additions: dict[date, str] = {}
    for h_date, h_name in holidays.items():
        if h_date.weekday() == 6:  # Sunday
            observed = h_date + timedelta(days=1)
            if observed not in holidays:
                additions[observed] = f"{h_name} (observed)"
    holidays.update(additions)

    return holidays


@lru_cache(maxsize=10)
def get_black_friday(year: int) -> date:
    """Last Friday of November for the given year."""
    d = date(year, 11, 30)
    while d.weekday() != 4:  # 4 = Friday
        d -= timedelta(days=1)
    return d


def _geographic_multiplier(
    sim_date: date, archetype_name: str, province: Optional[str]
) -> float:
    """Western Cape tourism lift and dip, and KZN holiday peaks (Calibration Doc Area 4)."""
    geo = load_calibration().get("geographic", {})
    month = sim_date.month
    if province == "western_cape" and archetype_name in geo.get("western_cape_summer_archetypes", []):
        if month in (12, 1, 2):
            return geo.get("western_cape_summer_lift", 1.0)
        if month in (6, 7, 8):
            return geo.get("western_cape_winter_dip", 1.0)
    if province == "kwazulu_natal" and month in geo.get("kzn_holiday_peak_months", []):
        return geo.get("kzn_holiday_lift", 1.0)
    return 1.0


def _load_shedding_multiplier(archetype_name: str, is_metro: bool) -> float:
    """Outage damper. 1.0 unless load_shedding.active is true, which V1 runs from
    March 2024 onwards leave off. Township spaza shops (non-metro) also close for part
    of each outage."""
    calibration = load_calibration()
    shedding = calibration.get("load_shedding", {})
    if not shedding.get("active", False):
        return 1.0
    outage_share = shedding.get("hours_off_per_day", 0) / 24.0
    stage_multiplier = shedding.get("stage_multipliers", {}).get(shedding.get("stage", 0), 1.0)
    damper = 1.0 - (1.0 - stage_multiplier) * outage_share
    if archetype_name == "spaza_shop" and not is_metro:
        closure = calibration.get("geographic", {}).get("township_sme_closure_rate", 0.0)
        damper *= 1.0 - closure * outage_share
    return damper


def get_intensity_multiplier(
    sim_date: date,
    archetype_name: str,
    stress_modifier: float = 1.0,
    province: Optional[str] = None,
    is_metro: bool = True,
) -> float:
    """
    Composite intensity multiplier for one archetype on one simulated date.

    Stacks the following components, then caps at calibration.saturation_cap:
      - weekday weight (0.0 if closed; normalized so mean across open days = 1.0)
      - day-of-month multiplier (DAILY_VOLUME_INDEX from calibration.yaml)
      - monthly seasonal multiplier (archetype-specific, Jan-Dec)
      - public holiday multiplier (0.85 on holidays; B2B archetypes close at 0.0)
      - province effects when province is given (Western Cape tourism lift and dip, KZN
        holiday peaks)
      - the load shedding damper (1.0 while load_shedding.active is false)
      - the merchant's Chain 2 stress modifier

    The stress modifier sits inside the cap: Causal Rules caps the product of all
    temporal modifiers and the stress modifier together, so a Growing merchant (1.05)
    cannot exceed the saturation cap and a Churned one (0.0) always returns 0.0.

    January count and ticket modifiers, the December peak days and Black Friday are not
    part of this value. Causal Rules applies them outside the cap and, apart from the
    January count modifier, to ticket size only; see get_tx_count_modifier() and
    get_ticket_size_modifier().

    Returns 0.0 if the archetype is closed on this day.
    """
    # Import inside function to avoid circular import at module level.
    from simulator.archetypes import get_archetype

    archetype = get_archetype(archetype_name)
    calibration = load_calibration()
    saturation_cap: float = calibration.get("seasonal", {}).get("saturation_cap", 2.50)

    # Weekday weight. 0.0 means closed.
    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    day_name = day_names[sim_date.weekday()]
    weekday_weight = archetype.weekday_weights.get(day_name, 0.0)
    if weekday_weight == 0.0:
        return 0.0

    # Day-of-month multiplier.
    dom_index: dict = calibration.get("daily_volume_index", {})
    dom_mult: float = dom_index.get(sim_date.day, 1.0)

    # Monthly seasonal multiplier (archetype-specific, 1-indexed).
    monthly_mults = archetype.monthly_seasonal_multipliers
    month_mult: float = monthly_mults[sim_date.month - 1] if monthly_mults else 1.0

    # Public holiday multiplier.
    holidays = get_sa_public_holidays(sim_date.year)
    if sim_date in holidays:
        # B2B archetypes are effectively closed on public holidays.
        if archetype_name == "wholesale":
            return 0.0
        holiday_mult = 0.85
    else:
        holiday_mult = 1.0

    composite = (
        weekday_weight * dom_mult * month_mult * holiday_mult
        * _geographic_multiplier(sim_date, archetype_name, province)
        * _load_shedding_multiplier(archetype_name, is_metro)
        * stress_modifier
    )
    return min(composite, saturation_cap)


def get_tx_count_modifier(sim_date: date) -> float:
    """January transaction-count modifier (more transactions). 1.0 in other months.

    Applied to the Poisson lambda, independently of get_ticket_size_modifier(), so
    January shows higher counts and lower GPV together.
    """
    seasonal = load_calibration().get("seasonal", {})
    if sim_date.month == 1:
        return seasonal.get("january_tx_count_modifier", 1.0)
    return 1.0


def get_ticket_size_modifier(sim_date: date, archetype_name: str) -> float:
    """Ticket-size modifier: smaller baskets in January, larger on the December peak
    days and on Black Friday (archetype-specific). 1.0 otherwise. Applied to the ticket
    draw, never to the transaction count."""
    seasonal = load_calibration().get("seasonal", {})
    if sim_date == get_black_friday(sim_date.year):
        by_archetype = seasonal.get("black_friday_ticket_multipliers", {})
        return by_archetype.get(archetype_name, seasonal.get("black_friday_ticket_default", 1.0))
    if sim_date.month == 1:
        return seasonal.get("january_ticket_size_modifier", 1.0)
    if sim_date.month == 12 and sim_date.day in seasonal.get("december_peak_days", []):
        return seasonal.get("december_peak_ticket_multiplier", 1.0)
    return 1.0
