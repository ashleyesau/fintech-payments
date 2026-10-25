"""
simulator/calendar.py: SA public holiday calendar and intensity multiplier.

Exports:
  get_sa_public_holidays(year) -> dict[date, str]
  get_black_friday(year) -> date
  get_intensity_multiplier(sim_date, archetype_name) -> float
"""
from __future__ import annotations

from datetime import date, timedelta
from functools import lru_cache

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


def get_intensity_multiplier(sim_date: date, archetype_name: str) -> float:
    """
    Composite intensity multiplier for one archetype on one simulated date.

    Stacks the following components, then caps at calibration.saturation_cap:
      - weekday weight (0.0 if closed; normalized so mean across open days = 1.0)
      - day-of-month multiplier (DAILY_VOLUME_INDEX from calibration.yaml)
      - monthly seasonal multiplier (archetype-specific, Jan-Dec)
      - public holiday multiplier (0.85 on holidays; B2B archetypes close at 0.0)
      - Black Friday multiplier (2.5x for retail; 3.0x for e_commerce)

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

    # Black Friday multiplier.
    black_friday = get_black_friday(sim_date.year)
    if sim_date == black_friday:
        bf_section = calibration.get("seasonal", {})
        if archetype_name == "e_commerce":
            bf_mult = bf_section.get("black_friday_ecom_multiplier", 3.00)
        else:
            bf_mult = bf_section.get("black_friday_day_multiplier", 2.50)
    else:
        bf_mult = 1.0

    composite = weekday_weight * dom_mult * month_mult * holiday_mult * bf_mult
    return min(composite, saturation_cap)
