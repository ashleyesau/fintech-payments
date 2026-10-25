"""
tests/test_calendar.py: Tests for calendar utilities and archetype loading.
"""
from datetime import date

import pytest

from simulator.archetypes import (
    ARCHETYPES,
    get_archetype,
    sample_metro,
    sample_province,
    sample_quality_percentile,
)
from simulator.calendar import get_black_friday, get_intensity_multiplier, get_sa_public_holidays


# ---------------------------------------------------------------------------
# Public holidays
# ---------------------------------------------------------------------------

def test_good_friday_2026():
    # Easter 2026 is April 5; Good Friday is April 3.
    holidays = get_sa_public_holidays(2026)
    assert date(2026, 4, 3) in holidays
    assert "Good Friday" in holidays[date(2026, 4, 3)]


def test_family_day_2026():
    # Easter Monday 2026 is April 6.
    holidays = get_sa_public_holidays(2026)
    assert date(2026, 4, 6) in holidays
    assert "Family Day" in holidays[date(2026, 4, 6)]


def test_sunday_rule_2026():
    # National Women's Day is Aug 9. In 2026 it falls on a Sunday.
    # The following Monday (Aug 10) must also be a public holiday.
    holidays = get_sa_public_holidays(2026)
    assert date(2026, 8, 9) in holidays
    assert date(2026, 8, 10) in holidays
    assert "observed" in holidays[date(2026, 8, 10)].lower()


def test_fixed_holidays_present_2025():
    holidays = get_sa_public_holidays(2025)
    assert date(2025, 1, 1) in holidays   # New Year's Day
    assert date(2025, 4, 27) in holidays  # Freedom Day
    assert date(2025, 12, 25) in holidays  # Christmas Day
    assert date(2025, 12, 26) in holidays  # Day of Goodwill


def test_holiday_count_reasonable():
    # SA has 12 base holidays + possible Sunday Rule additions + Easter-derived.
    holidays = get_sa_public_holidays(2025)
    assert 12 <= len(holidays) <= 16


# ---------------------------------------------------------------------------
# Black Friday
# ---------------------------------------------------------------------------

def test_black_friday_2025():
    # Last Friday of November 2025 is November 28.
    assert get_black_friday(2025) == date(2025, 11, 28)


def test_black_friday_2026():
    # Last Friday of November 2026 is November 27.
    assert get_black_friday(2026) == date(2026, 11, 27)


def test_black_friday_is_friday():
    for year in range(2024, 2030):
        bf = get_black_friday(year)
        assert bf.weekday() == 4, f"Black Friday {bf} is not a Friday"
        assert bf.month == 11


# ---------------------------------------------------------------------------
# Intensity multiplier
# ---------------------------------------------------------------------------

def test_intensity_multiplier_closed_day():
    # Spaza shop is closed on Sunday.
    sunday = date(2025, 6, 1)  # A Sunday
    assert sunday.weekday() == 6
    result = get_intensity_multiplier(sunday, "spaza_shop")
    assert result == 0.0


def test_intensity_multiplier_wholesale_on_holiday():
    # Wholesale is B2B and should return 0.0 on public holidays.
    freedom_day = date(2025, 4, 27)
    result = get_intensity_multiplier(freedom_day, "wholesale")
    assert result == 0.0


def test_intensity_multiplier_black_friday_capped():
    # Black Friday multiplier stacks but must not exceed saturation cap (2.5).
    bf_2025 = get_black_friday(2025)
    result = get_intensity_multiplier(bf_2025, "spaza_shop")
    assert result <= 2.5
    assert result > 1.0


def test_intensity_multiplier_ecom_black_friday():
    # E-commerce uses 3.0x BF multiplier, but result is still capped at 2.5.
    bf_2025 = get_black_friday(2025)
    result = get_intensity_multiplier(bf_2025, "e_commerce")
    assert result <= 2.5


def test_intensity_multiplier_positive_on_open_day():
    # A normal Tuesday for spaza_shop should return a positive multiplier.
    tuesday = date(2025, 6, 3)  # A Tuesday
    assert tuesday.weekday() == 1
    result = get_intensity_multiplier(tuesday, "spaza_shop")
    assert result > 0.0


def test_intensity_multiplier_wholesale_closed_weekend():
    saturday = date(2025, 6, 7)  # A Saturday
    assert saturday.weekday() == 5
    result = get_intensity_multiplier(saturday, "wholesale")
    assert result == 0.0


# ---------------------------------------------------------------------------
# Archetypes
# ---------------------------------------------------------------------------

def test_all_archetypes_loaded():
    expected = {
        "spaza_shop", "restaurant", "salon", "grocery", "liquor_store",
        "fuel_station", "e_commerce", "wholesale", "salon_low_cost",
        "tutor", "seasonal_retail",
    }
    assert set(ARCHETYPES.keys()) == expected


def test_archetype_open_days_consistent():
    for name, archetype in ARCHETYPES.items():
        for day in archetype.open_days:
            assert archetype.weekday_weights[day] > 0, (
                f"{name}: {day} in open_days but weekday_weight is 0"
            )


def test_archetype_hourly_weights_length():
    for name, archetype in ARCHETYPES.items():
        assert len(archetype.hourly_weights) == 24, (
            f"{name}: hourly_weights must have 24 elements"
        )


def test_archetype_monthly_multipliers_length():
    for name, archetype in ARCHETYPES.items():
        assert len(archetype.monthly_seasonal_multipliers) == 12, (
            f"{name}: monthly_seasonal_multipliers must have 12 elements"
        )


def test_archetype_weekday_weights_normalized():
    for name, archetype in ARCHETYPES.items():
        open_weights = [w for w in archetype.weekday_weights.values() if w > 0]
        if open_weights:
            mean = sum(open_weights) / len(open_weights)
            assert abs(mean - 1.0) < 0.01, (
                f"{name}: weekday_weights mean={mean:.3f}, expected ~1.0"
            )


def test_get_archetype_unknown_raises():
    with pytest.raises(ValueError, match="Unknown archetype"):
        get_archetype("nonexistent")


# ---------------------------------------------------------------------------
# Province and quality sampling
# ---------------------------------------------------------------------------

def test_sample_province_valid():
    valid_provinces = {
        "gauteng", "western_cape", "kwazulu_natal", "eastern_cape",
        "limpopo", "mpumalanga", "free_state", "north_west", "northern_cape",
    }
    for _ in range(20):
        province = sample_province("spaza_shop")
        assert province in valid_provinces


def test_sample_metro_returns_bool():
    result = sample_metro("gauteng")
    assert isinstance(result, bool)


def test_sample_quality_percentile_range():
    for _ in range(50):
        q = sample_quality_percentile()
        assert 0.0 <= q <= 1.0
