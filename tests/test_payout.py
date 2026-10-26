"""tests/test_payout.py: Settlement dates and batch ids."""
from datetime import date

import pytest

from simulator.payout import settlement_batch_id, settlement_date


def test_card_present_is_next_business_day():
    # Wednesday 2025-06-11 -> Thursday.
    assert settlement_date(date(2025, 6, 11), "card_present") == date(2025, 6, 12)


def test_card_present_friday_settles_monday():
    assert settlement_date(date(2025, 6, 6), "card_present") == date(2025, 6, 9)


def test_card_ecom_is_two_business_days():
    # Thursday 2025-06-05 -> Monday 2025-06-09.
    assert settlement_date(date(2025, 6, 5), "card_ecom") == date(2025, 6, 9)


def test_payment_link_follows_ecom_lag():
    assert settlement_date(date(2025, 6, 5), "payment_link") == date(2025, 6, 9)


def test_public_holiday_is_skipped():
    # Youth Day is Monday 2025-06-16, so Friday's T+1 lands on Tuesday.
    assert settlement_date(date(2025, 6, 13), "card_present") == date(2025, 6, 17)


def test_instant_rails_settle_same_day_including_weekends():
    saturday = date(2025, 6, 7)
    assert saturday.weekday() == 5
    assert settlement_date(saturday, "instant_eft") == saturday
    assert settlement_date(saturday, "payshap") == saturday


def test_unknown_channel_raises():
    with pytest.raises(KeyError):
        settlement_date(date(2025, 6, 6), "cheque")


def test_batch_id_groups_by_merchant_and_date():
    assert settlement_batch_id("abcdef12-0000", date(2025, 6, 9)) == "batch-2025-06-09-abcdef12"
