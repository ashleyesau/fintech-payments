"""tests/test_generator.py: Phase 5.1 transaction generation."""
from collections import Counter
from datetime import date, timedelta

import numpy as np
import pytest

from simulator import generator
from simulator.archetypes import ARCHETYPES
from simulator.generator import (
    _fees,
    _schedule_followups,
    _ticket_minor,
    simulate_day,
    simulate_transaction,
)
from simulator.models import MerchantTransaction
from simulator.state import Merchant

WEDNESDAY = date(2025, 5, 14)
SUNDAY = date(2025, 6, 1)


def merchant(archetype: str = "spaza_shop", **kwargs) -> Merchant:
    kwargs.setdefault("quality_percentile", 0.3)
    kwargs.setdefault("onboarded_at", date(2025, 1, 1))
    return Merchant(archetype=archetype, **kwargs)


def sample(archetype: str, n: int, seed: int = 11, day: date = WEDNESDAY) -> list[dict]:
    np.random.seed(seed)
    m = merchant(archetype)
    return [simulate_transaction(m, day) for _ in range(n)]


# ---------------------------------------------------------------------------
# Schema and state machine
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("archetype", sorted(ARCHETYPES))
def test_every_archetype_produces_valid_rows(archetype):
    for txn in sample(archetype, 300):
        MerchantTransaction(**txn)


def test_card_funnel_timestamps_increase():
    for txn in sample("restaurant", 400):
        if txn["status"] == "completed":
            assert txn["created_at"] < txn["authorized_at"] < txn["completed_at"]


def test_eft_funnel_timestamps_increase():
    order = [
        "created_at", "bank_selected_at", "login_succeeded_at",
        "otp_requested_at", "otp_entered_at", "authorized_at", "completed_at",
    ]
    completed = [t for t in sample("wholesale", 1500) if t["channel"] == "instant_eft" and t["status"] == "completed"]
    assert completed
    for txn in completed:
        stamps = [txn[f] for f in order]
        assert all(s is not None for s in stamps)
        assert stamps == sorted(stamps) and len(set(stamps)) == len(stamps)


def test_failed_card_payment_has_decline_code_and_no_money_fields():
    failed = [t for t in sample("restaurant", 1500) if t["status"] == "failed" and t["channel"] == "card_present"]
    assert failed
    for txn in failed:
        assert txn["auth_code"] is None
        assert txn["decline_code"] in {"51", "59", "05", "96"}
        assert txn["interchange_fee_minor"] == txn["scheme_fee_minor"] == txn["acquirer_markup_minor"] == 0
        assert txn["settlement_date"] is None and txn["batch_id"] is None


def test_fraud_blocked_decline_carries_block_action():
    blocked = [t for t in sample("restaurant", 6000) if t["decline_code"] == "59"]
    assert blocked
    assert all(t["risk_action"] == "block" and t["risk_score"] >= 80 for t in blocked)


def test_eft_dropouts_keep_only_steps_reached():
    abandoned_early = [
        t for t in sample("wholesale", 3000)
        if t["channel"] == "instant_eft" and t["status"] == "abandoned" and t["bank_selected_at"] is None
    ]
    assert abandoned_early
    for txn in abandoned_early:
        assert txn["bank_selected"] is None and txn["login_succeeded_at"] is None


def test_card_rows_never_carry_eft_fields():
    for txn in sample("restaurant", 500):
        if txn["channel"] in {"card_present", "card_ecom", "payment_link"}:
            assert txn["bank_selected"] is None and txn["otp_entered_at"] is None


def test_card_present_has_device_and_ecom_does_not():
    rows = sample("wholesale", 600)
    assert all(t["device_id"] for t in rows if t["channel"] == "card_present")
    assert all(t["device_id"] is None for t in rows if t["channel"] != "card_present")


# ---------------------------------------------------------------------------
# Rates (seeded, large samples)
# ---------------------------------------------------------------------------

def test_channel_mix_follows_archetype():
    counts = Counter(t["channel"] for t in sample("e_commerce", 6000))
    assert counts["card_ecom"] / 6000 == pytest.approx(0.60, abs=0.03)
    assert counts["instant_eft"] / 6000 == pytest.approx(0.30, abs=0.03)
    assert "card_present" not in counts


def test_card_authorization_rate_default_and_spaza_override():
    def card_auth(archetype):
        rows = [t for t in sample(archetype, 8000) if t["channel"] == "card_present"]
        return sum(t["status"] == "completed" for t in rows) / len(rows)

    assert card_auth("restaurant") == pytest.approx(0.95, abs=0.015)
    assert card_auth("spaza_shop") == pytest.approx(0.98, abs=0.012)


def test_card_not_present_authorization_rate_is_lower():
    rows = [t for t in sample("e_commerce", 9000) if t["channel"] == "card_ecom"]
    assert sum(t["status"] == "completed" for t in rows) / len(rows) == pytest.approx(0.90, abs=0.02)


def test_auth_rate_resolution_order():
    from simulator.generator import _card_auth_rate

    assert _card_auth_rate("spaza_shop", "card_present") == 0.98   # archetype override
    assert _card_auth_rate("restaurant", "card_present") == 0.95   # default
    assert _card_auth_rate("e_commerce", "card_ecom") == 0.90      # channel override
    assert _card_auth_rate("wholesale", "payment_link") == 0.90


def test_eft_funnel_completion_rate():
    rows = [t for t in sample("wholesale", 12000) if t["channel"] == "instant_eft"]
    expected = 0.95 * 0.90 * 0.85 * 0.92 * 0.99
    assert sum(t["status"] == "completed" for t in rows) / len(rows) == pytest.approx(expected, abs=0.025)


def test_ticket_size_centres_on_archetype_mean():
    np.random.seed(3)
    tickets = [_ticket_minor("restaurant", WEDNESDAY) for _ in range(4000)]
    assert np.mean(tickets) / 100 == pytest.approx(150, rel=0.03)


def test_january_baskets_are_smaller():
    np.random.seed(3)
    june = np.mean([_ticket_minor("restaurant", date(2025, 6, 11)) for _ in range(4000)])
    january = np.mean([_ticket_minor("restaurant", date(2025, 1, 15)) for _ in range(4000)])
    assert january / june == pytest.approx(0.85, rel=0.04)


def test_ticket_never_below_floor():
    np.random.seed(4)
    assert min(_ticket_minor("spaza_shop", WEDNESDAY) for _ in range(5000)) >= 500


# ---------------------------------------------------------------------------
# Fees
# ---------------------------------------------------------------------------

def test_card_present_fee_split_adds_up(monkeypatch):
    # Compliant, debit, lowest-volume tier: interchange 0.44%, scheme 0.10%, merchant 2.45%.
    monkeypatch.setattr(np.random, "random", lambda: 0.0)
    monkeypatch.setattr(generator, "_choose", lambda dist: "debit")
    interchange, scheme, markup = _fees(merchant(quality_percentile=0.0), "card_present", 10_000)
    assert (interchange, scheme, markup) == (44, 10, 191)
    assert interchange + scheme + markup == 245


def test_high_quality_merchant_pays_lower_rate(monkeypatch):
    monkeypatch.setattr(np.random, "random", lambda: 0.0)
    monkeypatch.setattr(generator, "_choose", lambda dist: "debit")
    low = sum(_fees(merchant(quality_percentile=0.0), "card_present", 10_000))
    high = sum(_fees(merchant(quality_percentile=1.0), "card_present", 10_000))
    assert (low, high) == (245, 195)


def test_non_compliant_card_not_present_pays_penalty_interchange(monkeypatch):
    monkeypatch.setattr(np.random, "random", lambda: 0.99)  # non-compliant, no COF penalty
    monkeypatch.setattr(generator, "_choose", lambda dist: "debit")
    interchange, scheme, _ = _fees(merchant("e_commerce"), "card_ecom", 10_000)
    assert interchange == 98 and scheme == 10


def test_card_markup_never_negative():
    np.random.seed(5)
    m = merchant("e_commerce", quality_percentile=1.0)
    for _ in range(2000):
        assert min(_fees(m, "card_ecom", int(np.random.randint(100, 500_000)))) >= 0


def test_instant_eft_fee_is_rate_capped_at_r3():
    m = merchant()
    assert _fees(m, "instant_eft", 10_000) == (0, 0, 150)
    assert _fees(m, "instant_eft", 1_000_000) == (0, 0, 300)


def test_payshap_free_under_r100_then_flat_r1():
    m = merchant()
    assert _fees(m, "payshap", 9_999) == (0, 0, 0)
    assert _fees(m, "payshap", 10_000) == (0, 0, 100)


def test_completed_row_has_settlement_fields():
    done = next(t for t in sample("restaurant", 200) if t["status"] == "completed" and t["channel"] == "card_present")
    assert done["settlement_date"] > done["completed_at"].date()
    assert done["batch_id"].startswith(f"batch-{done['settlement_date'].isoformat()}")


# ---------------------------------------------------------------------------
# simulate_day
# ---------------------------------------------------------------------------

def capture_lambda(monkeypatch) -> list[float]:
    seen: list[float] = []
    monkeypatch.setattr(np.random, "poisson", lambda lam: seen.append(lam) or 0)
    return seen


def test_lambda_combines_rate_intensity_and_january_modifier(monkeypatch):
    from simulator.calendar import get_intensity_multiplier, get_tx_count_modifier

    seen = capture_lambda(monkeypatch)
    m = merchant("restaurant", merchant_state="growing")
    d = date(2025, 1, 15)
    simulate_day(m, d)
    expected = (
        m.effective_tx_rate()
        * get_intensity_multiplier(d, "restaurant", 1.05)
        * get_tx_count_modifier(d)
    )
    assert seen == [pytest.approx(expected)]
    assert get_tx_count_modifier(d) == 1.07


def test_stress_modifier_lowers_lambda(monkeypatch):
    seen = capture_lambda(monkeypatch)
    simulate_day(merchant("restaurant", merchant_state="stable"), WEDNESDAY)
    simulate_day(merchant("restaurant", merchant_state="stressed"), WEDNESDAY)
    assert seen[1] == pytest.approx(seen[0] * 0.80)


def test_churned_merchant_generates_no_payments():
    np.random.seed(1)
    m = merchant(merchant_state="churned")
    assert simulate_day(m, WEDNESDAY) == []


def test_closed_day_generates_no_payments():
    np.random.seed(1)
    assert simulate_day(merchant("spaza_shop"), SUNDAY) == []


def test_merchant_not_yet_onboarded_generates_nothing():
    np.random.seed(1)
    m = merchant(onboarded_at=date(2025, 6, 1))
    assert simulate_day(m, date(2025, 5, 14)) == []
    assert m.trading_days == 0


def test_day_is_recorded_on_the_merchant():
    np.random.seed(2)
    m = merchant()
    events = simulate_day(m, WEDNESDAY)
    payments = [e for e in events if e["event_type"] == "payment"]
    completed_gpv = sum(e["amount_minor"] for e in payments if e["status"] == "completed")
    assert m.first_transaction_at == WEDNESDAY
    assert list(m.daily_gpv_minor) == [completed_gpv]
    assert m.trading_days == 1


def test_events_are_ordered_and_valid():
    np.random.seed(2)
    events = simulate_day(merchant("grocery"), WEDNESDAY)
    assert events
    assert [e["created_at"] for e in events] == sorted(e["created_at"] for e in events)
    assert all(e["created_at"].date() == WEDNESDAY for e in events)
    for e in events:
        MerchantTransaction(**e)


def test_merchants_activity_is_busier_on_peak_weekday_than_closed_day():
    np.random.seed(9)
    wednesday = len(simulate_day(merchant("restaurant"), WEDNESDAY))
    saturday = len(simulate_day(merchant("restaurant"), date(2025, 5, 17)))
    assert saturday > wednesday


def test_same_seed_reproduces_events_exactly():
    def run():
        np.random.seed(21)
        m = merchant("restaurant", merchant_id="m-fixed")
        return simulate_day(m, WEDNESDAY) + simulate_day(m, WEDNESDAY + timedelta(days=1))

    assert run() == run()


# ---------------------------------------------------------------------------
# Refunds and chargebacks
# ---------------------------------------------------------------------------

def completed_card_payment(m: Merchant) -> dict:
    np.random.seed(5)
    while True:
        txn = simulate_transaction(m, WEDNESDAY)
        if txn["status"] == "completed" and txn["channel"] == "card_present":
            return txn


def test_followups_scheduled_for_card_payment(monkeypatch):
    m = merchant("restaurant")
    txn = completed_card_payment(m)
    monkeypatch.setattr(np.random, "random", lambda: 0.0)
    _schedule_followups(m, txn, WEDNESDAY)
    kinds = {f["kind"] for f in m.scheduled_followups}
    assert kinds == {"refund", "chargeback"}
    for f in m.scheduled_followups:
        assert f["due_date"] > WEDNESDAY
        assert f["transaction_id"] == txn["transaction_id"]


def test_eft_payment_can_be_refunded_but_not_charged_back(monkeypatch):
    m = merchant("wholesale")
    np.random.seed(6)
    txn = next(
        t for t in (simulate_transaction(m, WEDNESDAY) for _ in range(200))
        if t["channel"] == "instant_eft" and t["status"] == "completed"
    )
    m.scheduled_followups.clear()
    monkeypatch.setattr(np.random, "random", lambda: 0.0)
    _schedule_followups(m, txn, WEDNESDAY)
    assert {f["kind"] for f in m.scheduled_followups} == {"refund"}


def test_due_followup_is_emitted_once_and_links_to_the_payment(monkeypatch):
    m = merchant("restaurant")
    txn = completed_card_payment(m)
    m.scheduled_followups = [{
        "kind": "refund",
        "due_date": WEDNESDAY + timedelta(days=3),
        "transaction_id": txn["transaction_id"],
        "channel": txn["channel"],
        "payment_method": txn["payment_method"],
        "amount_minor": txn["amount_minor"],
    }]
    monkeypatch.setattr(np.random, "poisson", lambda lam: 0)

    assert simulate_day(m, WEDNESDAY + timedelta(days=2)) == []
    assert len(m.scheduled_followups) == 1

    events = simulate_day(m, WEDNESDAY + timedelta(days=3))
    assert len(events) == 1 and m.scheduled_followups == []
    refund = events[0]
    MerchantTransaction(**refund)
    assert refund["event_type"] == "refund"
    assert refund["amount_minor"] == txn["amount_minor"]
    assert refund["correlation_id"] == txn["transaction_id"]
    assert refund["transaction_id"] != txn["transaction_id"]
    assert refund["interchange_fee_minor"] == 0
    assert simulate_day(m, WEDNESDAY + timedelta(days=4)) == []


def test_followups_still_arrive_after_the_merchant_churns(monkeypatch):
    m = merchant("restaurant", merchant_state="churned")
    m.scheduled_followups = [{
        "kind": "chargeback", "due_date": WEDNESDAY, "transaction_id": "t-1",
        "channel": "card_present", "payment_method": "visa", "amount_minor": 5000,
    }]
    events = simulate_day(m, WEDNESDAY)
    assert [e["event_type"] for e in events] == ["chargeback"]


def test_refund_and_chargeback_volumes_track_archetype_rates():
    np.random.seed(12)
    m = merchant("e_commerce")
    for day in range(200):
        simulate_day(m, WEDNESDAY + timedelta(days=day))
    assert m.trading_days == 200
    # Pending followups were drawn at about refund_rate 5% and fraud_rate 1% of completed card sales.
    kinds = Counter(f["kind"] for f in m.scheduled_followups)
    assert kinds["refund"] > kinds["chargeback"]


# ---------------------------------------------------------------------------
# Dormancy
# ---------------------------------------------------------------------------

def test_dormant_merchant_generates_no_payments_but_still_gets_followups():
    np.random.seed(1)
    m = merchant("restaurant")
    simulate_day(m, WEDNESDAY)  # start trading
    m.dormant_since = WEDNESDAY
    m.scheduled_followups = [{
        "kind": "refund", "due_date": WEDNESDAY + timedelta(days=1), "transaction_id": "t-1",
        "channel": "card_present", "payment_method": "visa", "amount_minor": 9000,
    }]
    events = simulate_day(m, WEDNESDAY + timedelta(days=1))
    assert [e["event_type"] for e in events] == ["refund"]
    assert list(m.daily_gpv_minor)[-1] == 0
    assert m.days_without_txn == 1


def test_going_dormant_stops_payments_the_same_day(monkeypatch):
    np.random.seed(1)
    m = merchant("restaurant")
    simulate_day(m, WEDNESDAY)
    monkeypatch.setattr(m, "maybe_go_dormant", lambda d: setattr(m, "dormant_since", d) or True)
    assert simulate_day(m, WEDNESDAY + timedelta(days=1)) == []


def test_merchant_that_never_traded_is_not_rolled_for_dormancy(monkeypatch):
    called = []
    m = merchant("restaurant")
    monkeypatch.setattr(np.random, "poisson", lambda lam: 0)
    monkeypatch.setattr(np.random, "random", lambda: called.append(1) or 0.0)
    simulate_day(m, WEDNESDAY)
    assert m.dormant_since is None
