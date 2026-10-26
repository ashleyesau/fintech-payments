"""
generator.py: Core event generation functions.

simulate_day() is the main entry point: given a merchant and a calendar date,
it returns the list of events that merchant would generate on that day.

Phase 5.1 (transactions) is implemented here: payments through the card or instant
EFT funnel, fees, settlement, and the refunds and chargebacks that follow earlier
payments. Loan applications, repayments and risk events are later Phase 5 steps.
"""
from __future__ import annotations

import string
import uuid
import zlib
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Optional

import numpy as np

from simulator.archetypes import get_archetype
from simulator.calendar import (
    get_intensity_multiplier,
    get_ticket_size_modifier,
    get_tx_count_modifier,
)
from simulator.config import load_fees, load_payments, settings
from simulator.payout import settlement_batch_id, settlement_date

_PAY = load_payments()
_FEES = load_fees()

# South Africa has no daylight saving; archetype hourly weights are local time.
SAST = timezone(timedelta(hours=2))

_CARD_FEE_CHANNELS = {"card_present", "card_ecom", "payment_link"}
_ECOM_CHANNELS = {"card_ecom", "payment_link"}
_AUTH_CODE_CHARS = string.ascii_uppercase + string.digits


# ---------------------------------------------------------------------------
# Sampling helpers
# ---------------------------------------------------------------------------

@lru_cache(maxsize=None)
def _cumulative(items: tuple) -> tuple[tuple, np.ndarray]:
    keys = tuple(k for k, _ in items)
    return keys, np.cumsum([w for _, w in items], dtype=float)


def _choose(dist: dict[str, float]) -> str:
    """Weighted draw. Weights need not sum to 1. Cumulative sums are cached because
    this runs several times per transaction."""
    keys, cumulative = _cumulative(tuple(dist.items()))
    index = int(np.searchsorted(cumulative, np.random.random() * cumulative[-1], side="right"))
    return keys[min(index, len(keys) - 1)]


def _seconds(step: str) -> timedelta:
    low, high = _PAY["step_seconds"][step]
    return timedelta(seconds=int(np.random.randint(low, high + 1)))


@lru_cache(maxsize=None)
def _hour_probabilities(archetype_name: str) -> np.ndarray:
    weights = np.array(get_archetype(archetype_name).hourly_weights, dtype=float)
    return weights / weights.sum()


def _random_time(sim_date: date, archetype_name: str) -> datetime:
    hour = int(np.random.choice(24, p=_hour_probabilities(archetype_name)))
    return datetime(
        sim_date.year, sim_date.month, sim_date.day,
        hour, int(np.random.randint(0, 60)), int(np.random.randint(0, 60)),
        tzinfo=SAST,
    )


def _channel_mix(archetype_name: str) -> dict[str, float]:
    mix = dict(_PAY["channel_mix"][archetype_name])
    if not settings.enable_payshap:
        mix.pop("payshap", None)
    return mix


def _ticket_minor(archetype_name: str, sim_date: date) -> int:
    archetype = get_archetype(archetype_name)
    ticket = np.random.normal(archetype.mean_ticket_zar, archetype.ticket_std_zar)
    ticket *= get_ticket_size_modifier(sim_date, archetype_name)
    # A normal draw can go to zero or below; keep tickets at a plausible minimum.
    ticket = max(ticket, 0.1 * archetype.mean_ticket_zar)
    return int(round(ticket * 100))


def _new_id() -> str:
    """UUID4 drawn from numpy's RNG so a seeded run reproduces its identifiers."""
    return str(uuid.UUID(bytes=np.random.bytes(16), version=4))


def _auth_code() -> str:
    return "".join(_AUTH_CODE_CHARS[i] for i in np.random.randint(0, len(_AUTH_CODE_CHARS), size=6))


def _risk(blocked: bool) -> tuple[int, str]:
    cfg = _PAY["risk"]
    if blocked:
        low, high = cfg["blocked_range"]
        score = int(np.random.randint(low, high + 1))
    else:
        score = max(0, min(99, int(round(np.random.normal(cfg["mean"], cfg["std"])))))
    if score >= cfg["block_min"]:
        action = "block"
    elif score >= cfg["review_min"]:
        action = "review"
    else:
        action = "allow"
    return score, action


def _device_fields(merchant: Any, channel: str) -> tuple[Optional[str], Optional[str]]:
    """Stable per-merchant POS device and firmware for card-present sales."""
    if channel != "card_present":
        return None, None
    versions = _PAY["pos_versions"]
    version = versions[zlib.crc32(merchant.merchant_id.encode()) % len(versions)]
    return f"POS-{merchant.merchant_id[:8]}", version


# ---------------------------------------------------------------------------
# Fees
# ---------------------------------------------------------------------------

def _fees(merchant: Any, channel: str, amount_minor: int) -> tuple[int, int, int]:
    """(interchange, scheme, acquirer markup) in cents for a completed transaction."""
    fee_class = _FEES["fee_class"][channel]

    if fee_class == "instant_eft":
        cfg = _FEES["merchant_rate"]["instant_eft"]
        return 0, 0, min(int(round(amount_minor * cfg["rate"])), cfg["cap_minor"])

    if fee_class == "payshap":
        cfg = _FEES["merchant_rate"]["payshap"]
        return 0, 0, 0 if amount_minor < cfg["free_below_minor"] else cfg["flat_minor"]

    if fee_class == "card_present":
        compliant = np.random.random() < _PAY["emv_compliance"]
        rates = _FEES["merchant_rate"]["card_present"]
        # Higher-quality merchants carry more volume and sit in a cheaper tier.
        merchant_rate = rates["max"] - (rates["max"] - rates["min"]) * min(merchant.quality_percentile, 1.0)
    else:
        compliant = np.random.random() < _PAY["threeds_compliance"]
        merchant_rate = _FEES["merchant_rate"]["card_not_present"]

    case = "both_compliant" if compliant else "issuer_only"
    card_type = _choose(_PAY["card_type_mix"])
    scheme_rate = _FEES["scheme_fee_rate"]
    if channel in _ECOM_CHANNELS and np.random.random() < _FEES["not_tokenized_cof_share_ecom"]:
        scheme_rate += _FEES["not_tokenized_cof_rate"]

    interchange = int(round(amount_minor * _FEES["interchange"][fee_class][case][card_type]))
    scheme = int(round(amount_minor * scheme_rate))
    markup = max(0, int(round(amount_minor * merchant_rate)) - interchange - scheme)
    return interchange, scheme, markup


# ---------------------------------------------------------------------------
# Single transaction
# ---------------------------------------------------------------------------

def _blank_transaction(merchant: Any, channel: str, created_at: datetime, amount_minor: int) -> dict:
    device_id, pos_version = _device_fields(merchant, channel)
    return {
        "transaction_id": _new_id(),
        "merchant_id": merchant.merchant_id,
        "event_type": "payment",
        "channel": channel,
        "status": "initiated",
        "entry_mode": None,
        "payment_method": None,
        "bank_selected": None,
        "amount_minor": amount_minor,
        "currency": "ZAR",
        "interchange_fee_minor": 0,
        "scheme_fee_minor": 0,
        "acquirer_markup_minor": 0,
        "mcc": get_archetype(merchant.archetype).mcc,
        "auth_code": None,
        "decline_code": None,
        "risk_score": None,
        "risk_action": None,
        "created_at": created_at,
        "bank_selected_at": None,
        "login_succeeded_at": None,
        "otp_requested_at": None,
        "otp_entered_at": None,
        "authorized_at": None,
        "completed_at": None,
        "refunded_at": None,
        "settlement_date": None,
        "batch_id": None,
        "device_id": device_id,
        "pos_version": pos_version,
        "is_test": False,
        "correlation_id": f"corr-{_new_id().replace('-', '')[:12]}",
    }


def _complete(txn: dict, merchant: Any, authorized_at: datetime, completed_at: datetime) -> None:
    txn["status"] = "completed"
    txn["auth_code"] = _auth_code()
    txn["authorized_at"] = authorized_at
    txn["completed_at"] = completed_at
    (
        txn["interchange_fee_minor"],
        txn["scheme_fee_minor"],
        txn["acquirer_markup_minor"],
    ) = _fees(merchant, txn["channel"], txn["amount_minor"])
    settle_on = settlement_date(completed_at.date(), txn["channel"])
    txn["settlement_date"] = settle_on
    txn["batch_id"] = settlement_batch_id(merchant.merchant_id, settle_on)


def _card_style_path(txn: dict, merchant: Any, auth_rate: float) -> None:
    """initiated -> authorized -> completed, with failed as a terminal state."""
    created_at = txn["created_at"]
    if np.random.random() < auth_rate:
        authorized_at = created_at + _seconds("card_authorize")
        completed_at = authorized_at + _seconds("card_complete")
        _complete(txn, merchant, authorized_at, completed_at)
        txn["risk_score"], txn["risk_action"] = _risk(blocked=False)
        return

    code = _choose(_PAY["card_decline_codes"])
    txn["status"] = "failed"
    txn["decline_code"] = code
    txn["risk_score"], txn["risk_action"] = _risk(blocked=code == _PAY["decline_codes"]["fraud_blocked"])


def _eft_path(txn: dict, merchant: Any) -> None:
    """Seven-state instant EFT funnel. A dropout leaves the timestamps of the steps reached."""
    funnel = _PAY["eft_funnel"]
    codes = _PAY["decline_codes"]
    t = txn["created_at"]

    def fail(status: str, code: Optional[str]) -> None:
        txn["status"] = status
        txn["decline_code"] = code
        txn["risk_score"], txn["risk_action"] = _risk(blocked=False)

    if np.random.random() >= funnel["bank_selected"]:
        return fail("abandoned", None)
    t += _seconds("bank_selected")
    txn["bank_selected"] = _choose(get_archetype(merchant.archetype).bank_weights)
    txn["bank_selected_at"] = t

    if np.random.random() >= funnel["login_succeeded"]:
        return fail("failed", codes["eft_login_failed"])
    t += _seconds("login_succeeded")
    txn["login_succeeded_at"] = t

    t += _seconds("otp_requested")
    txn["otp_requested_at"] = t
    if np.random.random() >= funnel["otp_entered"]:
        outcome = _choose(_PAY["eft_otp_failure_split"])
        return fail(outcome, codes["technical_error"] if outcome == "failed" else None)
    t += _seconds("otp_entered")
    txn["otp_entered_at"] = t

    if np.random.random() >= funnel["authorized"]:
        return fail("failed", codes["eft_insufficient_funds"])
    authorized_at = t + _seconds("eft_authorize")
    if np.random.random() >= funnel["completed"]:
        txn["authorized_at"] = authorized_at
        return fail("failed", codes["technical_error"])
    _complete(txn, merchant, authorized_at, authorized_at + _seconds("eft_complete"))
    txn["risk_score"], txn["risk_action"] = _risk(blocked=False)


def _card_auth_rate(archetype_name: str, channel: str) -> float:
    return _PAY["card_auth_rate_overrides"].get(
        archetype_name,
        _PAY["card_auth_rate_by_channel"].get(channel, _PAY["card_auth_rate"]),
    )


def simulate_transaction(merchant: Any, sim_date: date) -> dict:
    """
    Generate a single payment event for a merchant.

    Picks a channel from the archetype mix, runs the channel's state machine, derives
    fees and settlement for completed payments, and returns a merchant_transactions
    row. The row carries the final status and the timestamps of every step reached.
    """
    channel = _choose(_channel_mix(merchant.archetype))
    created_at = _random_time(sim_date, merchant.archetype)
    txn = _blank_transaction(merchant, channel, created_at, _ticket_minor(merchant.archetype, sim_date))

    if channel == "instant_eft":
        txn["payment_method"] = "eft"
        _eft_path(txn, merchant)
    elif channel == "payshap":
        txn["payment_method"] = "payshap"
        _card_style_path(txn, merchant, _PAY["payshap_auth_rate"])
    else:
        txn["payment_method"] = _choose(_PAY["payment_method_mix"]["card"])
        txn["entry_mode"] = (
            _choose(_PAY["card_entry_mode_mix"]) if channel == "card_present" else "ecom"
        )
        _card_style_path(txn, merchant, _card_auth_rate(merchant.archetype, channel))
    return txn


# ---------------------------------------------------------------------------
# Refunds and chargebacks
# ---------------------------------------------------------------------------

def _schedule_followups(merchant: Any, txn: dict, sim_date: date) -> None:
    """Decide at payment time whether a refund or chargeback will follow, and when.

    Refunds apply to every channel at the archetype refund_rate. Chargebacks are a card
    dispute, so only card channels, at the archetype fraud_rate (Causal Rules uses the
    same figures for the baseline chargeback rate and the fraud event probability).
    """
    archetype = get_archetype(merchant.archetype)
    delays = _PAY["followup_delay_days"]
    kinds = [("refund", archetype.refund_rate)]
    if txn["channel"] in _CARD_FEE_CHANNELS:
        kinds.append(("chargeback", archetype.fraud_rate))
    for kind, rate in kinds:
        if np.random.random() < rate:
            low, high = delays[kind]
            merchant.scheduled_followups.append({
                "kind": kind,
                "due_date": sim_date + timedelta(days=int(np.random.randint(low, high + 1))),
                "transaction_id": txn["transaction_id"],
                "channel": txn["channel"],
                "payment_method": txn["payment_method"],
                "amount_minor": txn["amount_minor"],
            })


def _followup_event(merchant: Any, followup: dict, sim_date: date) -> dict:
    created_at = _random_time(sim_date, merchant.archetype)
    event = _blank_transaction(merchant, followup["channel"], created_at, followup["amount_minor"])
    event["event_type"] = followup["kind"]
    event["status"] = "completed"
    event["payment_method"] = followup["payment_method"]
    event["completed_at"] = created_at + _seconds("card_complete")
    # Event Schemas has no parent column, so correlation_id points at the original payment.
    event["correlation_id"] = followup["transaction_id"]
    event["device_id"] = None
    event["pos_version"] = None
    return event


# ---------------------------------------------------------------------------
# One merchant, one day
# ---------------------------------------------------------------------------

def simulate_day(merchant: Any, sim_date: date) -> list[dict]:
    """
    Generate all transaction events for one merchant on one simulated day.

    Returns payments dated sim_date plus any refunds and chargebacks that fall due,
    ordered by created_at. A merchant that goes dormant today (maybe_go_dormant) stops
    generating payments from today. Also records the day on the merchant (GPV and
    transaction count), so the caller should then run merchant.evaluate_state() and decay_risk().
    """
    events = [
        _followup_event(merchant, f, sim_date)
        for f in merchant.scheduled_followups
        if f["due_date"] <= sim_date
    ]
    merchant.scheduled_followups = [
        f for f in merchant.scheduled_followups if f["due_date"] > sim_date
    ]

    payments: list[dict] = []
    not_yet_onboarded = merchant.onboarded_at is not None and sim_date < merchant.onboarded_at
    if not not_yet_onboarded:
        merchant.maybe_go_dormant(sim_date)
        lam = 0.0
        if merchant.dormant_since is None:
            intensity = get_intensity_multiplier(sim_date, merchant.archetype, merchant.stress_modifier())
            lam = merchant.effective_tx_rate() * intensity * get_tx_count_modifier(sim_date)
        count = int(np.random.poisson(lam)) if lam > 0 else 0
        for _ in range(count):
            txn = simulate_transaction(merchant, sim_date)
            payments.append(txn)
            if txn["status"] == "completed":
                _schedule_followups(merchant, txn, sim_date)
        gpv = sum(p["amount_minor"] for p in payments if p["status"] == "completed")
        merchant.record_day(sim_date, gpv, len(payments))

    events.extend(payments)
    events.sort(key=lambda e: e["created_at"])
    return events


def simulate_loan_application(merchant: Any, sim_date: date) -> dict | None:
    """
    Probabilistically generate a loan application event.

    Returns a loan_applications row dict, or None if the merchant does not
    apply this day. Eligibility gates from Chain 3 of the causal rules.

    Phase 5.2 implementation.
    """
    raise NotImplementedError("Phase 5.2")
