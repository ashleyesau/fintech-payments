"""
risk.py: Chain 5 fraud and risk rules.

Two jobs:

  - Fraud clusters. A merchant occasionally has a burst of abnormal activity for a few
    days (a compromised terminal, card testing, refund abuse). The generator reads
    cluster_active() to inflate payment counts, ticket sizes, refunds, chargebacks and
    OTP failures. Without them the triggers below would only ever fire on Poisson noise.
  - Rule evaluation. evaluate_risk() looks at one merchant's events for a day, fires the
    six Chain 5 triggers, applies each risk score delta to the merchant and returns
    risk_events rows. A refund spike that pushes the score above 55 opens a 30-day
    window in which the next chargeback adds 20 more and moves a Stable merchant to
    Stressed.

Call order for a day: generator.simulate_day(), then evaluate_risk() with the events it
returned. In Phase 6 each risk event commits in the same transaction as the event that
triggered it.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Optional

import numpy as np

from simulator.archetypes import get_archetype, hour_probabilities
from simulator.common import SAST, new_id
from simulator.config import load_causal_rules

_RULES = load_causal_rules()
_TRIGGERS = _RULES["fraud_triggers"]
_EVAL = _RULES["risk_events"]
_CLUSTER = _RULES["fraud_cluster"]
_ESCALATION = _RULES["fraud_escalation"]

_CARD_CHANNELS = {"card_present", "card_ecom", "payment_link"}
_RULE_IDS = {
    "refund_spike": "rule-refund-spike-7d",
    "velocity_breach": "rule-velocity-1h",
    "chargeback_accumulation": "rule-chargeback-90d",
    "scheme_warning": "rule-vamp-ratio",
    "large_txn_flag": "rule-large-txn",
    "otp_abuse": "rule-otp-abuse",
}
_ESCALATION_RULE_ID = "rule-refund-chargeback-escalation"


# ---------------------------------------------------------------------------
# Fraud clusters
# ---------------------------------------------------------------------------

def cluster_active(merchant: Any, sim_date: date) -> bool:
    return merchant.fraud_cluster_until is not None and sim_date <= merchant.fraud_cluster_until


def cluster_value(key: str) -> Any:
    """A fraud_cluster parameter from causal_rules.yaml."""
    return _CLUSTER[key]


def maybe_start_fraud_cluster(merchant: Any, sim_date: date) -> bool:
    """Roll for the start of a cluster. Only trading, non-dormant merchants have one, and
    one already in a cluster does not start another."""
    if (
        merchant.first_transaction_at is None
        or merchant.dormant_since is not None
        or merchant.merchant_state == "churned"
        or cluster_active(merchant, sim_date)
    ):
        return False
    monthly = min(
        1.0, get_archetype(merchant.archetype).fraud_rate * _CLUSTER["monthly_start_probability_per_fraud_rate"]
    )
    daily = 1.0 - (1.0 - monthly) ** (1.0 / 30.0)
    if np.random.random() >= daily:
        return False
    low, high = _CLUSTER["duration_days"]
    merchant.fraud_cluster_until = sim_date + timedelta(days=int(np.random.randint(low, high + 1)) - 1)
    return True


# ---------------------------------------------------------------------------
# Rule evaluation
# ---------------------------------------------------------------------------

def _action_time(event: Optional[dict], sim_date: date) -> datetime:
    """A moment just after the triggering event, or the end of the day for rules that
    look at the day as a whole."""
    if event is not None:
        return event["created_at"] + timedelta(seconds=int(np.random.randint(1, 6)))
    return datetime(sim_date.year, sim_date.month, sim_date.day, 23, 55, tzinfo=SAST)


def _emit(
    merchant: Any, rule_type: str, sim_date: date, entity: Optional[dict], delta: int,
    action: str, rule_id: Optional[str] = None,
) -> dict:
    merchant.apply_risk_delta(delta, sim_date)
    return {
        "alert_id": new_id(),
        "entity_type": "transaction" if entity is not None else "merchant",
        "entity_id": entity["transaction_id"] if entity is not None else merchant.merchant_id,
        "merchant_id": merchant.merchant_id,
        "rule_id": rule_id or _RULE_IDS[rule_type],
        "rule_type": rule_type,
        "risk_score": merchant.risk_score,
        "risk_score_delta": delta,
        "action": action,
        "action_by": "system_auto",
        "action_ts": _action_time(entity, sim_date),
        "resolved_ts": None,
        "outcome": None,
    }


def _may_fire(merchant: Any, rule_type: str, sim_date: date) -> bool:
    last = merchant.last_risk_event.get(rule_type)
    return last is None or (sim_date - last).days >= _EVAL["cooldown_days"][rule_type]


def _fire(merchant: Any, rule_type: str, sim_date: date, entity: Optional[dict], action: str = "flag_review") -> dict:
    merchant.last_risk_event[rule_type] = sim_date
    return _emit(merchant, rule_type, sim_date, entity, _TRIGGERS[rule_type]["risk_score_delta"], action)


def _sums(history: list, index: int, days: int) -> int:
    return sum(day[index] for day in list(history)[-days:])


def _hourly(events: list[dict], predicate) -> dict[int, list[dict]]:
    by_hour: dict[int, list[dict]] = {}
    for e in events:
        if predicate(e):
            by_hour.setdefault(e["created_at"].hour, []).append(e)
    return by_hour


def evaluate_risk(merchant: Any, events: list[dict], sim_date: date) -> list[dict]:
    """
    Fire the Chain 5 triggers for one merchant's transaction events on sim_date.

    `events` is what generator.simulate_day() returned: payments, refunds and chargebacks.
    Returns risk_events rows (possibly none) and applies each delta to the merchant, so
    merchant.risk_score reflects them. Call it every day, including days with no events,
    so the rolling windows advance.
    """
    archetype = get_archetype(merchant.archetype)
    payments = [e for e in events if e["event_type"] == "payment"]
    completed = [e for e in payments if e["status"] == "completed"]
    card_completed = [e for e in completed if e["channel"] in _CARD_CHANNELS]
    refunds = [e for e in events if e["event_type"] == "refund"]
    chargebacks = [e for e in events if e["event_type"] == "chargeback"]
    merchant.risk_history.append((len(completed), len(card_completed), len(refunds), len(chargebacks)))
    history = merchant.risk_history

    rows: list[dict] = []

    # Refund spike: refunds in the last 7 days against three times the archetype baseline.
    if refunds and _may_fire(merchant, "refund_spike", sim_date):
        refunds_7d, completed_7d = _sums(history, 2, 7), _sums(history, 0, 7)
        trigger = _TRIGGERS["refund_spike"]["refund_rate_7d_multiple_of_baseline"]
        expected = archetype.refund_rate * completed_7d
        if (
            refunds_7d >= _EVAL["min_refunds_for_spike"]
            and completed_7d > 0
            and refunds_7d / completed_7d > trigger * archetype.refund_rate
            and refunds_7d > expected + _EVAL["count_sigma"] * expected ** 0.5
        ):
            merchant.last_risk_event["refund_spike"] = sim_date
            delta = _TRIGGERS["refund_spike"]["risk_score_delta"]
            # Above the manual review score the spike is flagged and opens the escalation window.
            flagged = merchant.risk_score + delta > _ESCALATION["manual_review_risk_score_above"]
            rows.append(_emit(merchant, "refund_spike", sim_date, refunds[-1], delta,
                              "flag_review" if flagged else "allow"))
            if flagged:
                merchant.escalation_until = sim_date + timedelta(days=_ESCALATION["chargeback_followup_days"])

    # Escalation: a chargeback inside the window after a flagged refund spike.
    if (
        chargebacks
        and merchant.escalation_until is not None
        and sim_date <= merchant.escalation_until
    ):
        rows.append(_emit(
            merchant, "chargeback_accumulation", sim_date, chargebacks[-1],
            _ESCALATION["chargeback_followup_delta"], "flag_review", rule_id=_ESCALATION_RULE_ID,
        ))
        if _ESCALATION["stable_to_stressed_on_followup"]:
            merchant.escalate_to_stressed()
        merchant.escalation_until = None

    # Chargeback accumulation: enough chargebacks in 90 days, and well above what the
    # archetype's fraud rate predicts.
    if chargebacks and _may_fire(merchant, "chargeback_accumulation", sim_date):
        count_90d = _sums(history, 3, 90)
        expected = archetype.fraud_rate * _sums(history, 1, 90)
        if (
            count_90d >= _TRIGGERS["chargeback_accumulation"]["chargeback_count_90d"]
            and count_90d >= _EVAL["chargeback_baseline_multiple"] * expected
            and count_90d > expected + _EVAL["count_sigma"] * expected ** 0.5
        ):
            rows.append(_fire(merchant, "chargeback_accumulation", sim_date, chargebacks[-1]))

    # Visa VAMP ratio: chargebacks over completed card payments in 30 days.
    if chargebacks and _may_fire(merchant, "scheme_warning", sim_date):
        card_30d, chargebacks_30d = _sums(history, 1, 30), _sums(history, 3, 30)
        expected = archetype.fraud_rate * card_30d
        if (
            card_30d >= _EVAL["min_card_payments_for_vamp"]
            and chargebacks_30d / card_30d > _TRIGGERS["scheme_warning"]["vamp_ratio"]
            and chargebacks_30d > expected + _EVAL["count_sigma"] * expected ** 0.5
        ):
            rows.append(_fire(merchant, "scheme_warning", sim_date, None))

    # Velocity: payments in one hour above the hour's expected count plus 3 sigma.
    if payments and _may_fire(merchant, "velocity_breach", sim_date):
        probabilities = hour_probabilities(merchant.archetype)
        sigma = _EVAL["velocity_sigma"]
        worst: Optional[tuple[float, list[dict]]] = None
        for hour, hour_events in _hourly(payments, lambda e: True).items():
            mean = merchant.baseline_daily_tx * probabilities[hour]
            excess = len(hour_events) - (mean + sigma * mean ** 0.5)
            if len(hour_events) >= _EVAL["velocity_min_count"] and excess > 0:
                if worst is None or excess > worst[0]:
                    worst = (excess, hour_events)
        if worst is not None:
            rows.append(_fire(merchant, "velocity_breach", sim_date, worst[1][-1]))

    # Large transaction: a payment far above the archetype's mean ticket.
    if payments and _may_fire(merchant, "large_txn_flag", sim_date):
        limit = _TRIGGERS["large_txn_flag"]["multiple_of_archetype_mean_ticket"] * archetype.mean_ticket_zar * 100
        large = [e for e in payments if e["amount_minor"] > limit]
        if large:
            rows.append(_fire(merchant, "large_txn_flag", sim_date, large[-1]))

    # OTP abuse: several EFT payments failing at the OTP step in one hour.
    if payments and _may_fire(merchant, "otp_abuse", sim_date):
        limit = _TRIGGERS["otp_abuse"]["otp_failures_session"]
        failed = _hourly(
            payments, lambda e: e["otp_requested_at"] is not None and e["otp_entered_at"] is None
        )
        sessions = [v for v in failed.values() if len(v) >= limit]
        if sessions:
            rows.append(_fire(merchant, "otp_abuse", sim_date, max(sessions, key=len)[-1], action="block"))

    return rows
