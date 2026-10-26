"""
state.py: Stateful domain objects: Merchant and Loan. Mandate state rides on Loan
(mandate_id, mandate_status), as Event Schemas stamps it inline on each repayment.

These classes hold the in-memory state that evolves across simulated days.
State is never read from Postgres during a run; it lives here and is
checkpointed to disk at the end of each session.

Merchant implements Chain 2 (health state transitions) and the Chain 4 risk
score rules. Every threshold, probability and delta is read from
config/causal_rules.yaml.
"""
from __future__ import annotations

import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Optional

import numpy as np

from simulator.archetypes import get_archetype, sample_quality_percentile
from simulator.config import load_calibration, load_causal_rules

_RULES = load_causal_rules()
_ST = _RULES["state_transitions"]
_PROB = _ST["probabilities"]
_RISK = _RULES["risk_score"]
_FORCED = _RULES["forced_missed"]
_CHURN_MULTIPLIER = load_calibration().get("lifecycle", {}).get("churn_multiplier", 1.0)

# Structural, not tunable: the state engine compares two 30-day windows (60 days), and
# loan eligibility needs GPV over 90 days, so the buffer holds 90.
_WINDOW_DAYS = 30
_HISTORY_DAYS = 2 * _WINDOW_DAYS
_BUFFER_DAYS = 90


def risk_delta(name: str) -> int:
    """Chain 4 risk score delta by event name (keys of risk_score.deltas)."""
    return _RISK["deltas"][name]


def _roll(probability: float) -> bool:
    return bool(np.random.random() < probability)


# ---------------------------------------------------------------------------
# Merchant
# ---------------------------------------------------------------------------

MerchantState = str  # "new" | "growing" | "stable" | "stressed" | "delinquent" | "churned"


@dataclass
class Merchant:
    merchant_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    archetype: str = ""
    province: str = "gauteng"
    is_metro: bool = True

    # Lifecycle
    onboarded_at: Optional[date] = None
    first_transaction_at: Optional[date] = None
    merchant_state: MerchantState = "new"
    months_on_platform: int = 0

    # Quality tier (drawn from Beta(1.5, 4.0) at creation, fixed for lifetime)
    quality_percentile: float = field(default_factory=sample_quality_percentile)

    # Risk
    risk_score: int = 10          # 0-100, higher = riskier
    loan_count: int = 0
    has_active_loan: bool = False
    is_written_off: bool = False

    # Internal counters (not persisted to Postgres)
    consecutive_good_days: int = 0
    days_without_txn: int = 0
    trading_days: int = 0
    days_in_state: int = 0
    # Consecutive days the 30-day growth rate has been at or above / below the
    # growth threshold. Chain 2 speaks in months; 30 days is one month.
    days_growth_high: int = 0
    days_growth_low: int = 0
    missed_repayment_flag: bool = False
    consecutive_successful_repayments: int = 0
    last_adverse_event_date: Optional[date] = None
    last_risk_decay_date: Optional[date] = None

    # Chain 5 bookkeeping, driven by risk.py. risk_history holds one tuple per day:
    # (completed payments, completed card payments, refunds, chargebacks).
    risk_history: deque = field(default_factory=lambda: deque(maxlen=90))
    baseline_daily_tx: float = 0.0           # expected payments today, before any fraud cluster
    fraud_cluster_until: Optional[date] = None
    last_risk_event: dict = field(default_factory=dict)   # rule_type -> date it last fired
    escalation_until: Optional[date] = None  # a chargeback before this date escalates a refund spike

    # Lending. A loan is active from disbursement until it is repaid or written off.
    active_loan: Optional["Loan"] = None
    prior_loan_repaid: bool = False
    last_application_date: Optional[date] = None
    # Approved applications waiting for their disbursement day (0 to 3 days after the decision).
    scheduled_disbursements: list = field(default_factory=list)

    # Refunds and chargebacks that will be emitted on a later day. Each entry is a dict
    # with kind, due_date and the original payment's identifying fields.
    scheduled_followups: list = field(default_factory=list)

    # Set when the merchant permanently stops transacting (see maybe_go_dormant).
    dormant_since: Optional[date] = None
    # Whether a dormant merchant closed its business (stops repaying) or moved to another
    # acquirer (keeps repaying). None until the merchant goes dormant.
    business_closed: Optional[bool] = None

    # Daily GPV in minor units, oldest first. Calendar days since the first
    # transaction; closed days are recorded as zero. The state engine reads the latest
    # 60 days; gpv_90d_minor() reads all 90.
    daily_gpv_minor: deque = field(default_factory=lambda: deque(maxlen=_BUFFER_DAYS))

    def effective_tx_rate(self) -> float:
        """Base tx rate adjusted for quality tier. Chain 1 multiplier."""
        archetype = get_archetype(self.archetype)
        return archetype.tx_per_day * (0.4 + 1.6 * self.quality_percentile)

    def stress_modifier(self) -> float:
        """Chain 2 multiplier fed back into Chain 1 event intensity."""
        return _ST["stress_modifier"][self.merchant_state]

    def snapshot(self, snapshot_at: datetime) -> dict:
        """The merchants dimension row for this merchant at snapshot_at (Stream 5).

        Dates become midnight timestamps in snapshot_at's timezone, since the in-memory
        state keeps calendar dates only.
        """
        def stamp(d: Optional[date]) -> Optional[datetime]:
            if d is None:
                return None
            return datetime.combine(d, time.min, tzinfo=snapshot_at.tzinfo)

        return {
            "merchant_id": self.merchant_id,
            "archetype": self.archetype,
            "province": self.province,
            "is_metro": self.is_metro,
            "onboarded_at": stamp(self.onboarded_at),
            "first_transaction_at": stamp(self.first_transaction_at),
            "merchant_state": self.merchant_state,
            "risk_score": self.risk_score,
            "loan_count": self.loan_count,
            "has_active_loan": self.has_active_loan,
            "snapshot_at": snapshot_at,
        }

    # -----------------------------------------------------------------------
    # Daily bookkeeping
    # -----------------------------------------------------------------------

    def record_day(self, sim_date: date, gpv_minor: int, txn_count: int) -> None:
        """Record one simulated day. Nothing is recorded before the first transaction,
        so pre-trading zeros never enter the GPV windows."""
        if txn_count > 0 and self.first_transaction_at is None:
            self.first_transaction_at = sim_date
        if self.first_transaction_at is None:
            return
        self.daily_gpv_minor.append(gpv_minor)
        self.trading_days += 1
        self.days_without_txn = 0 if txn_count > 0 else self.days_without_txn + 1

    def gpv_90d_minor(self) -> int:
        """GPV over the latest 90 days (fewer for a merchant with a shorter history)."""
        return sum(self.daily_gpv_minor)

    def gpv_30d_avg_minor(self) -> float:
        """Average daily GPV over the latest 30 days (fewer for a short history)."""
        recent = list(self.daily_gpv_minor)[-_WINDOW_DAYS:]
        return sum(recent) / len(recent) if recent else 0.0

    def _latest_60_days(self) -> Optional[list]:
        """The latest 60 daily GPV values, or None until 60 days exist."""
        if len(self.daily_gpv_minor) < _HISTORY_DAYS:
            return None
        return list(self.daily_gpv_minor)[-_HISTORY_DAYS:]

    # -----------------------------------------------------------------------
    # Merchant going dark
    # -----------------------------------------------------------------------

    def maybe_go_dormant(self, sim_date: date) -> bool:
        """Roll the archetype's churn hazard once. On a hit the merchant stops
        transacting for good; the state engine then carries it to Churned through the
        usual GPV decline, because no transactions for 30 days is what Churned means.

        monthly_churn_prob is "the monthly chance the merchant permanently stops
        transacting" (Simulator Design), so it becomes a daily hazard of
        1 - (1 - p) ** (1 / 30), scaled by churn_multiplier in calibration.yaml.
        Only merchants that have started trading can go dormant. Reactivation is not
        modelled. A dormant merchant either closed its business or moved to another
        acquirer; only closure stops it repaying a loan (forced_missed.dormant_closure_share).
        """
        if self.dormant_since is not None or self.first_transaction_at is None:
            return False
        monthly = get_archetype(self.archetype).monthly_churn_prob * _CHURN_MULTIPLIER
        daily = 1.0 - (1.0 - min(monthly, 1.0)) ** (1.0 / 30.0)
        if _roll(daily):
            self.dormant_since = sim_date
            self.business_closed = _roll(_FORCED["dormant_closure_share"])
            return True
        return False

    # -----------------------------------------------------------------------
    # Chain 2: state evaluation
    # -----------------------------------------------------------------------

    def _gpv_metrics(self) -> Optional[tuple[Optional[float], Optional[float], float]]:
        """Return (mom_growth, decline_60d, current_30d_avg), or None until 60 days exist.

        gpv_baseline_60d is the trailing 60-day mean per Causal Rules, so it includes
        the latest 30 days. That damps the decline: with prior-window average p and
        current average c, decline_60d = (p - c) / (p + c). A 20% decline therefore
        needs c about 33% below p, and 40% needs c about 57% below p. Retune the
        thresholds in causal_rules.yaml if Stressed or Delinquent are underpopulated.
        """
        history = self._latest_60_days()
        if history is None:
            return None
        prior = sum(history[:_WINDOW_DAYS]) / _WINDOW_DAYS
        current = sum(history[_WINDOW_DAYS:]) / _WINDOW_DAYS
        baseline_60d = (prior + current) / 2
        mom_growth = (current - prior) / prior if prior > 0 else None
        decline_60d = (baseline_60d - current) / baseline_60d if baseline_60d > 0 else None
        return mom_growth, decline_60d, current

    def _update_growth_counters(self, mom_growth: Optional[float]) -> None:
        if mom_growth is None:
            return
        if mom_growth >= _ST["growth_threshold"]:
            self.days_growth_high += 1
            self.days_growth_low = 0
        else:
            self.days_growth_low += 1
            self.days_growth_high = 0

    def _update_months_on_platform(self, sim_date: date) -> None:
        if self.onboarded_at is not None:
            self.months_on_platform = (sim_date - self.onboarded_at).days // 30
        else:
            self.months_on_platform = self.trading_days // 30

    def _gpv_recovered(self, current_30d_avg: float) -> bool:
        """True when the latest 30 days are within the recovery band of the 60-day mean."""
        latest_60 = self._latest_60_days()
        if latest_60 is None:
            return False
        baseline_60d = sum(latest_60) / _HISTORY_DAYS
        return current_30d_avg >= (1 - _ST["stressed_recovery_band"]) * baseline_60d

    def _candidate_transition(
        self,
        mom_growth: Optional[float],
        decline_60d: Optional[float],
        current_30d_avg: float,
    ) -> Optional[tuple[str, float]]:
        """Highest-priority transition whose condition holds, with its probability.

        Only that one condition is rolled. If several hold, lower-priority ones are not
        tried after a failed roll, which keeps one transition per cycle.
        """
        state = self.merchant_state
        month = _WINDOW_DAYS
        candidates: list[tuple[str, float]] = []

        if state == "new":
            if self.days_without_txn >= _ST["churn_no_txn_days"]:
                candidates.append(("churned", _PROB["new_to_churned"]))
            if mom_growth is not None:
                if self.months_on_platform >= 1 and mom_growth >= _ST["growth_threshold"]:
                    candidates.append(("growing", _PROB["new_to_growing"]))
                if self.months_on_platform >= 3 and abs(mom_growth) < _ST["stable_band"]:
                    candidates.append(("stable", _PROB["new_to_stable"]))

        elif state == "growing":
            if decline_60d is not None and decline_60d >= _ST["stressed_gpv_60d_decline"]:
                candidates.append(("stressed", _PROB["growing_to_stressed"]))
            if self.days_growth_low >= _ST["growing_to_stable_months"] * month:
                candidates.append(("stable", _PROB["growing_to_stable"]))

        elif state == "stable":
            if decline_60d is not None and decline_60d >= _ST["stressed_gpv_60d_decline"]:
                candidates.append(("stressed", _PROB["stable_to_stressed"]))
            if self.days_growth_high >= _ST["stable_to_growing_months"] * month:
                candidates.append(("growing", _PROB["stable_to_growing"]))

        elif state == "stressed":
            delinquent_p = 0.0
            if decline_60d is not None and decline_60d >= _ST["delinquent_gpv_60d_decline"]:
                delinquent_p = _PROB["stressed_to_delinquent_gpv"]
            if self.missed_repayment_flag:
                delinquent_p = max(delinquent_p, _PROB["stressed_to_delinquent_missed_repayment"])
            if delinquent_p > 0.0:
                candidates.append(("delinquent", delinquent_p))
            if self._gpv_recovered(current_30d_avg):
                candidates.append(("stable", _PROB["stressed_to_stable"]))

        elif state == "delinquent":
            if self.days_without_txn >= _ST["churn_no_txn_days"]:
                candidates.append(("churned", _PROB["delinquent_to_churned"]))
            # Not in Causal Rules, which gives Delinquent no way out except Churned. A
            # trading merchant whose GPV has recovered steps back to Stressed. Without
            # this Delinquent is a trap: a merchant that missed one repayment stays
            # Delinquent, so its collections keep failing and its loan never recovers.
            if self._gpv_recovered(current_30d_avg):
                candidates.append(("stressed", _PROB["delinquent_to_stressed"]))

        # Churned is terminal here: reactivation needs per-archetype probabilities
        # that are not in the config yet.
        for target in _ST["priority"]:
            for name, probability in candidates:
                if name == target:
                    return name, probability
        return None

    def evaluate_state(self, sim_date: date) -> Optional[str]:
        """Evaluate Chain 2 once for this merchant on sim_date.

        Returns the new state if a transition happened, otherwise None. Call after
        record_day() so the GPV windows include today.
        """
        self.days_in_state += 1
        self._update_months_on_platform(sim_date)

        # Gate: no evaluation at all before min_trading_days of history.
        if self.trading_days < _ST["min_trading_days"]:
            return None

        metrics = self._gpv_metrics()
        # Between day 30 and day 60 there is no prior window to compare against.
        if metrics is None:
            return None
        mom_growth, decline_60d, current_30d_avg = metrics
        self._update_growth_counters(mom_growth)

        candidate = self._candidate_transition(mom_growth, decline_60d, current_30d_avg)
        # The missed-repayment trigger is a single event; it is rolled once and consumed.
        self.missed_repayment_flag = False
        if candidate is None:
            return None
        target, probability = candidate
        if not _roll(probability):
            return None
        self._enter_state(target)
        return target

    def _enter_state(self, state: MerchantState) -> None:
        self.merchant_state = state
        self.days_in_state = 0
        # Month counters restart so the next transition needs a fresh run of months.
        self.days_growth_high = 0
        self.days_growth_low = 0

    def escalate_to_stressed(self) -> bool:
        """Chain 5 escalation: a Stable merchant that takes a chargeback soon after a refund
        spike becomes Stressed. Any other state is left alone."""
        if self.merchant_state != "stable":
            return False
        self._enter_state("stressed")
        return True

    # -----------------------------------------------------------------------
    # Chain 4: risk score
    # -----------------------------------------------------------------------

    def _clamp_risk(self, score: int) -> int:
        floor = _RISK["permanent_floor_written_off"] if self.is_written_off else 0
        return max(floor, min(100, score))

    def apply_risk_delta(self, delta: int, sim_date: date) -> None:
        """Apply a risk score delta. Positive deltas count as adverse events and
        restart the decay clock."""
        if delta > 0:
            self.last_adverse_event_date = sim_date
        self.risk_score = self._clamp_risk(self.risk_score + delta)

    def record_repayment_outcome(self, success: bool, sim_date: date) -> None:
        """Apply the Chain 4 repayment deltas, including the bonus for every
        third consecutive success."""
        if success:
            self.apply_risk_delta(risk_delta("repayment_successful"), sim_date)
            self.consecutive_successful_repayments += 1
            if self.consecutive_successful_repayments % 3 == 0:
                self.apply_risk_delta(risk_delta("three_consecutive_successful"), sim_date)
        else:
            self.apply_risk_delta(risk_delta("repayment_missed_nsf"), sim_date)
            self.consecutive_successful_repayments = 0
            # Only a miss while Stressed feeds the Stressed to Delinquent transition.
            if self.merchant_state == "stressed":
                self.missed_repayment_flag = True

    def write_off(self, sim_date: date) -> None:
        """Mark the merchant written off. The score jumps by the write-off delta and
        can never fall below the permanent floor afterwards."""
        self.is_written_off = True
        self.apply_risk_delta(risk_delta("write_off"), sim_date)

    def decay_risk(self, sim_date: date) -> None:
        """Passive decay, called daily but applied once per period with no adverse
        events: 1 point, or 3 once there have been 90 or more clean days."""
        decay = _RISK["decay"]
        anchors = [
            d for d in (self.last_adverse_event_date, self.last_risk_decay_date) if d is not None
        ]
        if anchors:
            anchor = max(anchors)
        elif self.onboarded_at is not None:
            anchor = self.onboarded_at
        else:
            return
        if (sim_date - anchor).days < decay["period_days"]:
            return
        clean_since = self.last_adverse_event_date or self.onboarded_at or anchor
        clean_days = (sim_date - clean_since).days
        points = (
            decay["sustained_points_per_period"]
            if clean_days >= decay["sustained_good_days"]
            else decay["points_per_period"]
        )
        self.risk_score = self._clamp_risk(self.risk_score - points)
        self.last_risk_decay_date = sim_date


# ---------------------------------------------------------------------------
# Loan
# ---------------------------------------------------------------------------

@dataclass
class Loan:
    # A loan is its application: loan_id equals application_id, which is what the
    # loan_repayments foreign key (migration 002) requires.
    loan_id: str = ""
    merchant_id: str = ""
    application_id: str = ""

    principal_minor: int = 0       # disbursed amount in cents
    term_months: int = 6
    total_cost_pct: float = 0.07   # NCA fee as fraction

    disbursed_at: Optional[date] = None
    outstanding_principal_minor: int = 0
    days_past_due: int = 0
    is_written_off: bool = False

    mandate_id: Optional[str] = None
    mandate_status: str = "PDNG"   # ACCP | RJCT | PDNG | CANC

    # Repayment bookkeeping, driven by repayments.py. outstanding_balance_minor is what is
    # still owed including the fee, the figure Event Schemas calls outstanding_before and
    # outstanding_after; outstanding_principal_minor is the principal part only.
    fee_minor: int = 0
    outstanding_balance_minor: int = 0
    baseline_gpv_30d_minor: float = 0.0   # average daily GPV at disbursement, for forced misses
    next_installment: int = 1
    next_due_date: Optional[date] = None
    unpaid_installments: list = field(default_factory=list)   # numbers, oldest first
    bucket: str = "current"                # current | 1-30 | 31-60 | 61-90 | 90+
    retrying_installment: Optional[int] = None
    retry_dates: list = field(default_factory=list)
    # True from the moment retries are exhausted until the next monthly cycle decides
    # whether the instalment is paid before it counts as 1-30 DPD at month end.
    late_recovery_due: bool = False

    def __post_init__(self) -> None:
        if not self.application_id:
            self.application_id = self.loan_id or str(uuid.uuid4())
        self.loan_id = self.application_id


# ---------------------------------------------------------------------------
# Checkpoint helpers: Phase 7 will implement full serialisation
# ---------------------------------------------------------------------------

def save_checkpoint(merchants: list[Merchant], path: str = "checkpoint.json") -> None:
    """Persist merchant states to disk. Phase 7 implementation."""
    raise NotImplementedError("Phase 7")


def load_checkpoint(path: str = "checkpoint.json") -> list[Merchant]:
    """Load merchant states from disk. Phase 7 implementation."""
    raise NotImplementedError("Phase 7")
