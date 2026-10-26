"""tests/test_checkpoint.py: saving and loading run state.

The point of a checkpoint is that a resumed run equals an uninterrupted one. The
generator draws from numpy's global RNG, so the checkpoint carries that state too.
"""
import json
from collections import deque
from datetime import date, datetime, timedelta

import numpy as np
import pytest

from simulator.common import SAST
from simulator.generator import simulate_day
from simulator.lending import simulate_lending_day
from simulator.risk import evaluate_risk
from simulator.state import (
    CHECKPOINT_VERSION,
    Checkpoint,
    CheckpointMismatch,
    Loan,
    Merchant,
    load_checkpoint,
    save_checkpoint,
)

START = date(2025, 1, 1)
FLAGS = {"enable_risk_events": True, "enable_loan_applications": True}


def populated_merchant() -> Merchant:
    """One merchant with every kind of state a checkpoint has to carry."""
    m = Merchant(
        merchant_id="m-1", archetype="restaurant", province="western_cape", is_metro=False,
        quality_percentile=0.31, onboarded_at=START, activation_date=START + timedelta(days=2),
        merchant_state="stressed", risk_score=44, loan_count=1, has_active_loan=True,
    )
    for i in range(100):
        m.record_day(START + timedelta(days=2 + i), 1_000 + i, 3)
    m.risk_history.extend([(5, 4, 0, 0), (6, 5, 1, 0)])
    m.last_risk_event = {"refund_spike": date(2025, 3, 1)}
    m.fraud_cluster_until = date(2025, 4, 2)
    m.last_adverse_event_date = date(2025, 3, 1)
    m.scheduled_followups = [{
        "kind": "refund", "due_date": date(2025, 4, 9), "transaction_id": "t-1",
        "channel": "card_present", "payment_method": "visa", "amount_minor": 12_345,
    }]
    m.scheduled_disbursements = [{
        "application_id": "a-2", "decided_at": datetime(2025, 4, 1, 10, 30, tzinfo=SAST),
        "due_date": date(2025, 4, 3), "amount_minor": 500_000, "term_months": 6,
        "total_cost_pct": 0.07,
    }]
    m.active_loan = Loan(
        loan_id="a-1", merchant_id="m-1", principal_minor=1_000_000, disbursed_at=date(2025, 2, 1),
        outstanding_principal_minor=700_000, next_due_date=date(2025, 4, 1),
        unpaid_installments=[2, 3], retry_dates=[date(2025, 4, 2), date(2025, 4, 5)],
        retrying_installment=3, bucket="1-30", days_past_due=12, late_recovery_due=True,
        mandate_id="mandate-1", mandate_status="ACCP",
    )
    return m


def run_days(merchants: list[Merchant], first_day: date, count: int) -> list:
    """The per-merchant daily loop; returns everything it emitted."""
    emitted = []
    for offset in range(count):
        day = first_day + timedelta(days=offset)
        for m in merchants:
            events = simulate_day(m, day)
            risk_rows = evaluate_risk(m, events, day)
            lending = simulate_lending_day(m, day)
            m.evaluate_state(day)
            m.decay_risk(day)
            emitted.append((m.merchant_id, day, events, risk_rows, lending))
    return emitted


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------

def test_a_populated_merchant_survives_a_round_trip(tmp_path):
    original = populated_merchant()
    path = str(tmp_path / "checkpoint.json")
    save_checkpoint([original], date(2025, 4, 1), FLAGS, path)
    (loaded,) = load_checkpoint(path).merchants
    assert loaded == original


def test_container_and_date_types_come_back_as_they_went_in(tmp_path):
    path = str(tmp_path / "checkpoint.json")
    save_checkpoint([populated_merchant()], date(2025, 4, 1), FLAGS, path)
    (m,) = load_checkpoint(path).merchants
    assert isinstance(m.daily_gpv_minor, deque) and m.daily_gpv_minor.maxlen == 90
    assert isinstance(m.risk_history, deque) and m.risk_history.maxlen == 90
    assert all(isinstance(entry, tuple) for entry in m.risk_history)
    assert isinstance(m.onboarded_at, date) and not isinstance(m.onboarded_at, datetime)
    decided = m.scheduled_disbursements[0]["decided_at"]
    assert isinstance(decided, datetime) and decided.utcoffset() == timedelta(hours=2)
    assert isinstance(m.active_loan, Loan)
    assert m.active_loan.retry_dates == [date(2025, 4, 2), date(2025, 4, 5)]


def test_the_checkpoint_is_plain_json_with_a_version(tmp_path):
    path = tmp_path / "checkpoint.json"
    save_checkpoint([populated_merchant()], date(2025, 4, 1), FLAGS, str(path))
    payload = json.loads(path.read_text())
    assert payload["version"] == CHECKPOINT_VERSION
    assert payload["last_day"] == {"__date__": "2025-04-01"}
    assert len(payload["merchants"]) == 1


def test_numpy_numbers_are_saved_as_plain_numbers(tmp_path):
    m = populated_merchant()
    m.risk_score = np.int64(44)
    m.quality_percentile = np.float64(0.31)
    path = str(tmp_path / "checkpoint.json")
    save_checkpoint([m], date(2025, 4, 1), FLAGS, path)
    (loaded,) = load_checkpoint(path).merchants
    assert loaded.risk_score == 44 and type(loaded.risk_score) is int
    assert loaded.quality_percentile == 0.31 and type(loaded.quality_percentile) is float


def test_day_flags_and_extra_come_back(tmp_path):
    path = str(tmp_path / "checkpoint.json")
    extra = {"onboarded": 120, "next_ramp_day": date(2025, 2, 1)}
    save_checkpoint([], date(2025, 4, 1), FLAGS, path, extra=extra)
    checkpoint = load_checkpoint(path)
    assert isinstance(checkpoint, Checkpoint)
    assert checkpoint.last_day == date(2025, 4, 1)
    assert checkpoint.flags == FLAGS
    assert checkpoint.extra == extra


def test_the_parent_directory_is_created(tmp_path):
    path = tmp_path / "runs" / "one" / "checkpoint.json"
    save_checkpoint([], date(2025, 4, 1), FLAGS, str(path))
    assert path.exists()


# ---------------------------------------------------------------------------
# RNG state
# ---------------------------------------------------------------------------

def test_restoring_the_rng_repeats_the_draws_that_followed_the_save(tmp_path):
    np.random.seed(9)
    np.random.normal()  # leaves a cached Gaussian, which is part of the state
    path = str(tmp_path / "checkpoint.json")
    save_checkpoint([], date(2025, 4, 1), FLAGS, path)
    expected = (np.random.random(4).tolist(), np.random.normal(), np.random.bytes(8))

    np.random.seed(123)  # a different stream, as in a fresh process
    load_checkpoint(path).restore_rng()
    assert (np.random.random(4).tolist(), np.random.normal(), np.random.bytes(8)) == expected


def test_loading_does_not_touch_the_rng_until_asked(tmp_path):
    np.random.seed(9)
    path = str(tmp_path / "checkpoint.json")
    save_checkpoint([], date(2025, 4, 1), FLAGS, path)
    np.random.seed(77)
    state_before = np.random.get_state()[1].copy()
    load_checkpoint(path)
    assert (np.random.get_state()[1] == state_before).all()


# ---------------------------------------------------------------------------
# Refusing a bad checkpoint
# ---------------------------------------------------------------------------

def test_a_resume_with_different_flags_is_refused(tmp_path):
    path = str(tmp_path / "checkpoint.json")
    save_checkpoint([], date(2025, 4, 1), FLAGS, path)
    different = {**FLAGS, "enable_loan_applications": False}
    with pytest.raises(CheckpointMismatch, match="flags"):
        load_checkpoint(path, expected_flags=different)
    assert load_checkpoint(path, expected_flags=FLAGS).flags == FLAGS


def test_a_checkpoint_from_another_version_is_refused(tmp_path):
    path = tmp_path / "checkpoint.json"
    save_checkpoint([], date(2025, 4, 1), FLAGS, str(path))
    payload = json.loads(path.read_text())
    payload["version"] = CHECKPOINT_VERSION + 1
    path.write_text(json.dumps(payload))
    with pytest.raises(CheckpointMismatch, match="version"):
        load_checkpoint(str(path))


def test_a_missing_checkpoint_raises_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_checkpoint(str(tmp_path / "absent.json"))


# ---------------------------------------------------------------------------
# Writing safely
# ---------------------------------------------------------------------------

def test_saving_again_replaces_the_file_and_leaves_no_temporary(tmp_path):
    path = tmp_path / "checkpoint.json"
    save_checkpoint([], date(2025, 4, 1), FLAGS, str(path))
    save_checkpoint([], date(2025, 4, 2), FLAGS, str(path))
    assert load_checkpoint(str(path)).last_day == date(2025, 4, 2)
    assert [p.name for p in tmp_path.iterdir()] == ["checkpoint.json"]


def test_a_failed_save_keeps_the_previous_checkpoint(tmp_path):
    path = tmp_path / "checkpoint.json"
    save_checkpoint([], date(2025, 4, 1), FLAGS, str(path))
    with pytest.raises(TypeError, match="object"):
        save_checkpoint([], date(2025, 4, 2), FLAGS, str(path), extra={"bad": object()})
    assert load_checkpoint(str(path)).last_day == date(2025, 4, 1)
    assert [p.name for p in tmp_path.iterdir()] == ["checkpoint.json"]


def test_dict_keys_must_be_strings(tmp_path):
    with pytest.raises(TypeError, match="keys"):
        save_checkpoint([], date(2025, 4, 1), FLAGS, str(tmp_path / "c.json"), extra={"x": {1: 2}})


# ---------------------------------------------------------------------------
# Resumed run equals uninterrupted run
# ---------------------------------------------------------------------------

def equal_after_resume(tmp_path, archetypes, first_days, second_days, seed=11):
    np.random.seed(seed)
    merchants = [
        Merchant(merchant_id=f"m-{i}", archetype=a, onboarded_at=START)
        for i, a in enumerate(archetypes)
    ]
    run_days(merchants, START, first_days)
    in_flight = {
        "loans": sum(1 for m in merchants if m.active_loan is not None),
        "followups": sum(len(m.scheduled_followups) for m in merchants),
    }
    path = str(tmp_path / "checkpoint.json")
    resume_day = START + timedelta(days=first_days)
    save_checkpoint(merchants, resume_day - timedelta(days=1), FLAGS, path)

    # Uninterrupted: carry on in the same process.
    uninterrupted = run_days(merchants, resume_day, second_days)

    # Resumed: a different RNG stream, as in a new process, then the checkpoint.
    np.random.seed(999)
    checkpoint = load_checkpoint(path, expected_flags=FLAGS)
    checkpoint.restore_rng()
    resumed_merchants = checkpoint.merchants
    resumed = run_days(resumed_merchants, resume_day, second_days)
    return uninterrupted, resumed, merchants, resumed_merchants, in_flight


def test_a_resumed_run_equals_an_uninterrupted_one(tmp_path):
    uninterrupted, resumed, merchants, resumed_merchants, in_flight = equal_after_resume(
        tmp_path, ["spaza_shop", "restaurant", "salon"], first_days=15, second_days=10,
    )
    assert in_flight["followups"] > 0
    assert sum(len(events) for _, _, events, _, _ in uninterrupted) > 100
    assert resumed == uninterrupted
    assert resumed_merchants == merchants


@pytest.mark.slow
def test_a_resumed_run_equals_an_uninterrupted_one_with_loans_in_flight(tmp_path):
    # Seed 1 leaves two disbursed loans running at day 150, so the checkpoint has to
    # carry live repayment state, and the second half goes on to collect instalments.
    archetypes = ["wholesale", "e_commerce", "salon", "restaurant", "salon_low_cost", "tutor"]
    uninterrupted, resumed, merchants, resumed_merchants, in_flight = equal_after_resume(
        tmp_path, archetypes, first_days=150, second_days=120, seed=1,
    )
    assert in_flight["loans"] > 0, "no loan was running at the checkpoint, so it proves little"
    repayments = sum(len(lending["loan_repayments"]) for _, _, _, _, lending in uninterrupted)
    assert repayments > 0
    assert resumed == uninterrupted
    assert resumed_merchants == merchants
