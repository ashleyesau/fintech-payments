"""tests/test_runner.py: the daily loop and the backfill run.

advance_day() tests need no database. The backfill tests write to the throwaway database
from conftest.py and skip when Postgres is not reachable.
"""
from collections import Counter
from datetime import date, timedelta

import numpy as np
import pytest

from simulator import db, lending, runner
from simulator.db import STREAMS
from simulator.onboarding import RAMP_DAYS, ramp_count, ramp_target
from simulator.runner import (
    RunError,
    RunSettings,
    advance_day,
    format_summary,
    run_backfill,
)
from simulator.state import Merchant, load_checkpoint

START = date(2025, 1, 1)


def settings_for(tmp_path, days=60, count=60, **kwargs) -> RunSettings:
    return RunSettings(
        start_date=START,
        end_date=START + timedelta(days=days),
        merchant_count=count,
        checkpoint_path=str(tmp_path / "checkpoint.json"),
        **kwargs,
    )


def trading_merchant(**kwargs) -> Merchant:
    return Merchant(archetype="spaza_shop", quality_percentile=0.3, onboarded_at=START, **kwargs)


# ---------------------------------------------------------------------------
# advance_day: who is simulated
# ---------------------------------------------------------------------------

def test_the_ramp_onboards_merchants_each_day_and_they_trade_only_after_activation(tmp_path):
    np.random.seed(2)
    run = settings_for(tmp_path, count=300)
    merchants: list[Merchant] = []
    joined = []
    for d in range(14):
        result = advance_day(merchants, START + timedelta(days=d), run)
        joined.append(result.onboarded)
        assert result.onboarded == ramp_count(d, 300)
    assert sum(joined) == ramp_target(13, 300) == len(merchants)
    for m in merchants:
        assert m.first_transaction_at is None or m.first_transaction_at >= m.activation_date
        if m.activation_date:
            assert m.activation_date > m.onboarded_at


def test_a_never_activator_generates_nothing_and_churns_on_day_thirty(tmp_path):
    np.random.seed(2)
    run = settings_for(tmp_path, count=300)
    merchants: list[Merchant] = []
    churned = 0
    for d in range(45):
        result = advance_day(merchants, START + timedelta(days=d), run)
        churned += result.churned
    never = [m for m in merchants if m.never_activates and m.onboarded_at <= START + timedelta(days=14)]
    assert never
    for m in never:
        assert m.first_transaction_at is None and m.trading_days == 0
        assert m.merchant_state == "churned"
        assert m.churned_at == m.onboarded_at + timedelta(days=30)
    assert churned == sum(1 for m in merchants if m.churned_at is not None)


def test_a_merchant_before_its_activation_date_is_not_simulated(monkeypatch, tmp_path):
    def explode(*args, **kwargs):
        raise AssertionError("simulated a merchant that cannot trade yet")

    monkeypatch.setattr(runner, "simulate_day", explode)
    waiting = trading_merchant(activation_date=START + timedelta(days=5))
    result = advance_day([waiting], START + timedelta(days=2), settings_for(tmp_path, count=0))
    assert result.merchant_days == []


def test_merchants_are_replaced_one_for_one_only_from_month_seven(tmp_path):
    np.random.seed(2)
    run = settings_for(tmp_path, days=400, count=1)
    old = Merchant(archetype="spaza_shop", onboarded_at=START, never_activates=True)

    during = [old]
    ramp_day = START + timedelta(days=100)
    result = advance_day(during, ramp_day, run)
    assert result.churned == 1
    assert result.onboarded == ramp_count(100, 1)  # the schedule only, no replacement

    after = [Merchant(archetype="spaza_shop", onboarded_at=START, never_activates=True)]
    late_day = START + timedelta(days=RAMP_DAYS + 20)
    result = advance_day(after, late_day, run)
    assert result.churned == 1 and result.onboarded == 1
    assert len(after) == 2 and after[1].onboarded_at == late_day


def test_a_merchant_that_churns_through_the_state_chain_is_counted_and_replaced(monkeypatch, tmp_path):
    run = settings_for(tmp_path, days=400, count=1)
    m = trading_merchant()
    monkeypatch.setattr(runner, "simulate_day", lambda *_: [])
    monkeypatch.setattr(runner, "evaluate_risk", lambda *_: [])
    monkeypatch.setattr(runner, "simulate_lending_day", lambda *_: {})
    monkeypatch.setattr(Merchant, "evaluate_state", lambda self, day: "churned")
    merchants = [m]
    result = advance_day(merchants, START + timedelta(days=RAMP_DAYS + 5), run)
    assert result.churned == 1 and result.onboarded == 1 and len(merchants) == 2


# ---------------------------------------------------------------------------
# advance_day: flags and snapshots
# ---------------------------------------------------------------------------

@pytest.fixture
def spies(monkeypatch):
    calls = Counter()

    def spy(name, value):
        def inner(*args, **kwargs):
            calls[name] += 1
            return value
        return inner

    monkeypatch.setattr(runner, "simulate_day", spy("day", []))
    monkeypatch.setattr(runner, "evaluate_risk", spy("risk", []))
    monkeypatch.setattr(runner, "simulate_lending_day", spy("lending", {}))
    return calls


def test_both_flags_on_call_risk_and_lending(spies, tmp_path):
    advance_day([trading_merchant()], START + timedelta(days=3), settings_for(tmp_path, count=0))
    assert spies == {"day": 1, "risk": 1, "lending": 1}


def test_risk_events_flag_off_skips_evaluate_risk_only(spies, tmp_path):
    run = settings_for(tmp_path, count=0, enable_risk_events=False)
    advance_day([trading_merchant()], START + timedelta(days=3), run)
    assert spies == {"day": 1, "lending": 1}


def test_loan_applications_flag_off_skips_the_lending_call_only(spies, tmp_path):
    run = settings_for(tmp_path, count=0, enable_loan_applications=False)
    advance_day([trading_merchant()], START + timedelta(days=3), run)
    assert spies == {"day": 1, "risk": 1}


def test_the_flags_are_part_of_the_run_settings(tmp_path):
    run = settings_for(tmp_path, enable_risk_events=False)
    assert run.flags == {"enable_risk_events": False, "enable_loan_applications": True}


def test_snapshots_include_new_merchants_and_drop_long_churned_ones(tmp_path):
    day = START + timedelta(days=120)
    long_gone = Merchant(archetype="spaza_shop", onboarded_at=START, never_activates=True,
                         merchant_state="churned", churned_at=day - timedelta(days=40))
    recent = Merchant(archetype="spaza_shop", onboarded_at=START, never_activates=True,
                      merchant_state="churned", churned_at=day - timedelta(days=10))
    merchants = [long_gone, recent]
    result = advance_day(merchants, day, settings_for(tmp_path, days=200, count=500))
    ids = {r["merchant_id"] for r in result.snapshots}
    assert long_gone.merchant_id not in ids and recent.merchant_id in ids
    joined = [m for m in merchants if m.onboarded_at == day]
    assert result.onboarded == len(joined) > 0
    assert all(m.merchant_id in ids for m in joined)


def test_a_day_with_nothing_to_write_has_no_merchant_days(tmp_path):
    merchants = [Merchant(archetype="spaza_shop", onboarded_at=START, never_activates=True)]
    result = advance_day(merchants, START + timedelta(days=2), settings_for(tmp_path, count=0))
    assert result.merchant_days == []


# ---------------------------------------------------------------------------
# The backfill run against Postgres
# ---------------------------------------------------------------------------

@pytest.fixture
def boosted(monkeypatch):
    """Ten times the application propensity, so a short run reaches lending."""
    for state in ("growing", "stable"):
        monkeypatch.setitem(lending._PROPENSITY, state, lending._PROPENSITY[state] * 10)


def table_contents(conn) -> dict[str, list]:
    out = {}
    for name, stream in STREAMS.items():
        order = ", ".join(stream.key)
        out[name] = [tuple(r) for r in conn.execute(f"SELECT * FROM {stream.table} ORDER BY {order}")]
    return out


def reset(conn) -> None:
    for stream in STREAMS.values():
        conn.execute(f"TRUNCATE {stream.table} CASCADE")
    conn.commit()


def test_a_backfill_writes_every_day_and_reconciles(conn, tmp_path):
    np.random.seed(0)
    run = settings_for(tmp_path, days=60, count=60)
    summary = run_backfill(run, conn)

    assert summary.days == 60 and summary.last_day == START + timedelta(days=59)
    assert summary.resumed is False
    written = {name: conn.execute(f"SELECT count(*) FROM {s.table}").fetchone()[0] for name, s in STREAMS.items()}
    for name, count in written.items():
        assert summary.rows_written[name] == count, name
    assert written["merchant_transactions"] > 0

    # Every onboarded merchant has a snapshot row from its onboarding day.
    ids = conn.execute("SELECT count(DISTINCT merchant_id) FROM merchants").fetchone()[0]
    assert ids == summary.merchants_onboarded == ramp_target(59, 60)
    first_seen = conn.execute(
        "SELECT (min(snapshot_at) AT TIME ZONE 'Africa/Johannesburg')::date, merchant_id, "
        "(min(onboarded_at) AT TIME ZONE 'Africa/Johannesburg')::date FROM merchants GROUP BY merchant_id"
    ).fetchall()
    assert all(seen == onboarded for seen, _, onboarded in first_seen)

    # The checkpoint is the last day's.
    checkpoint = load_checkpoint(run.checkpoint_path, expected_flags=run.flags)
    assert checkpoint.last_day == START + timedelta(days=59)
    assert checkpoint.extra == run.extra()
    assert len(checkpoint.merchants) == summary.merchants_onboarded


def test_merchants_that_never_activate_appear_in_the_dimension_but_never_transact(conn, tmp_path):
    np.random.seed(0)
    run_backfill(settings_for(tmp_path, days=62, count=100), conn)
    never_ids = {
        r[0] for r in conn.execute(
            "SELECT merchant_id FROM merchants GROUP BY merchant_id "
            "HAVING bool_or(merchant_state = 'churned') AND max(first_transaction_at) IS NULL"
        )
    }
    assert never_ids
    transacting = {r[0] for r in conn.execute("SELECT DISTINCT merchant_id FROM merchant_transactions")}
    assert not never_ids & transacting


def test_the_summary_reports_both_counts(conn, tmp_path):
    np.random.seed(0)
    summary = run_backfill(settings_for(tmp_path, days=60, count=60), conn)
    assert summary.merchants_ever_transacted < summary.merchants_onboarded
    text = format_summary(summary)
    assert "Merchants onboarded:" in text and "Merchants that ever transacted:" in text
    assert "merchant_transactions:" in text and "Checkpoint:" in text


LONG_RUN = {"days": 200, "count": 25}  # long enough to originate loans and collect instalments


@pytest.fixture(scope="module")
def uninterrupted(dsn, tmp_path_factory):
    """Every table after one uninterrupted LONG_RUN, written once for the resume tests."""
    with pytest.MonkeyPatch.context() as patch:
        for state in ("growing", "stable"):
            patch.setitem(lending._PROPENSITY, state, lending._PROPENSITY[state] * 10)
        connection = db.connect(dsn)
        reset(connection)
        path = tmp_path_factory.mktemp("uninterrupted") / "checkpoint.json"
        run = RunSettings(START, START + timedelta(days=LONG_RUN["days"]), LONG_RUN["count"],
                          checkpoint_path=str(path))
        run_backfill(run, connection)
        contents = table_contents(connection)
        reset(connection)
        connection.close()
    assert len(contents["loan_applications"]) > 0 and len(contents["loan_repayments"]) > 0
    return contents


@pytest.mark.slow
def test_a_resumed_backfill_matches_an_uninterrupted_one(conn, tmp_path, boosted, uninterrupted):
    run_backfill(settings_for(tmp_path, days=100, **{"count": LONG_RUN["count"]}), conn)
    np.random.seed(12345)  # a different stream, as in a new process
    summary = run_backfill(settings_for(tmp_path, **LONG_RUN), conn, resume=True)
    assert summary.resumed and summary.first_day == START + timedelta(days=100)
    assert table_contents(conn) == uninterrupted


@pytest.mark.slow
def test_replaying_a_day_that_was_committed_but_not_checkpointed_changes_nothing(
    conn, tmp_path, boosted, uninterrupted,
):
    count = LONG_RUN["count"]
    run_backfill(settings_for(tmp_path, days=120, count=count), conn)
    saved = (tmp_path / "checkpoint.json").read_bytes()  # the checkpoint after day 119
    run_backfill(settings_for(tmp_path, days=121, count=count), conn, resume=True)  # day 120 committed
    (tmp_path / "checkpoint.json").write_bytes(saved)  # the crash: day 120's checkpoint never landed
    run_backfill(settings_for(tmp_path, **LONG_RUN), conn, resume=True)  # replays day 120
    assert table_contents(conn) == uninterrupted


@pytest.mark.slow
def test_feature_flags_off_write_no_risk_or_loan_rows(conn, tmp_path, boosted):
    np.random.seed(1)
    run = settings_for(tmp_path, days=110, count=30, enable_risk_events=False, enable_loan_applications=False)
    run_backfill(run, conn)
    contents = table_contents(conn)
    assert contents["risk_events"] == [] and contents["loan_applications"] == []
    assert contents["loan_repayments"] == [] and len(contents["merchant_transactions"]) > 0


# ---------------------------------------------------------------------------
# Refusing to start or resume
# ---------------------------------------------------------------------------

def test_a_fresh_run_refuses_an_existing_checkpoint(conn, tmp_path):
    np.random.seed(0)
    run_backfill(settings_for(tmp_path, days=20, count=20), conn)
    reset(conn)
    with pytest.raises(RunError, match="--resume"):
        run_backfill(settings_for(tmp_path, days=20, count=20), conn)


def test_a_fresh_run_refuses_a_database_that_already_has_rows(conn, tmp_path):
    np.random.seed(0)
    run_backfill(settings_for(tmp_path, days=20, count=20), conn)
    other = RunSettings(START, START + timedelta(days=20), 20, checkpoint_path=str(tmp_path / "other.json"))
    with pytest.raises(RunError, match="empty database"):
        run_backfill(other, conn)
    assert not (tmp_path / "other.json").exists()


def test_a_resume_with_no_checkpoint_is_refused(conn, tmp_path):
    with pytest.raises(RunError, match="nothing to resume"):
        run_backfill(settings_for(tmp_path), conn, resume=True)


def test_a_resume_under_different_flags_is_refused(conn, tmp_path):
    np.random.seed(0)
    run_backfill(settings_for(tmp_path, days=20, count=20), conn)
    different = settings_for(tmp_path, days=40, count=20, enable_loan_applications=False)
    with pytest.raises(RunError, match="flags"):
        run_backfill(different, conn, resume=True)


@pytest.mark.parametrize("override", [{"count": 21}, {"start": START + timedelta(days=1)}])
def test_a_resume_for_a_different_run_is_refused(conn, tmp_path, override):
    np.random.seed(0)
    run_backfill(settings_for(tmp_path, days=20, count=20), conn)
    other = RunSettings(
        start_date=override.get("start", START), end_date=START + timedelta(days=40),
        merchant_count=override.get("count", 20), checkpoint_path=str(tmp_path / "checkpoint.json"),
    )
    with pytest.raises(RunError, match="checkpoint was written with"):
        run_backfill(other, conn, resume=True)


def test_a_resume_that_has_nothing_left_to_do_is_refused(conn, tmp_path):
    np.random.seed(0)
    run_backfill(settings_for(tmp_path, days=20, count=20), conn)
    with pytest.raises(RunError, match="Nothing to do"):
        run_backfill(settings_for(tmp_path, days=20, count=20), conn, resume=True)


def test_a_refused_run_leaves_the_database_alone(conn, tmp_path):
    np.random.seed(0)
    run_backfill(settings_for(tmp_path, days=20, count=20), conn)
    before = table_contents(conn)
    with pytest.raises(RunError):
        run_backfill(settings_for(tmp_path, days=40, count=20), conn)
    assert table_contents(conn) == before
