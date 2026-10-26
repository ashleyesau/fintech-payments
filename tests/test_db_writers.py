"""tests/test_db_writers.py: Phase 6 writers against a real Postgres.

Each test run gets its own throwaway database, created from the migrations, so nothing
touches the fintech database. Skips when Postgres is not reachable.
"""
import os
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import psycopg
import pytest

from simulator import db, lending
from simulator.common import SAST, end_of_day
from simulator.config import settings
from simulator.db import (
    STREAMS,
    BackfillWriter,
    RowValidationError,
    StreamingWriter,
    disburse_loan,
    insert_statement,
    snapshot_rows,
    validated,
)
from simulator.generator import simulate_day
from simulator.lending import simulate_lending_day
from simulator.risk import evaluate_risk
from simulator.state import Merchant

MIGRATIONS = sorted((Path(__file__).parent.parent / "migrations").glob("*.sql"))
START = date(2025, 1, 1)


def _dsn(dbname: str) -> str:
    return (
        f"host={settings.postgres_host} port={settings.postgres_port} dbname={dbname} "
        f"user={settings.postgres_user} password={settings.postgres_password}"
    )


@pytest.fixture(scope="module")
def dsn():
    name = f"fintech_test_{os.getpid()}"
    try:
        admin = psycopg.connect(_dsn(settings.postgres_db), autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable: {exc}")
    admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    admin.execute(f'CREATE DATABASE "{name}"')
    try:
        with psycopg.connect(_dsn(name), autocommit=True) as conn:
            for path in MIGRATIONS:
                conn.execute(path.read_text())
        yield _dsn(name)
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()


@pytest.fixture
def conn(dsn):
    connection = db.connect(dsn)
    for table in ("loan_repayments", "risk_events", "loan_applications", "merchant_transactions", "merchants"):
        connection.execute(f"TRUNCATE {table} CASCADE")
    connection.commit()
    yield connection
    connection.rollback()
    connection.close()


def count(conn, table: str) -> int:
    return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


# ---------------------------------------------------------------------------
# A small, real simulation to write
# ---------------------------------------------------------------------------

def simulate(days=260, seed=3, boost=10.0):
    """Generator output for a handful of low-volume merchants, grouped by day."""
    np.random.seed(seed)
    original = {s: lending._PROPENSITY[s] for s in ("growing", "stable")}
    for state in original:
        lending._PROPENSITY[state] = original[state] * boost
    try:
        merchants = [
            Merchant(
                archetype=a, quality_percentile=float(np.random.beta(1.5, 4.0)),
                onboarded_at=START, province="gauteng",
            )
            for a in ["wholesale", "tutor", "salon", "e_commerce"] * 2
        ]
        out = []
        for offset in range(days):
            day = START + timedelta(days=offset)
            per_merchant = []
            for m in merchants:
                events = simulate_day(m, day)
                risk_rows = evaluate_risk(m, events, day)
                lend = simulate_lending_day(m, day)
                m.evaluate_state(day)
                m.decay_risk(day)
                per_merchant.append((events, risk_rows, lend))
            out.append((day, per_merchant, snapshot_rows(merchants, day)))
        return merchants, out
    finally:
        lending._PROPENSITY.update(original)


@pytest.fixture(scope="module")
def run():
    return simulate()


def totals(run_output):
    merchants, days = run_output
    t = Counter()
    for _, per_merchant, snapshots in days:
        t["merchants"] += len(snapshots)
        for events, risk_rows, lend in per_merchant:
            t["merchant_transactions"] += len(events)
            t["risk_events"] += len(risk_rows)
            t["loan_applications"] += len(lend["loan_applications"])
            t["updates"] += len(lend["loan_application_updates"])
            t["loan_repayments"] += len(lend["loan_repayments"])
    return t


def write_all(writer, run_output):
    _, days = run_output
    for _, per_merchant, snapshots in days:
        for events, risk_rows, lend in per_merchant:
            writer.write_merchant_day(events, risk_rows, lend)
        writer.write_snapshots(snapshots)
        writer.end_of_day()
    writer.close()


# ---------------------------------------------------------------------------
# Schema agreement
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("stream", sorted(STREAMS))
def test_model_fields_are_exactly_the_table_columns(conn, stream):
    table = STREAMS[stream].table
    columns = {
        r[0] for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s",
            (table,),
        )
    }
    assert set(STREAMS[stream].model.model_fields) == columns


def test_insert_statement_casts_the_array_column():
    sql = insert_statement("loan_applications")
    assert "%(decline_reasons)s::text[]" in sql
    assert "ON CONFLICT" not in sql
    assert "ON CONFLICT (application_id) DO NOTHING" in insert_statement("loan_applications", True)
    assert "ON CONFLICT (merchant_id, snapshot_at) DO NOTHING" in insert_statement("merchants", True)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def test_an_invalid_row_is_rejected_before_anything_is_written(conn, run):
    _, days = run
    events = next(e for _, pm, _ in days for e, _, _ in pm if len(e) >= 2)
    bad = dict(events[0], channel="cheque")
    writer = BackfillWriter(conn)
    with pytest.raises(RowValidationError, match=r"merchant_transactions row \(transaction_id='.*'\) failed validation: channel"):
        writer.add("merchant_transactions", [events[1], bad])
    assert writer.buffered() == 0      # the valid row before the bad one was not kept either
    writer.flush()
    assert count(conn, "merchant_transactions") == 0


def test_validation_applies_to_the_streaming_writer_too(conn, run):
    _, days = run
    event = next(e for _, pm, _ in days for evs, _, _ in pm for e in evs)
    with pytest.raises(RowValidationError):
        StreamingWriter(conn).write("merchant_transactions", [dict(event, amount_minor=0)])
    assert count(conn, "merchant_transactions") == 0


def test_validated_dumps_the_model_values():
    row = {"alert_id": "a", "entity_type": "merchant", "entity_id": "m", "merchant_id": "m",
           "rule_id": "r", "rule_type": "refund_spike", "risk_score": 20, "action": "allow",
           "action_ts": datetime(2025, 1, 1, tzinfo=SAST)}
    assert validated("risk_events", row)["risk_score_delta"] == 0   # default filled in


@pytest.mark.parametrize("bad", [
    {"application_id": "", "disbursed_at": datetime(2025, 1, 1, tzinfo=SAST), "disbursement_amount_minor": 1},
    {"application_id": "a", "disbursed_at": datetime(2025, 1, 1), "disbursement_amount_minor": 1},
    {"application_id": "a", "disbursed_at": datetime(2025, 1, 1, tzinfo=SAST), "disbursement_amount_minor": -5},
    {"application_id": "a", "disbursed_at": "2025-01-01", "disbursement_amount_minor": 1},
])
def test_invalid_disbursement_updates_are_rejected(bad):
    with pytest.raises(ValueError, match="failed validation"):
        db.validated_update(bad)


# ---------------------------------------------------------------------------
# Backfill writer
# ---------------------------------------------------------------------------

def test_backfill_writes_every_row_of_every_stream(conn, run):
    expected = totals(run)
    assert expected["loan_repayments"] > 20 and expected["updates"] > 3 and expected["risk_events"] >= 0
    writer = BackfillWriter(conn, batch_size=5000)
    write_all(writer, run)
    for stream, n in expected.items():
        if stream != "updates":
            assert count(conn, STREAMS[stream].table) == n, stream
    assert writer.written["loan_application_updates"] == expected["updates"]
    assert count(conn, "loan_applications") == expected["loan_applications"]


def test_backfill_results_reconcile_with_the_generator(conn, run):
    merchants, days = run
    write_all(BackfillWriter(conn), run)
    generated_gpv = sum(
        e["amount_minor"] for _, pm, _ in days for events, _, _ in pm for e in events
        if e["event_type"] == "payment" and e["status"] == "completed"
    )
    stored_gpv = conn.execute(
        "SELECT coalesce(sum(amount_minor), 0) FROM merchant_transactions WHERE event_type='payment' AND status='completed'"
    ).fetchone()[0]
    assert stored_gpv == generated_gpv > 0
    disbursed = conn.execute("SELECT count(*) FROM loan_applications WHERE disbursed_at IS NOT NULL").fetchone()[0]
    assert disbursed == totals(run)["updates"]
    assert conn.execute(
        "SELECT count(*) FROM loan_repayments r LEFT JOIN loan_applications a ON a.application_id = r.loan_id WHERE a.application_id IS NULL"
    ).fetchone()[0] == 0
    # Every disbursed loan's amount in Postgres equals the approved amount.
    assert conn.execute(
        "SELECT count(*) FROM loan_applications WHERE disbursed_at IS NOT NULL AND disbursement_amount_minor <> approved_amount"
    ).fetchone()[0] == 0
    assert merchants  # the run included merchants


def test_loan_application_columns_round_trip(conn, run):
    write_all(BackfillWriter(conn), run)
    row = conn.execute(
        "SELECT decline_reasons, submission_ts, requested_term_months, total_cost_percent FROM loan_applications "
        "WHERE decision = 'declined' LIMIT 1"
    ).fetchone()
    if row:  # a declined application exists in this run
        assert isinstance(row[0], list) and row[0]
    approved = conn.execute(
        "SELECT decline_reasons, submission_ts, total_cost_percent FROM loan_applications WHERE decision = 'approved' LIMIT 1"
    ).fetchone()
    assert approved[0] == [] and approved[1].tzinfo is not None
    assert approved[2] is not None


def test_timestamps_are_stored_with_their_instant(conn, run):
    write_all(BackfillWriter(conn), run)
    created, completed = conn.execute(
        "SELECT created_at, completed_at FROM merchant_transactions WHERE status = 'completed' LIMIT 1"
    ).fetchone()
    assert created < completed
    assert created.astimezone(SAST).date() >= START


def test_rows_in_the_wrong_order_still_flush_in_dependency_order(conn, run):
    _, days = run
    applications, repayments = [], []
    for _, pm, _ in days:
        for _, _, lend in pm:
            applications += lend["loan_applications"]
            repayments += lend["loan_repayments"]
    writer = BackfillWriter(conn, batch_size=10**6)
    writer.add("loan_repayments", repayments)     # added first, written last
    writer.add("loan_applications", applications)
    writer.flush()
    assert count(conn, "loan_repayments") == len(repayments) > 0


def test_the_buffer_flushes_itself_at_batch_size(conn, run):
    _, days = run
    events = [e for _, pm, _ in days for evs, _, _ in pm for e in evs][:300]
    writer = BackfillWriter(conn, batch_size=100)
    writer.add("merchant_transactions", events[:99])
    assert count(conn, "merchant_transactions") == 0 and writer.buffered() == 99
    writer.add("merchant_transactions", events[99:101])
    assert count(conn, "merchant_transactions") == 101 and writer.buffered() == 0


def test_a_failed_flush_writes_nothing(conn, run):
    _, days = run
    events = [e for _, pm, _ in days for evs, _, _ in pm for e in evs][:50]
    writer = BackfillWriter(conn, batch_size=10**6)
    writer.add("merchant_transactions", events)
    writer.add("merchant_transactions", [events[0]])      # same primary key twice
    with pytest.raises(psycopg.errors.UniqueViolation):
        writer.flush()
    conn.rollback()
    assert count(conn, "merchant_transactions") == 0


def test_an_update_for_a_missing_application_fails_the_flush(conn):
    writer = BackfillWriter(conn)
    writer.add_updates([{"application_id": "nope", "disbursed_at": datetime(2025, 1, 1, tzinfo=SAST),
                         "disbursement_amount_minor": 100}])
    with pytest.raises(LookupError):
        writer.flush()
    conn.rollback()


def test_skip_duplicates_makes_a_rerun_harmless(conn, run):
    expected = totals(run)
    write_all(BackfillWriter(conn, skip_duplicates=True), run)
    write_all(BackfillWriter(conn, skip_duplicates=True), run)
    assert count(conn, "merchant_transactions") == expected["merchant_transactions"]
    assert count(conn, "merchants") == expected["merchants"]


def test_a_rerun_without_skip_duplicates_fails(conn, run):
    write_all(BackfillWriter(conn), run)
    with pytest.raises(psycopg.errors.UniqueViolation):
        write_all(BackfillWriter(conn), run)
    conn.rollback()


# ---------------------------------------------------------------------------
# Streaming writer
# ---------------------------------------------------------------------------

def test_streaming_writes_the_same_rows_as_backfill(conn, run):
    expected = totals(run)
    write_all(StreamingWriter(conn), run)
    for stream, n in expected.items():
        if stream != "updates":
            assert count(conn, STREAMS[stream].table) == n, stream


def risk_row_for(txn_id: str, merchant_id: str, alert_id: str, entity_type="transaction") -> dict:
    return {"alert_id": alert_id, "entity_type": entity_type, "entity_id": txn_id, "merchant_id": merchant_id,
            "rule_id": "rule-large-txn", "rule_type": "large_txn_flag", "risk_score": 15,
            "risk_score_delta": 5, "action": "flag_review", "action_by": "system_auto",
            "action_ts": datetime(2025, 1, 1, 10, tzinfo=SAST)}


def test_a_transaction_and_its_risk_event_commit_together(conn, run):
    _, days = run
    txn = next(e for _, pm, _ in days for evs, _, _ in pm for e in evs if e["event_type"] == "payment")
    StreamingWriter(conn).write_events([txn], [risk_row_for(txn["transaction_id"], txn["merchant_id"], "alert-1")])
    assert count(conn, "merchant_transactions") == 1 and count(conn, "risk_events") == 1


def test_if_the_risk_event_fails_the_transaction_is_rolled_back(conn, run):
    _, days = run
    txn = next(e for _, pm, _ in days for evs, _, _ in pm for e in evs if e["event_type"] == "payment")
    twice = [risk_row_for(txn["transaction_id"], txn["merchant_id"], "alert-dup")] * 2   # second hits the PK
    with pytest.raises(psycopg.errors.UniqueViolation):
        StreamingWriter(conn).write_events([txn], twice)
    conn.rollback()
    assert count(conn, "merchant_transactions") == 0 and count(conn, "risk_events") == 0


def test_a_failed_event_leaves_earlier_events_committed(conn, run):
    _, days = run
    events = [e for _, pm, _ in days for evs, _, _ in pm for e in evs if e["event_type"] == "payment"][:3]
    bad = dict(events[2], channel="cheque")
    with pytest.raises(RowValidationError):
        StreamingWriter(conn).write_events(events[:2] + [bad], [])
    assert count(conn, "merchant_transactions") == 2


def test_merchant_level_risk_events_commit_on_their_own(conn, run):
    _, days = run
    merchant_id = days[0][2][0]["merchant_id"]
    row = risk_row_for(merchant_id, merchant_id, "alert-m", entity_type="merchant")
    StreamingWriter(conn).write_events([], [row])
    assert count(conn, "risk_events") == 1


def test_streaming_disbursement_updates_the_existing_row(conn, run):
    _, days = run
    applications = [a for _, pm, _ in days for _, _, lend in pm for a in lend["loan_applications"]]
    updates = [u for _, pm, _ in days for _, _, lend in pm for u in lend["loan_application_updates"]]
    assert updates
    writer = StreamingWriter(conn)
    writer.write("loan_applications", applications)
    for update in updates:
        disburse_loan(conn, update)
    done = conn.execute("SELECT count(*) FROM loan_applications WHERE disbursed_at IS NOT NULL").fetchone()[0]
    assert done == len(updates)
    assert count(conn, "loan_applications") == len(applications)   # an UPDATE, not new rows


def test_disbursing_an_unknown_application_raises(conn):
    with pytest.raises(LookupError):
        disburse_loan(conn, {"application_id": "ghost", "disbursed_at": datetime(2025, 1, 1, tzinfo=SAST),
                             "disbursement_amount_minor": 1})
    conn.rollback()


# ---------------------------------------------------------------------------
# Daily snapshots
# ---------------------------------------------------------------------------

def test_snapshot_rows_are_end_of_day_and_skip_unonboarded_merchants():
    early = Merchant(archetype="spaza_shop", onboarded_at=START)
    late = Merchant(archetype="spaza_shop", onboarded_at=START + timedelta(days=5))
    rows = snapshot_rows([early, late], START + timedelta(days=2))
    assert [r["merchant_id"] for r in rows] == [early.merchant_id]
    assert rows[0]["snapshot_at"] == end_of_day(START + timedelta(days=2))
    assert end_of_day(START).hour == 23 and end_of_day(START).minute == 59
    assert len(snapshot_rows([early, late], START + timedelta(days=5))) == 2


def test_one_snapshot_per_merchant_per_day_is_stored(conn, run):
    merchants, days = run
    write_all(BackfillWriter(conn), run)
    assert count(conn, "merchants") == len(days) * len(merchants)
    per_merchant = conn.execute("SELECT count(DISTINCT snapshot_at) FROM merchants WHERE merchant_id = %s",
                                (merchants[0].merchant_id,)).fetchone()[0]
    assert per_merchant == len(days)


def test_a_second_snapshot_for_the_same_day_is_rejected(conn, run):
    merchants, days = run
    rows = days[0][2]
    writer = BackfillWriter(conn)
    writer.write_snapshots(rows)
    writer.flush()
    writer.write_snapshots(rows[:1])
    with pytest.raises(psycopg.errors.UniqueViolation):
        writer.flush()
    conn.rollback()


def test_snapshots_track_state_and_loan_flags(conn, run):
    merchants, days = run
    write_all(BackfillWriter(conn), run)
    last = conn.execute(
        "SELECT merchant_id, merchant_state, loan_count, has_active_loan FROM merchants "
        "WHERE snapshot_at = %s ORDER BY merchant_id", (end_of_day(days[-1][0]),)
    ).fetchall()
    expected = sorted((m.merchant_id, m.merchant_state, m.loan_count, m.has_active_loan) for m in merchants)
    assert [tuple(r) for r in last] == expected
