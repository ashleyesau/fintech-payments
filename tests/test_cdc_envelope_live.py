"""tests/test_cdc_envelope_live.py: M3 on a real seeded run.

A seeded backfill (with application propensity raised so it reaches lending, as the other long
runs do) is written into a database that already has a slot. Every message the slot holds is
assembled and turned into events, and the events are checked against the tables themselves.
event_date is checked against Postgres's own time zone arithmetic, so the oracle is not the
code under test. Needs wal_level=logical (decision D9); slow.
"""
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from cdc.columns import EXPECTED_COLUMNS
from cdc.envelope import build_events, to_line
from cdc.lsn import parse_lsn
from cdc.transactions import TransactionAssembler
from simulator import db, lending
from simulator.db import STREAMS
from simulator.runner import RunSettings, run_backfill
from tests.conftest import new_cdc_slot
from tests.helpers import LONG_RUN, START, peek_changes

NOW = datetime(2026, 10, 27, 9, 41, 7, 490000, tzinfo=timezone.utc)

# Postgres's own answer for each table's event date, in South African time.
SAST_DATE_SQL = {
    "merchants": "(snapshot_at AT TIME ZONE 'Africa/Johannesburg')::date::text",
    "merchant_transactions": "(created_at AT TIME ZONE 'Africa/Johannesburg')::date::text",
    "loan_applications": "(submission_ts AT TIME ZONE 'Africa/Johannesburg')::date::text",
    "loan_repayments": "COALESCE((payment_ts AT TIME ZONE 'Africa/Johannesburg')::date, due_date)::text",
    "risk_events": "(action_ts AT TIME ZONE 'Africa/Johannesburg')::date::text",
}


@pytest.fixture(scope="module")
def seeded(cdc_dsn, tmp_path_factory):
    """(events, transactions, database connection string) for a seeded run with loans, written
    once for the module into a database that already has a slot."""
    with new_cdc_slot(cdc_dsn) as slot:
        return seed_and_read(cdc_dsn, slot, tmp_path_factory.mktemp("envelope"))


def seed_and_read(cdc_dsn, cdc_slot, tmp_path):
    with pytest.MonkeyPatch.context() as patch:
        for state in ("growing", "stable"):
            patch.setitem(lending._PROPENSITY, state, lending._PROPENSITY[state] * 10)
        writer = db.connect(cdc_dsn)
        run = RunSettings(START, START + timedelta(days=LONG_RUN["days"]), LONG_RUN["count"],
                          checkpoint_path=str(tmp_path / "checkpoint.json"))
        run_backfill(run, writer)
        writer.close()
    assembler = TransactionAssembler()
    transactions = []
    with psycopg.connect(cdc_dsn, autocommit=True) as reader:
        for lsn, _, data in peek_changes(reader, cdc_slot):
            done = assembler.feed(parse_lsn(lsn), data)
            if done is not None:
                transactions.append(done)
    assert not assembler.in_transaction
    events = [e for t in transactions for e in build_events(t, "fintech_cdc", NOW)]
    return events, transactions, cdc_dsn


@pytest.mark.slow
def test_every_row_of_a_seeded_run_converts_to_an_event(seeded):
    events, _, dsn = seeded
    with psycopg.connect(dsn) as conn:
        for name, stream in STREAMS.items():
            rows = conn.execute(f"SELECT count(1) FROM {stream.table}").fetchone()[0]
            inserted = [e for e in events if e.source.table == name and e.op == "c"]
            assert len(inserted) == rows > 0, name
            assert all(tuple(e.after) == EXPECTED_COLUMNS[name] for e in inserted)
        disbursed = conn.execute("SELECT count(1) FROM loan_applications WHERE disbursed_at IS NOT NULL").fetchone()[0]
    updates = [e for e in events if e.op == "u"]
    assert len(updates) == disbursed > 0
    assert {e.source.table for e in updates} == {"loan_applications"}
    for event in events:
        assert to_line(event).endswith(b"\n")


# The key of a row as the envelope shows it: merchants' second key column is a timestamp.
KEY_SQL = {
    "merchants": "merchant_id, to_char(snapshot_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')",
    "merchant_transactions": "transaction_id",
    "loan_applications": "application_id",
    "loan_repayments": "repayment_id",
    "risk_events": "alert_id",
}


@pytest.mark.slow
def test_event_date_agrees_with_postgres_for_every_row(seeded):
    events, _, dsn = seeded
    with psycopg.connect(dsn) as conn:
        for name, stream in STREAMS.items():
            width = len(stream.key)
            oracle = {tuple(row[:width]): row[width] for row in conn.execute(
                f"SELECT {KEY_SQL[name]}, {SAST_DATE_SQL[name]} FROM {stream.table}")}
            got = {tuple(e.after[k] for k in stream.key): e.event_date
                   for e in events if e.source.table == name and e.op == "c"}
            assert len(oracle) > 0 and got == oracle, name


@pytest.mark.slow
def test_a_disbursement_update_is_in_the_same_partition_as_its_insert(seeded):
    events, _, _ = seeded
    inserts = {e.after["application_id"]: e for e in events if e.source.table == "loan_applications" and e.op == "c"}
    updates = [e for e in events if e.op == "u"]
    assert updates
    for update in updates:
        insert = inserts[update.after["application_id"]]
        assert (update.event_ts, update.event_date) == (insert.event_ts, insert.event_date)
        assert update.before["disbursed_at"] is None and update.after["disbursed_at"] is not None
        assert update.source.commit_lsn > insert.source.commit_lsn
        assert update.flags == []


@pytest.mark.slow
def test_unpaid_repayments_get_the_fallback_and_paid_ones_do_not(seeded):
    events, _, dsn = seeded
    repayments = [e for e in events if e.source.table == "loan_repayments"]
    fallback = [e for e in repayments if "event_time_fallback" in e.flags]
    with psycopg.connect(dsn) as conn:
        unpaid = conn.execute("SELECT count(1) FROM loan_repayments WHERE payment_ts IS NULL").fetchone()[0]
    assert len(fallback) == unpaid
    assert all(e.after["payment_ts"] is None and e.event_date == e.after["due_date"] for e in fallback)
    assert all(e.after["payment_ts"] is not None for e in repayments if e not in fallback)


@pytest.mark.slow
def test_order_dedup_key_and_transaction_completeness_hold_for_the_whole_run(seeded):
    events, transactions, _ = seeded
    keys = [(e.source.commit_lsn, e.transaction.position) for e in events]
    assert len(keys) == len(set(keys)) and keys == sorted(keys), "commit order, unique"
    commits = [t.commit_lsn for t in transactions]
    assert commits == sorted(commits) and len(set(commits)) == len(commits)
    ends = [t.end_lsn for t in transactions]
    assert ends == sorted(ends) and all(t.end_lsn > t.commit_lsn for t in transactions)
    seen = {}
    for e in events:
        seen.setdefault(e.source.commit_lsn, []).append(e.transaction)
    for refs in seen.values():
        assert sorted(r.position for r in refs) == list(range(refs[0].size)) and {r.size for r in refs} == {len(refs)}
    for table in EXPECTED_COLUMNS:
        lsns = [(e.source.commit_lsn, e.transaction.position) for e in events if e.source.table == table]
        assert lsns == sorted(lsns), f"{table}: LSN order"
