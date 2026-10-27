"""tests/test_cdc_failures.py: M5, the failures the contract names, end to end on a real slot (CDC Plan section 11).

The consumer is run through the command line's own `execute()` with the real reader and the in-memory
sink, so what is checked is what an operator would see: the exit code, the one line on stderr, what had
landed, and where the slot stood. A planted DELETE and TRUNCATE, a column added, renamed or retyped, and
a replayed disbursement (the same UPDATE executed twice, which is what a resumed generator does).
"""
import contextlib
import io
import os

import psycopg
import pytest

from cdc.config import CdcSettings
from cdc.dedup import dedup, events_in, incomplete_transactions, key
from cdc.main import execute
from cdc.reader import Reader
from simulator import db
from tests.fakes import MemorySink
from tests.helpers import table_contents
from tests.roundtrip import apply_events, blank_database, in_commit_order, landed_events
from tests.test_cdc_consumer_live import expected_keys, write_chain5
from tests.test_cdc_foundation import application, disbursement
from tests.test_cdc_main import live_settings
from tests.test_cdc_reader import assemble, slot_position

PUBLICATION = "fintech_cdc"


@pytest.fixture(autouse=True)
def default_cdc_settings(monkeypatch):
    for name in [n for n in os.environ if n.startswith("CDC_")]:
        monkeypatch.delenv(name)


def run(dsn, slot, sink=None, **settings):
    """The consumer, once, as the command line runs it. Returns (exit code, stdout, stderr, sink)."""
    sink = sink if sink is not None else MemorySink()
    out, err = io.StringIO(), io.StringIO()
    code = execute(live_settings(dsn, slot, **settings), False,
                   reader_factory=lambda s: Reader(s.dsn, s.slot_name, s.publication),
                   sink_factory=lambda s: sink, out=out, err=err, handle_signals=False)
    return code, out.getvalue(), err.getvalue(), sink


def waiting(dsn, slot):
    with Reader(dsn, slot, PUBLICATION) as peek:
        return assemble(peek.fetch(10 ** 9))


def raw_waiting(dsn, slot):
    with Reader(dsn, slot, PUBLICATION) as peek:
        return peek.fetch(10 ** 9)


def keys_landed(sink):
    return {key(e) for data in sink.objects.values() for e in events_in(data)}


def execute_sql(dsn, *statements):
    with psycopg.connect(dsn, autocommit=True) as conn:
        for statement in statements:
            conn.execute(statement)


# ---------------------------------------------------------------------------
# a DELETE and a TRUNCATE (D5: fail loudly, stop without confirming)
# ---------------------------------------------------------------------------

def plant_after_three_good_transactions(dsn, slot, statement):
    write_chain5(dsn, [1, 2, 3])
    good = waiting(dsn, slot)                         # the three transactions before the planted change
    execute_sql(dsn, statement)
    write_chain5(dsn, [4], first=3)                   # a good transaction behind it
    return good


@pytest.mark.parametrize("statement, said", [
    ("DELETE FROM risk_events WHERE alert_id = 'a-1'", "delete on risk_events"),
    ("TRUNCATE merchants", "truncate of merchants"),
])
def test_a_planted_delete_or_truncate_stops_the_consumer_at_that_transaction_and_it_stays_in_the_slot(cdc_dsn, cdc_slot, statement, said):
    good = plant_after_three_good_transactions(cdc_dsn, cdc_slot, statement)
    code, out, err, sink = run(cdc_dsn, cdc_slot, batch_size=1)          # one transaction to a batch
    assert code == 2
    assert err.startswith("error: ") and said in err and len(err.strip().splitlines()) == 1
    assert "done:" not in out
    assert keys_landed(sink) == expected_keys(good)                     # the three before it landed
    assert slot_position(cdc_dsn, cdc_slot) == good[-1].end_lsn         # and were confirmed, but nothing past them
    kinds = [data[:1] for _, data in raw_waiting(cdc_dsn, cdc_slot)]
    assert (b"D" in kinds) or (b"T" in kinds)                           # the planted change is still waiting
    assert kinds.count(b"B") == 2                                       # with the good transaction behind it


@pytest.mark.parametrize("statement", ["DELETE FROM risk_events WHERE alert_id = 'a-1'", "TRUNCATE merchants"])
def test_with_one_big_batch_the_planted_change_refuses_the_whole_batch_and_nothing_lands_or_is_confirmed(cdc_dsn, cdc_slot, statement):
    plant_after_three_good_transactions(cdc_dsn, cdc_slot, statement)
    start = slot_position(cdc_dsn, cdc_slot)
    code, out, err, sink = run(cdc_dsn, cdc_slot)
    assert code == 2 and sink.attempts == 0 and slot_position(cdc_dsn, cdc_slot) == start


# ---------------------------------------------------------------------------
# schema drift (D6): a column added, renamed or retyped
# ---------------------------------------------------------------------------

@contextlib.contextmanager
def drift_database(dsn):
    """A database of its own (a schema change must not touch the shared test database) with a slot,
    created before any data."""
    with blank_database(dsn) as drift_dsn:
        slot = f"cdc_drift_{os.getpid()}"
        execute_sql(drift_dsn, f"SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')")
        yield drift_dsn, slot


def merchant(dsn, name):
    execute_sql(dsn, "INSERT INTO merchants (merchant_id, archetype, province, onboarded_at, snapshot_at) "
                     f"VALUES ('{name}', 'tutor', 'gauteng', '2025-01-01 00:00+00', '2025-03-01 21:59+00')")


@pytest.mark.parametrize("change, said", [
    ("ALTER TABLE merchants ADD COLUMN extra TEXT", "in the database but not the model: extra"),
    ("ALTER TABLE merchants RENAME COLUMN loan_count TO loans",
     "in the database but not the model: loans; in the model but not the database: loan_count"),
    ("ALTER TABLE merchants ALTER COLUMN archetype TYPE VARCHAR(40)", "archetype"),
])
def test_a_schema_change_stops_the_consumer_naming_the_column_and_the_change_before_it_lands(cdc_dsn, change, said):
    with drift_database(cdc_dsn) as (dsn, slot):
        merchant(dsn, "m-before")
        before = waiting(dsn, slot)
        execute_sql(dsn, change)
        merchant(dsn, "m-after")                                          # its Relation message carries the new shape
        code, out, err, sink = run(dsn, slot, batch_size=1)
        assert code == 2
        assert err.startswith("error: ") and said in err and len(err.strip().splitlines()) == 1
        assert keys_landed(sink) == expected_keys(before)                  # what was written before the change landed
        assert slot_position(dsn, slot) == before[-1].end_lsn              # and nothing after it was confirmed


def test_a_schema_change_in_one_big_batch_lands_nothing(cdc_dsn):
    with drift_database(cdc_dsn) as (dsn, slot):
        merchant(dsn, "m-before")
        execute_sql(dsn, "ALTER TABLE merchants ADD COLUMN extra TEXT")
        merchant(dsn, "m-after")
        start = slot_position(dsn, slot)
        code, out, err, sink = run(dsn, slot)
        assert code == 2 and "extra" in err and sink.attempts == 0 and slot_position(dsn, slot) == start


# ---------------------------------------------------------------------------
# a replayed disbursement (a resumed generator re-runs the same UPDATE)
# ---------------------------------------------------------------------------

def test_a_replayed_disbursement_lands_as_a_second_update_flagged_as_a_no_op_and_replays_to_the_same_table(cdc_dsn, cdc_slot):
    writer = db.connect(cdc_dsn)
    batch = db.BackfillWriter(writer)
    batch.add("loan_applications", [application("app-1")])
    batch.end_of_day()
    db.disburse_loan(writer, disbursement("app-1"))
    db.disburse_loan(writer, disbursement("app-1"))                      # the replay: the UPDATE is not guarded
    writer.close()

    code, out, err, sink = run(cdc_dsn, cdc_slot)
    assert code == 0 and err == ""
    events = landed_events(sink)
    assert [e["op"] for e in events] == ["c", "u", "u"]
    assert [e["flags"] for e in events] == [[], [], ["noop_update"]]
    first_update, second_update = events[1], events[2]
    assert first_update["before"] != first_update["after"]
    assert first_update["before"]["disbursed_at"] is None and first_update["after"]["disbursed_at"] is not None
    assert second_update["before"] == second_update["after"]               # nothing changed: a no-op, but still recorded
    assert {e["event_date"] for e in events} == {"2025-03-01"} and len(sink.objects) == 1   # all in the insert's partition
    assert len({key(e) for e in events}) == 3 and incomplete_transactions(events) == {}

    with psycopg.connect(cdc_dsn) as source:
        expected = table_contents(source)
    with blank_database(cdc_dsn) as replay_dsn, psycopg.connect(replay_dsn) as replayed:
        apply_events(replayed, in_commit_order(events))
        replayed.commit()
        assert table_contents(replayed) == expected
