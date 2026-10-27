"""tests/roundtrip.py: replay landed events into a blank database, to check them against the source.

Test support, not shipped code (applying events to tables is the later Bronze step's job). The point
is an independent check of the landed events: the rows are rebuilt by Postgres itself from each
event's `after` image (json_populate_record), so a mistake in the decoder's conversion of a
timestamp, a numeric or an array shows up as a difference instead of being undone by a matching
reverse conversion in the test.
"""
import contextlib
import json
import os

import psycopg
from psycopg import conninfo

from cdc.columns import EXPECTED_COLUMNS
from cdc.dedup import dedup, events_in, key
from simulator.db import STREAMS
from tests.conftest import MIGRATIONS

RUN = 1000   # consecutive inserts into one table applied in a single statement


@contextlib.contextmanager
def blank_database(dsn):
    """A new database on the same server, built from the migrations, dropped afterwards. Yields its DSN."""
    name = f"fintech_replay_{os.getpid()}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.execute(f'CREATE DATABASE "{name}"')
        replay_dsn = conninfo.make_conninfo(dsn, dbname=name)
        try:
            with psycopg.connect(replay_dsn, autocommit=True) as setup:
                for path in MIGRATIONS:
                    setup.execute(path.read_text())
            yield replay_dsn
        finally:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


def landed_events(sink) -> list[dict]:
    """Every event in every object, in object-name order (so not necessarily commit order, and with
    any duplicates that landed)."""
    return [event for name in sorted(sink.objects) for event in events_in(sink.objects[name])]


def in_commit_order(events: list[dict]) -> list[dict]:
    """Each change once, in (commit_lsn, position) order: what a consumer of the landed objects does."""
    return sorted(dedup(events), key=key)


def apply_events(conn, events: list[dict]) -> None:
    """Apply deduplicated events in commit order: an insert adds the row, an update replaces the row with
    that primary key. Strict: a duplicate insert or an update that finds no row raises."""
    run: list[dict] = []

    def flush() -> None:
        if not run:
            return
        table = run[0]["source"]["table"]
        conn.execute(f"INSERT INTO {table} SELECT * FROM json_populate_recordset(NULL::{table}, %s::json)",
                     (json.dumps([e["after"] for e in run]),))
        run.clear()

    for event in events:
        table, op = event["source"]["table"], event["op"]
        if op == "c":
            if run and (run[0]["source"]["table"] != table or len(run) >= RUN):
                flush()
            run.append(event)
            continue
        flush()
        assert op == "u", f"unexpected operation {op!r}"
        keys = STREAMS[table].key
        columns = [c for c in EXPECTED_COLUMNS[table] if c not in keys]
        sets = ", ".join(f"{c} = r.{c}" for c in columns)
        match = " AND ".join(f"x.{k} = r.{k}" for k in keys)
        updated = conn.execute(
            f"UPDATE {table} AS x SET {sets} FROM json_populate_record(NULL::{table}, %s::json) AS r WHERE {match}",
            (json.dumps(event["after"]),)).rowcount
        if updated != 1:
            raise AssertionError(f"an update of {table} found {updated} rows to replace, not 1: {event['source']}")
    flush()


def partition_order_problems(sink) -> list[str]:
    """Contract section 7 on order: within a table and event date the objects' LSN ranges follow one another
    and the events across them ascend strictly by (commit_lsn, position). Returns what breaks it."""
    partitions: dict[tuple[str, str], list[tuple[int, str]]] = {}
    for name in sink.objects:
        _, _, table, date_part, file = name.split("/")
        first = int(file.split("_")[1], 16)
        partitions.setdefault((table, date_part), []).append((first, name))
    problems = []
    for (table, date_part), objects in sorted(partitions.items()):
        previous = None
        for _, name in sorted(objects):
            events = events_in(sink.objects[name])
            keys = [key(e) for e in events]
            if keys != sorted(set(keys)):
                problems.append(f"{name}: events are not strictly ascending inside the object")
            if previous is not None and keys and keys[0] <= previous:
                problems.append(f"{name}: starts at {keys[0]}, not after the previous object's last {previous}")
            previous = keys[-1] if keys else previous
    return problems
