"""
reconcile.py: do the landed objects add up to the source database? (M6-Q2, the definition of done)

    python -m cdc.reconcile                       the objects in the bucket under this slot's prefix
    python -m cdc.reconcile --directory DIR       the same, from a directory laid out like the bucket

It reads every object under the prefix, then checks, in this order:
  - each object is what its name says (table, event date, first and last commit LSN), is valid gzipped
    NDJSON of events, and its events ascend strictly by (commit_lsn, position);
  - every transaction is whole once duplicates are removed (positions 0 to size - 1, contract section 7);
  - the landed inserts of each table equal the rows in the source, and the landed updates touch exactly
    the loans the source shows as disbursed;
  - the events replayed into a temporary blank database reproduce every table of the source: compared by
    row count and an order-independent digest of every row, computed in SQL.
Duplicates (the same change landed twice, as at-least-once delivery allows) are counted and absorbed.
Objects whose LSN ranges overlap within a table and event date are a note, not a failure: a replay that
covered more data writes overlapping objects by design.

It holds one object at a time, never the whole run: a 100-merchant year is about 1M events, 1.3 GB as JSON.
The source must be quiet and the consumer drained (`python -m cdc.main --once` says nothing was waiting).

Exit codes: 0 reconciled, 2 not reconciled (the problems are printed), 3 the bucket failed, 1 anything else.
"""
from __future__ import annotations

import contextlib
import json
import os
import re
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterator, Optional, Protocol, TextIO

import click
import psycopg
from psycopg import conninfo

from cdc.columns import EXPECTED_COLUMNS
from cdc.config import CdcSettings
from cdc.dedup import events_in, key
from cdc.errors import SinkError
from simulator.db import STREAMS

MIGRATIONS = sorted((Path(__file__).resolve().parent.parent / "migrations").glob("*.sql"))
# Inserts are applied in this order: a repayment points at its application (migration 002).
TABLE_ORDER = ("merchants", "merchant_transactions", "risk_events", "loan_applications", "loan_repayments")
RUN = 1000   # rows inserted per statement

NAME = re.compile(r"^(?P<prefix>.*/)(?P<table>[a-z_]+)/event_date=(?P<date>\d{4}-\d\d-\d\d)/"
                  r"lsn_(?P<first>[0-9a-f]{16})_(?P<last>[0-9a-f]{16})\.ndjson\.gz$")

# A digest of a table that does not depend on row order: the sum of 60 bits of each row's md5.
DIGEST = "SELECT count(1), coalesce(sum(('x' || substr(md5(t::text), 1, 15))::bit(60)::bigint), 0) FROM {table} t"


class Objects(Protocol):
    def keys(self) -> list[str]: ...
    def read(self, key: str) -> bytes: ...


class MemoryObjects:
    """Objects held in a dict (tests)."""
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    def keys(self) -> list[str]:
        return sorted(self.objects)

    def read(self, key: str) -> bytes:
        return self.objects[key]


class DirectoryObjects:
    """Objects in a directory laid out like the bucket (a rehearsal's landing directory), under `prefix`."""
    def __init__(self, root, prefix: str = "") -> None:
        self.root, self.prefix = Path(root), prefix

    def keys(self) -> list[str]:
        found = (p.relative_to(self.root).as_posix() for p in self.root.rglob("*.ndjson.gz"))
        return sorted(k for k in found if k.startswith(self.prefix))

    def read(self, key: str) -> bytes:
        return (self.root / key).read_bytes()


class BucketObjects:
    """Objects under a prefix in the bucket: one listing (never a loop) and one read per object."""
    def __init__(self, sink, prefix: str) -> None:
        self.sink, self.prefix = sink, prefix

    def keys(self) -> list[str]:
        return self.sink.list_keys(self.prefix)

    def read(self, key: str) -> bytes:
        return self.sink.read(key)


# ---------------------------------------------------------------------------
# applying events (also used by the round-trip tests)
# ---------------------------------------------------------------------------

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


def insert_rows(conn, table: str, rows: list[dict]) -> None:
    """Insert rows rebuilt by Postgres itself from the events' `after` images (json_populate_recordset), so the
    decoder's conversions are checked by Postgres's own, not by the reverse of our own."""
    for start in range(0, len(rows), RUN):
        conn.execute(f"INSERT INTO {table} SELECT * FROM json_populate_recordset(NULL::{table}, %s::json)",
                     (json.dumps(rows[start:start + RUN]),))


def update_row(conn, event: dict) -> None:
    """A disbursement update replaces the row with that primary key; it must find exactly one."""
    table = event["source"]["table"]
    keys = STREAMS[table].key
    columns = [c for c in EXPECTED_COLUMNS[table] if c not in keys]
    sets = ", ".join(f"{c} = r.{c}" for c in columns)
    match = " AND ".join(f"x.{k} = r.{k}" for k in keys)
    updated = conn.execute(
        f"UPDATE {table} AS x SET {sets} FROM json_populate_record(NULL::{table}, %s::json) AS r WHERE {match}",
        (json.dumps(event["after"]),)).rowcount
    if updated != 1:
        raise AssertionError(f"an update of {table} found {updated} rows to replace, not 1: {event['source']}")


def in_commit_order(events: list[dict]) -> list[dict]:
    """Each change once, in (commit_lsn, position) order."""
    seen, unique = set(), []
    for event in events:
        if key(event) not in seen:
            seen.add(key(event))
            unique.append(event)
    return sorted(unique, key=key)


def apply_events(conn, events: list[dict]) -> None:
    """Apply deduplicated events in commit order, strictly: a duplicate insert or an update that finds no row raises."""
    run: list[dict] = []

    def flush() -> None:
        if run:
            insert_rows(conn, run[0]["source"]["table"], [e["after"] for e in run])
            run.clear()

    for event in events:
        table, op = event["source"]["table"], event["op"]
        if op == "c":
            if run and run[0]["source"]["table"] != table:
                flush()
            run.append(event)
        else:
            flush()
            assert op == "u", f"unexpected operation {op!r}"
            update_row(conn, event)
    flush()


# ---------------------------------------------------------------------------
# the reconciliation
# ---------------------------------------------------------------------------

@dataclass
class Result:
    problems: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    objects: int = 0
    events: int = 0
    duplicates: int = 0
    inserts: dict = field(default_factory=dict)
    updates: int = 0
    megabytes: float = 0.0

    @property
    def ok(self) -> bool:
        return not self.problems


def parse_name(name: str):
    match = NAME.match(name)
    return None if match is None else (match["table"], match["date"], int(match["first"], 16), int(match["last"], 16))


def check_object(name: str, parsed, events: list[dict]) -> list[str]:
    table, date, first, last = parsed
    if not events:
        return [f"{name}: holds no events"]
    problems = []
    if {e["source"]["table"] for e in events} != {table}:
        problems.append(f"{name}: holds events of other tables than {table}")
    if {e["event_date"] for e in events} != {date}:
        problems.append(f"{name}: holds events of other dates than {date}")
    keys = [key(e) for e in events]
    if keys != sorted(set(keys)):
        problems.append(f"{name}: its events do not ascend strictly by (commit_lsn, position)")
    if events[0]["source"]["commit_lsn"] != first or events[-1]["source"]["commit_lsn"] != last:
        problems.append(f"{name}: its first and last commit LSN are not the ones in its name")
    return problems


def table_digest(dsn: str, table: str) -> tuple[int, int]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SET TimeZone = 'UTC'")
        count, digest = conn.execute(DIGEST.format(table=table)).fetchone()
        return count, int(digest)


def row_hashes(dsn: str, table: str) -> dict:
    keys = ", ".join(STREAMS[table].key)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SET TimeZone = 'UTC'")
        return {tuple(row[:-1]): row[-1] for row in conn.execute(f"SELECT {keys}, md5(t::text) FROM {table} t")}


def explain_difference(source_dsn: str, replay_dsn: str, table: str) -> str:
    source, replayed = row_hashes(source_dsn, table), row_hashes(replay_dsn, table)
    only_source = [k for k in source if k not in replayed]
    only_replayed = [k for k in replayed if k not in source]
    different = [k for k in source if k in replayed and source[k] != replayed[k]]
    parts = []
    for label, found in (("in the source only", only_source), ("in the replay only", only_replayed), ("with different values", different)):
        if found:
            parts.append(f"{len(found)} {label}, for example {found[0]}")
    return "; ".join(parts) or "the digests differ but no row differs by key"


def reconcile(objects: Objects, source_dsn: str, *, workers: int = 8,
              say: Callable[[str], None] = lambda message: None) -> Result:
    result = Result(inserts={t: 0 for t in TABLE_ORDER})
    names = objects.keys()
    result.objects = len(names)
    say(f"{len(names)} objects to read")
    parsed, order = {}, []
    for name in names:
        info = parse_name(name)
        if info is None:
            result.problems.append(f"{name}: not an object name of the form <prefix><table>/event_date=<date>/lsn_<first>_<last>.ndjson.gz")
        elif info[0] not in TABLE_ORDER:
            result.problems.append(f"{name}: {info[0]} is not one of the five tables")
        else:
            parsed[name] = info
    order = sorted(parsed, key=lambda n: (TABLE_ORDER.index(parsed[n][0]), parsed[n][2], n))

    seen: dict[int, set] = {}          # commit_lsn -> the positions landed
    sizes: dict[int, set] = {}         # commit_lsn -> the sizes the events claim
    partitions: dict[tuple, list] = {}
    updates: dict[tuple, dict] = {}
    with blank_database(source_dsn) as replay_dsn:
        with psycopg.connect(replay_dsn) as target:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                for name, data in zip(order, pool.map(objects.read, order)):
                    result.megabytes += len(data) / 1e6
                    try:
                        events = events_in(data)
                    except (OSError, EOFError, ValueError) as error:
                        result.problems.append(f"{name}: not gzipped NDJSON of events ({type(error).__name__}: {error})")
                        continue
                    broken = check_object(name, parsed[name], events)
                    result.problems.extend(broken)
                    if broken:
                        continue              # an object that is not what its name says is not replayed; the counts will show it
                    table = parsed[name][0]
                    partitions.setdefault((table, parsed[name][1]), []).append((key(events[0]), key(events[-1]), name))
                    rows = []
                    for event in events:
                        result.events += 1
                        commit_lsn, position = key(event)
                        sizes.setdefault(commit_lsn, set()).add(event["transaction"]["size"])
                        if position in seen.setdefault(commit_lsn, set()):
                            result.duplicates += 1
                            continue
                        seen[commit_lsn].add(position)
                        if event["op"] == "c":
                            rows.append(event["after"])
                            result.inserts[event["source"]["table"]] += 1
                        else:
                            updates[(commit_lsn, position)] = event
                    try:
                        with target.transaction():           # a savepoint: one bad object must not spoil the rest
                            insert_rows(target, table, rows)
                    except psycopg.Error as error:
                        result.problems.append(f"{name}: its rows could not be replayed ({type(error).__name__}: "
                                               f"{' '.join(str(error).split())[:200]})")
            for event_key in sorted(updates):
                try:
                    with target.transaction():
                        update_row(target, updates[event_key])
                    result.updates += 1
                except (psycopg.Error, AssertionError) as error:
                    result.problems.append(f"update at {event_key}: {' '.join(str(error).split())[:200]}")
            target.commit()
        say(f"read {result.megabytes:,.1f} MB: {result.events:,} events, {result.duplicates:,} duplicates absorbed")

        for commit_lsn, positions in seen.items():
            claimed = sizes[commit_lsn]
            if len(claimed) != 1:
                result.problems.append(f"transaction {commit_lsn}: its events disagree about its size {sorted(claimed)}")
            elif positions != set(range(next(iter(claimed)))):
                result.problems.append(f"transaction {commit_lsn}: landed positions {sorted(positions)} of {next(iter(claimed))}")
        overlaps = 0
        for parts in partitions.values():
            parts.sort()
            overlaps += sum(1 for a, b in zip(parts, parts[1:]) if b[0] <= a[1])
        if overlaps:
            result.notes.append(f"{overlaps} objects overlap the one before them within a table and event date "
                                "(a replay that covered more data): the duplicates were absorbed")

        with psycopg.connect(source_dsn, autocommit=True) as source:
            for table in TABLE_ORDER:
                rows = source.execute(f"SELECT count(1) FROM {table}").fetchone()[0]
                if result.inserts[table] != rows:
                    result.problems.append(f"{table}: {result.inserts[table]:,} inserts landed, the source has {rows:,} rows")
            disbursed = {r[0] for r in source.execute("SELECT application_id FROM loan_applications WHERE disbursed_at IS NOT NULL")}
        updated = {e["after"]["application_id"] for e in updates.values()}
        if updated != disbursed:
            result.problems.append(f"updates landed for {len(updated):,} loans, the source shows {len(disbursed):,} disbursed "
                                   f"({len(updated ^ disbursed):,} differ)")
        for table in TABLE_ORDER:
            ours, theirs = table_digest(source_dsn, table), table_digest(replay_dsn, table)
            if ours != theirs:
                result.problems.append(f"{table}: the replayed table differs from the source (rows {theirs[0]:,} against "
                                       f"{ours[0]:,}): {explain_difference(source_dsn, replay_dsn, table)}")
            else:
                say(f"{table}: {ours[0]:,} rows, the replay equals the source")
    return result


# ---------------------------------------------------------------------------
# the command line
# ---------------------------------------------------------------------------

def execute(settings: CdcSettings, *, directory: Optional[str] = None, workers: int = 8,
            objects_factory: Optional[Callable[[CdcSettings], Objects]] = None,
            out: Optional[TextIO] = None, err: Optional[TextIO] = None) -> int:
    out, err = out or sys.stdout, err or sys.stderr

    def say(message: str) -> None:
        print(message, file=out, flush=True)

    try:
        if objects_factory is not None:
            objects = objects_factory(settings)
        elif directory is not None:
            objects = DirectoryObjects(directory, settings.gcs_prefix)
        else:
            from cdc.gcs import GcsSink          # only the bucket needs Google's libraries
            objects = BucketObjects(GcsSink.connect(settings.gcs_bucket, project=settings.project), settings.gcs_prefix)
        where = f"directory {directory}" if directory else f"gs://{settings.gcs_bucket}/{settings.gcs_prefix}"
        say(f"reconciling {where} with {settings.db}")
        result = reconcile(objects, settings.dsn, workers=workers, say=say)
    except SinkError as error:
        print(f"error: {' '.join(str(error).split())}", file=err, flush=True)
        return 3
    except BaseException as error:
        if isinstance(error, (SystemExit, KeyboardInterrupt)):
            raise
        text = " ".join(str(error).split())
        if isinstance(error, psycopg.OperationalError):
            print(f"error: lost or could not make the database connection: {text}", file=err, flush=True)
        else:
            print(f"error: unexpected {type(error).__name__}: {text}", file=err, flush=True)
            traceback.print_exception(error, file=err)
        return 1
    for note in result.notes:
        say(f"note: {note}")
    if result.ok:
        landed = sum(result.inserts.values())
        say(f"RECONCILED: {result.objects:,} objects, {result.events:,} events ({result.duplicates:,} duplicates absorbed), "
            f"{landed:,} inserts and {result.updates:,} updates; the replay equals {settings.db} in every table")
        return 0
    for problem in result.problems:
        print(f"PROBLEM: {problem}", file=err, flush=True)
    print(f"error: not reconciled: {len(result.problems)} problems", file=err, flush=True)
    return 2


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--directory", type=click.Path(file_okay=False), default=None,
              help="Read the objects from a directory laid out like the bucket, not from the bucket.")
@click.option("--workers", default=8, show_default=True, help="Objects read at once.")
def cli(directory: Optional[str], workers: int) -> None:
    """Check that the landed objects add up to the source database: whole objects, whole transactions, matching
    counts, and a replay into a blank database that equals every table. Settings come from the CDC_ variables."""
    sys.exit(execute(CdcSettings(), directory=directory, workers=workers))


def main(argv: Optional[list[str]] = None) -> int:
    try:
        cli.main(args=argv, standalone_mode=False)
    except SystemExit as stop:
        return int(stop.code or 0)
    except click.ClickException as error:
        print(f"error: {error.format_message()}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
