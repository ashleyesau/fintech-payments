"""
replay.py: does Bronze hold what landed, and does it replay into the source? (P4, G4's check built early)

Three questions, answered in this order by `check`:

  1. Does Bronze hold exactly the landed events? Every landed event (the first copy of each key, the consumer's
     clock fields left out) is compared, as an order-independent digest per table, with every Bronze row mapped
     back to an event through row_to_event. This needs no database. A key held twice in Bronze is counted.
  2. Does Bronze replay into the source? The Bronze events are inserted into a blank database built from the
     migrations and each table is compared with the source by the digest cdc.reconcile uses. The rows are
     rebuilt by Postgres from each event's `after` image, so a wrong conversion of a NUMERIC, a timestamp or a
     list is seen as a difference and not undone by a matching reverse conversion in the test.
  3. Do the updates found in Bronze touch exactly the loans the source shows as disbursed?

`roundtrip` is a fourth, asked of the landing alone before anything is written (P5): every landed event is converted
exactly as pass 2 will convert it (events_to_arrow with the table's real Arrow schema), read back out of Arrow and
mapped back with row_to_event, and must equal the original event, field by field. It writes nothing and needs no
catalog, warehouse or database.

It turns rows into events one slice at a time (SLICE rows; dict rows cost about 5.8 KB each) and, for question 1, reads
one landed object at a time, plus the keys seen so far. It is NOT bounded by one slice in the Arrow read: PyIceberg's
batch reader held about 0.9 KB a row of the whole table (measured at 10.7M rows, P6: 12.9 GB peak in all), so the check
needs roughly 1 GB a million rows of the largest table plus the key sets. That fits the Air at the real run's size (about
1M rows) and does not at 11M events. G4 should read one data file at a time if it must run at that size. It reads the landing through the
read-only LandingReader and writes nothing to Bronze or to the landing. `python -m bronze.verify` (bronze/verify.py) is the
command over `check`.
"""
from __future__ import annotations

import hashlib
import json
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from itertools import groupby
from typing import Callable, Iterator, Optional

import psycopg
from pyiceberg.catalog import Catalog

from bronze.batches import arrow_schema_of, events_to_arrow
from bronze.errors import MappingError, RoundTripError
from bronze.landing import LandingReader
from bronze.mapping import row_to_event
from bronze.run import NAMESPACE, _read_ahead
from bronze.schema import build_schema
from cdc.dedup import events_in, key
from cdc.landing import CLOCK_FIELDS
from cdc.reconcile import TABLE_ORDER, blank_database, explain_difference, insert_rows, table_digest, update_row

SLICE = 10_000   # rows turned into events at a time

# Where the time of a check goes, in the order it is shown: (key, what it is). `landed_wait` is the time the main thread
# spent idle waiting for the next landed object to arrive; `landed_work` is the time it spent on the object once it had
# it. If waiting is large the reads are the bottleneck (the read-ahead is not keeping the digest fed); if it is small the
# digest itself is.
STAGES = (
    ("source_before", "source digests, before the check"),
    ("landed_wait", "landed side: waiting for objects to arrive"),
    ("landed_work", "landed side: parsing and digesting"),
    ("bronze_read", "Bronze side: reading the tables"),
    ("bronze_convert", "Bronze side: converting rows to events"),
    ("bronze_digest", "Bronze side: digesting"),
    ("replay_insert", "replay: inserting into the scratch database"),
    ("replay_updates", "replay: applying the updates"),
    ("source_compare", "source and replay digests, and the disbursed loans"),
)


class Stopwatch:
    """Adds up the seconds spent in named stages. A stage may be entered many times; the seconds add. The clock is
    injected so a test can drive it."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self.clock = clock
        self.seconds: dict[str, float] = {}

    @contextmanager
    def stage(self, name: str):
        started = self.clock()
        try:
            yield
        finally:
            self.seconds[name] = self.seconds.get(name, 0.0) + (self.clock() - started)


def event_hash(event: dict) -> int:
    """60 bits of the md5 of the event without the consumer's clock, in a fixed key order. The clock differs
    between replays of the same change, so it is not part of what must be equal."""
    kept = {name: value for name, value in event.items() if name not in CLOCK_FIELDS}
    text = json.dumps(kept, sort_keys=True, separators=(",", ":"))
    return int(hashlib.md5(text.encode()).hexdigest()[:15], 16)


def payload_hash(event: dict) -> int:
    """60 bits of the md5 of what an event says about the row and not about where or when it was read: the table, the
    operation, the event date and time, the flags and the before and after images. The commit and change LSNs, the
    commit time, the transaction id and the consumer's clock are left out, because they differ between two clusters
    that hold the same rows, so equal payload digests mean the same rows were landed."""
    kept = {"table": event["source"]["table"], **{name: event[name] for name in
                                                  ("op", "event_date", "event_ts", "flags", "before", "after")}}
    return int(hashlib.md5(json.dumps(kept, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:15], 16)


@dataclass(frozen=True, slots=True)
class Digest:
    """How many events a table holds and the sum of 60 bits of each one's hash: equal if the same events are
    held, in any order."""
    rows: int = 0
    total: int = 0

    def add(self, event: dict, hasher: Callable[[dict], int] = event_hash) -> "Digest":
        return Digest(self.rows + 1, self.total + hasher(event))


def landed_digests(reader: LandingReader, say: Callable[[str], None] = lambda message: None, *, workers: int = 8,
                   stats: Optional[dict] = None, watch: Optional[Stopwatch] = None) -> dict[str, Digest]:
    """The digest of every landed change, once per key (the first copy in listing order, as the writer takes it).
    The objects are read `workers` at a time, a short way ahead of the digest, but folded in listing order, so the first
    copy of a key is the same one the writer takes whatever the order the reads finish in. With a dict as `stats` it
    is given the number of objects read. With a Stopwatch it is told how long the main thread waited for objects and how
    long it worked on them. It does not use anything else from bronze (except the read-ahead helper), so
    the check does not rest on the code it checks."""
    watch = watch or Stopwatch()
    digests = {table: Digest() for table in TABLE_ORDER}
    seen: set[tuple[int, int]] = set()
    objects = reader.list_keys()
    if stats is not None:
        stats["objects"] = len(objects)
    arriving = iter(_read_ahead(reader, objects, workers))
    number = 0
    while True:
        with watch.stage("landed_wait"):
            arrived = next(arriving, None)
        if arrived is None:
            break
        landed, data = arrived
        number += 1
        with watch.stage("landed_work"):
            for event in events_in(data):
                if key(event) in seen:
                    continue
                seen.add(key(event))
                table = event["source"]["table"]
                digests[table] = digests[table].add(event)
        if number % 100 == 0:
            say(f"landed side: {number:,} of {len(objects):,} objects")
    return digests


def bronze_slices(catalog: Catalog, *, source_db: str, snapshots: Optional[dict] = None,
                  watch: Optional[Stopwatch] = None) -> Iterator[tuple[str, list[dict]]]:
    """Every Bronze row as an event, table by table in the replay order, SLICE rows at a time. With a dict as
    `snapshots` it is given, for each table as it is loaded, the id of the snapshot being read (None if the table has
    none): the version of Bronze this read is of. With a Stopwatch it is told the time spent getting rows out of the
    tables and the time spent turning them into events."""
    watch = watch or Stopwatch()
    for name in TABLE_ORDER:
        with watch.stage("bronze_read"):
            table = catalog.load_table(f"{NAMESPACE}.{name}")
            if snapshots is not None:
                current = table.current_snapshot()
                snapshots[name] = current.snapshot_id if current else None
            batches = iter(table.scan().to_arrow_batch_reader())
        while True:
            with watch.stage("bronze_read"):
                batch = next(batches, None)
            if batch is None:
                break
            for start in range(0, batch.num_rows, SLICE):
                with watch.stage("bronze_convert"):
                    rows = batch.slice(start, SLICE).to_pylist()
                    events = [row_to_event(name, row, source_db=source_db) for row in rows]
                yield name, events


@dataclass
class Held:
    """What reading Bronze found: a digest per table, the keys held twice, and the updates (held for last)."""
    digests: dict[str, Digest] = field(default_factory=lambda: {table: Digest() for table in TABLE_ORDER})
    duplicates: dict[str, int] = field(default_factory=lambda: {table: 0 for table in TABLE_ORDER})
    updates: list[dict] = field(default_factory=list)
    snapshots: dict[str, Optional[int]] = field(default_factory=dict)


def read_bronze(catalog: Catalog, *, source_db: str, replay_into=None,
                say: Callable[[str], None] = lambda message: None, watch: Optional[Stopwatch] = None) -> Held:
    """One pass over Bronze. With a connection, the inserts are applied to it as they are read (the updates are
    kept for the caller: they go in last, in commit order, like cdc.reconcile does)."""
    watch = watch or Stopwatch()
    held = Held()
    seen: dict[str, set[tuple[int, int]]] = {table: set() for table in TABLE_ORDER}
    for name, slices in groupby(bronze_slices(catalog, source_db=source_db, snapshots=held.snapshots, watch=watch),
                                key=lambda part: part[0]):
        for _, events in slices:
            inserts = []
            with watch.stage("bronze_digest"):
                for event in events:
                    held.digests[name] = held.digests[name].add(event)
                    if key(event) in seen[name]:
                        held.duplicates[name] += 1
                    seen[name].add(key(event))
                    if event["op"] == "c":
                        inserts.append(event["after"])
                    else:
                        held.updates.append(event)
            if replay_into is not None and inserts:
                with watch.stage("replay_insert"):
                    insert_rows(replay_into, name, inserts)
        say(f"Bronze side: {name} {held.digests[name].rows:,} rows")
    return held


@dataclass
class Report:
    """The outcome of check: the problems (none means the check passed) and the figures it compared."""
    problems: list[str] = field(default_factory=list)
    landed: dict[str, Digest] = field(default_factory=dict)
    bronze: dict[str, Digest] = field(default_factory=dict)
    source: dict[str, tuple[int, int]] = field(default_factory=dict)
    replayed: dict[str, tuple[int, int]] = field(default_factory=dict)
    updates: int = 0
    disbursed: Optional[int] = None                     # loans the source shows as disbursed (None: not asked)
    snapshots: dict[str, Optional[int]] = field(default_factory=dict)   # the Bronze snapshot each table was read at
    objects: int = 0                                    # landed objects read
    moved: list[str] = field(default_factory=list)      # tables whose source digest changed while the check ran
    source_before: dict[str, tuple[int, int]] = field(default_factory=dict)   # the source's digests when the check began
    timings: dict[str, float] = field(default_factory=dict)   # seconds per stage (STAGES keys)
    total_seconds: float = 0.0                               # the whole check

    @property
    def ok(self) -> bool:
        """Passed: nothing differs and the source held still. A moved source is not a pass and not a Bronze failure."""
        return not self.problems and not self.moved


def landed_differences(landed: dict[str, Digest], held: Held) -> list[str]:
    problems = []
    for table in TABLE_ORDER:
        if held.duplicates[table]:
            problems.append(f"{table}: Bronze holds {held.duplicates[table]:,} keys more than once")
        if landed[table].rows != held.digests[table].rows:
            problems.append(f"{table}: {landed[table].rows:,} changes landed, Bronze holds {held.digests[table].rows:,}")
        elif landed[table] != held.digests[table]:
            problems.append(f"{table}: Bronze holds the same number of changes as landed ({landed[table].rows:,}) "
                            "but not the same events")
    return problems


def check(catalog: Catalog, reader: LandingReader, source_dsn: Optional[str], *, source_db: str,
          say: Callable[[str], None] = lambda message: None, workers: int = 8,
          clock: Callable[[], float] = time.monotonic) -> Report:
    """Run the three questions. With no source_dsn only the first is asked (no database is needed).

    With a source the check takes the source's table digests before it starts and again at the end: a table that
    changed in between is reported in `moved` and not compared (the comparison would be of Bronze with a source that
    was not the one Bronze was checked against), so a source that moved is "not proven" and never a Bronze mismatch.

    The report says where the time went (`timings`, the stages in STAGES) and how long the whole check took."""
    watch = Stopwatch(clock)
    began = clock()
    report = Report()
    try:
        return _check(catalog, reader, source_dsn, source_db, say, workers, watch, report)
    finally:
        report.timings, report.total_seconds = dict(watch.seconds), clock() - began


def _check(catalog, reader, source_dsn, source_db, say, workers, watch, report) -> Report:
    if source_dsn is not None:
        with watch.stage("source_before"):
            report.source_before = {table: table_digest(source_dsn, table) for table in TABLE_ORDER}
    stats: dict = {}
    report.landed = landed_digests(reader, say, workers=workers, stats=stats, watch=watch)
    report.objects = stats["objects"]
    if source_dsn is None:
        held = read_bronze(catalog, source_db=source_db, say=say, watch=watch)
        report.bronze, report.snapshots = held.digests, held.snapshots
        report.problems.extend(landed_differences(report.landed, held))
        report.updates = len(held.updates)
        return report

    with blank_database(source_dsn) as replay_dsn:
        with psycopg.connect(replay_dsn) as target:
            held = read_bronze(catalog, source_db=source_db, replay_into=target, say=say, watch=watch)
            report.bronze, report.updates, report.snapshots = held.digests, len(held.updates), held.snapshots
            report.problems.extend(landed_differences(report.landed, held))
            with watch.stage("replay_updates"):
                for event in sorted(held.updates, key=key):
                    try:
                        with target.transaction():
                            update_row(target, event)
                    except (psycopg.Error, AssertionError) as error:
                        report.problems.append(f"update at {key(event)}: {' '.join(str(error).split())[:200]}")
        with watch.stage("source_compare"):
            for table in TABLE_ORDER:
                report.source[table] = table_digest(source_dsn, table)
                report.replayed[table] = table_digest(replay_dsn, table)
                if report.source[table] != report.source_before[table]:
                    report.moved.append(table)
                    say(f"{table}: the source changed during the check ({report.source_before[table][0]:,} rows before, "
                        f"{report.source[table][0]:,} after); not compared")
                elif report.source[table] != report.replayed[table]:
                    report.problems.append(
                        f"{table}: the table replayed from Bronze differs from the source (rows {report.replayed[table][0]:,} "
                        f"against {report.source[table][0]:,}): {explain_difference(source_dsn, replay_dsn, table)}")
                else:
                    say(f"{table}: {report.source[table][0]:,} rows, the replay from Bronze equals the source")
            with psycopg.connect(source_dsn, autocommit=True) as source:
                disbursed = {row[0] for row in source.execute(
                    "SELECT application_id FROM loan_applications WHERE disbursed_at IS NOT NULL")}
        report.disbursed = len(disbursed)
        updated = {event["after"]["application_id"] for event in held.updates}
        if updated != disbursed and "loan_applications" not in report.moved:
            report.problems.append(f"Bronze holds updates for {len(updated):,} loans, the source shows {len(disbursed):,} "
                                   f"disbursed ({len(updated ^ disbursed):,} differ)")
    return report


# ---------------------------------------------------------------------------
# P5: the real events through the pass 2 conversion and back, before anything is written
# ---------------------------------------------------------------------------

def first_difference(expected, found, path: str = "") -> Optional[str]:
    """Where two events (or parts of them) first differ, as `path: expected ..., got ...`, or None if they are equal.
    Types count: 1 and True and 1.0 are different, and so are a tuple and a list."""
    if type(expected) is not type(found):
        return f"{path or 'event'}: expected {expected!r}, got {found!r}"
    if isinstance(expected, dict):
        for name in expected:
            if name not in found:
                return f"{path + '.' if path else ''}{name}: missing"
        for name in found:
            if name not in expected:
                return f"{path + '.' if path else ''}{name}: unexpected"
        for name in expected:
            difference = first_difference(expected[name], found[name], f"{path + '.' if path else ''}{name}")
            if difference:
                return difference
        return None
    if isinstance(expected, list):
        if len(expected) != len(found):
            return f"{path}: expected {len(expected)} elements, got {len(found)}"
        for number, (a, b) in enumerate(zip(expected, found)):
            difference = first_difference(a, b, f"{path}[{number}]")
            if difference:
                return difference
        return None
    return None if expected == found else f"{path or 'event'}: expected {expected!r}, got {found!r}"


@dataclass
class RoundTrip:
    """What `roundtrip` found: how many events and objects, and a payload digest per table."""
    events: int = 0
    objects: int = 0
    digests: dict[str, Digest] = field(default_factory=lambda: {table: Digest() for table in TABLE_ORDER})


def roundtrip(reader: LandingReader, *, source_db: str, workers: int = 8,
              say: Callable[[str], None] = lambda message: None) -> RoundTrip:
    """Convert every landed event as pass 2 will, read it back, map it back and compare with the original. Raises
    RoundTripError at the first event that does not come back equal, and a MappingError (naming the object and event)
    if the conversion or its inverse refuses a value. One object is held at a time, plus a short read-ahead of bytes."""
    result = RoundTrip()
    schemas = {table: arrow_schema_of(build_schema(table)) for table in TABLE_ORDER}
    objects = reader.list_keys()
    for number, (obj, data) in enumerate(_read_ahead(reader, objects, workers), 1):
        events = events_in(data)
        rows = events_to_arrow(events, schemas[obj.table], source_db=source_db, label=obj.key).to_pylist()
        for position, (event, row) in enumerate(zip(events, rows), 1):
            try:
                back = row_to_event(obj.table, row, source_db=source_db)
            except MappingError as error:
                error.where = f"{obj.key}, event {position}: "
                raise
            difference = first_difference(event, back)
            if difference:
                raise RoundTripError(f"{obj.key}, event {position}: {obj.table} did not come back equal from the pass 2 "
                                     f"conversion: {difference}")
            result.digests[obj.table] = result.digests[obj.table].add(event, payload_hash)
        result.events += len(events)
        result.objects += 1
        if number % 100 == 0:
            say(f"round trip: {number:,} of {len(objects):,} objects")
    return result
