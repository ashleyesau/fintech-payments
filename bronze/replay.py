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

It holds one slice of a table at a time (SLICE rows, dict rows cost about 5.8 KB each) and, for question 1, one
landed object at a time plus the keys seen so far, so the real run fits the Air. It reads the landing through the
read-only LandingReader and writes nothing to Bronze or to the landing. There is no command yet: G4 decides it.
"""
from __future__ import annotations

import hashlib
import json
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


def landed_digests(reader: LandingReader, say: Callable[[str], None] = lambda message: None) -> dict[str, Digest]:
    """The digest of every landed change, once per key (the first copy in listing order, as the writer takes it).
    It does not use anything else from bronze, so the check does not rest on the code it checks."""
    digests = {table: Digest() for table in TABLE_ORDER}
    seen: set[tuple[int, int]] = set()
    objects = reader.list_keys()
    for number, landed in enumerate(objects, 1):
        for event in events_in(reader.read(landed.key)):
            if key(event) in seen:
                continue
            seen.add(key(event))
            table = event["source"]["table"]
            digests[table] = digests[table].add(event)
        if number % 100 == 0:
            say(f"landed side: {number:,} of {len(objects):,} objects")
    return digests


def bronze_slices(catalog: Catalog, *, source_db: str) -> Iterator[tuple[str, list[dict]]]:
    """Every Bronze row as an event, table by table in the replay order, SLICE rows at a time."""
    for name in TABLE_ORDER:
        table = catalog.load_table(f"{NAMESPACE}.{name}")
        for batch in table.scan().to_arrow_batch_reader():
            for start in range(0, batch.num_rows, SLICE):
                rows = batch.slice(start, SLICE).to_pylist()
                yield name, [row_to_event(name, row, source_db=source_db) for row in rows]


@dataclass
class Held:
    """What reading Bronze found: a digest per table, the keys held twice, and the updates (held for last)."""
    digests: dict[str, Digest] = field(default_factory=lambda: {table: Digest() for table in TABLE_ORDER})
    duplicates: dict[str, int] = field(default_factory=lambda: {table: 0 for table in TABLE_ORDER})
    updates: list[dict] = field(default_factory=list)


def read_bronze(catalog: Catalog, *, source_db: str, replay_into=None,
                say: Callable[[str], None] = lambda message: None) -> Held:
    """One pass over Bronze. With a connection, the inserts are applied to it as they are read (the updates are
    kept for the caller: they go in last, in commit order, like cdc.reconcile does)."""
    held = Held()
    seen: dict[str, set[tuple[int, int]]] = {table: set() for table in TABLE_ORDER}
    for name, slices in groupby(bronze_slices(catalog, source_db=source_db), key=lambda part: part[0]):
        for _, events in slices:
            inserts = []
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

    @property
    def ok(self) -> bool:
        return not self.problems


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
          say: Callable[[str], None] = lambda message: None) -> Report:
    """Run the three questions. With no source_dsn only the first is asked (no database is needed)."""
    report = Report()
    report.landed = landed_digests(reader, say)
    if source_dsn is None:
        held = read_bronze(catalog, source_db=source_db, say=say)
        report.bronze = held.digests
        report.problems.extend(landed_differences(report.landed, held))
        report.updates = len(held.updates)
        return report

    with blank_database(source_dsn) as replay_dsn:
        with psycopg.connect(replay_dsn) as target:
            held = read_bronze(catalog, source_db=source_db, replay_into=target, say=say)
            report.bronze, report.updates = held.digests, len(held.updates)
            report.problems.extend(landed_differences(report.landed, held))
            for event in sorted(held.updates, key=key):
                try:
                    with target.transaction():
                        update_row(target, event)
                except (psycopg.Error, AssertionError) as error:
                    report.problems.append(f"update at {key(event)}: {' '.join(str(error).split())[:200]}")
        for table in TABLE_ORDER:
            report.source[table] = table_digest(source_dsn, table)
            report.replayed[table] = table_digest(replay_dsn, table)
            if report.source[table] != report.replayed[table]:
                report.problems.append(
                    f"{table}: the table replayed from Bronze differs from the source (rows {report.replayed[table][0]:,} "
                    f"against {report.source[table][0]:,}): {explain_difference(source_dsn, replay_dsn, table)}")
            else:
                say(f"{table}: {report.source[table][0]:,} rows, the replay from Bronze equals the source")
        with psycopg.connect(source_dsn, autocommit=True) as source:
            disbursed = {row[0] for row in source.execute(
                "SELECT application_id FROM loan_applications WHERE disbursed_at IS NOT NULL")}
        updated = {event["after"]["application_id"] for event in held.updates}
        if updated != disbursed:
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
