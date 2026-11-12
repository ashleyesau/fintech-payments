"""
run.py: one Bronze run, from the landed objects to appended Iceberg rows (GCS Plan (V2) sections 4 and 19, G3).

run(reader, catalog, ...) is the whole write path as a function that is given what it touches: a LandingReader
(the landed objects, read only) and an Iceberg catalog (the Bronze tables). The command line (step 5) and the
tests give it the real bucket or a local warehouse and a fake landing; no test flag exists in the code. With
write=False it is a plan: it reads, checks and counts, and writes nothing anywhere.

The order of a run (G3 questions 1 to 3, decided 2026-10-29):

  1. Every table that already exists is loaded and checked against the contract (bronze.tables.check_table):
     schema, partition spec, the four properties, location. A difference is a TableDriftError, before pass 1.
     Then the newest run marker is read (one listing of _runs/ and one read) and the catalog is checked against it
     (bronze.marker.check_against_marker): a catalog that lacks a table the marker names, or its snapshot, stops the
     run here, before any object is read, because creating the tables again would write them beside the old ones.
  1b. A WRITE run with nothing new since the newest marker stops here, having read no object: the tables are at the
     marker's snapshots, the listing holds the marker's number of objects and its highest last_lsn is the marker's
     highest_commit_lsn (_unchanged_since_marker). A plan never takes this path: it always checks everything.
  2. Pass 1 (bronze.index.build_index): every landed object is read once, checked and mapped; the index must be
     complete. Anything wrong stops the run here, with no table created and nothing written.
  3. For every existing table, the keys Bronze holds are read (bronze.present) and compared with the index.
     Both integrity stops (a key twice, a key the landing does not hold) therefore fire before any write, not
     only before the first write to that table. (My call: the decided design read each table's keys just before
     its own first batch; reading all of them first costs about 1.5 bytes an event and moves the two stops in
     front of every commit.) From the keys and the index come, per table, the objects that hold a key Bronze
     lacks, and how many rows the run will append: the unique keys the landing holds for the table minus the
     keys Bronze holds, exact because both integrity stops passed.
  4. A plan (write=False) ends here and returns what it found.
  5. All the missing tables are created, before any append, with exactly build_schema, build_partition_spec and
     table_properties, so a run that dies early leaves all five tables or none. One that
     turns out to exist already (TableAlreadyExistsError: a second writer, which is unsupported) is loaded,
     checked and its keys read, then treated like any other existing table.
  6. Per table, in TABLE_ORDER, pass 2: threads prefetch the bytes of the objects that hold missing keys (at
     most 2 x workers objects ahead), the main thread parses each object, keeps the events whose key Bronze lacks
     and whose first copy is this object, maps them straight to Arrow (bronze.batches) and drops the dict rows.
     Whole objects gather into a Batch; a full batch is sorted and appended as ONE commit, and is committed
     before the next is built. The last, partial batch is committed at the end of the table.

  7. At the end of a write run (G3 question 4): if the tables are not at the newest marker's snapshots, a new marker is
     created (bronze.marker; every table holds every key the landing holds by now, which the total check of step 6
     guarantees); then, if a marker was written, the local catalog file is copied to the bucket
     (bronze.catalog_copy). A run that created a table or committed a batch has new snapshots, so it writes a marker:
     this is "the copy follows any run that committed something". A failure of either is a SinkError whose message says what is already
     committed; a re-run writes the marker. A plan only reads the newest marker.

What a re-run does. A batch is whole objects and a commit is atomic, so after a crash a table holds some whole
objects' missing events; the next run reads the keys again and finds only the objects still missing. With
nothing missing a run makes no snapshot, creates no table and writes no file: it only reads. A key is appended
once because the filter is the key check and because an event repeated inside one object is taken once.

Errors are the bronze.errors classes and cdc.errors.SinkError; the command (step 5) turns them into exit codes. A
failure while committing leaves the batches before it committed, and on_batch has been told about each of them, so the
command can say how many.
"""
from __future__ import annotations

import zlib
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator, Optional

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError, TableAlreadyExistsError
from pyiceberg.table import Table

from bronze.batches import Batch, arrow_schema_of, events_to_arrow
from bronze.catalog_copy import copy_catalog
from bronze.errors import ConfigError, LandingChanged
from bronze.index import Index, IndexSummary, build_index
from bronze.landing import LandedObject, LandingReader
from bronze.marker import Marker, TableMark, build_marker, check_against_marker, describes, newest_marker, write_marker
from bronze.present import PresentKeys, read_present_keys
from bronze.schema import build_partition_spec, build_schema, table_properties
from bronze.store import Store
from bronze.tables import check_table
from cdc.columns import EXPECTED_COLUMNS
from cdc.dedup import events_in, key
from cdc.errors import SinkError

NAMESPACE = "bronze"
# The order tables are written in (G3 question 4): dimension first, the big table last.
TABLE_ORDER = ("merchants", "loan_applications", "loan_repayments", "risk_events", "merchant_transactions")
READ_AHEAD_PER_WORKER = 2


@dataclass(frozen=True, slots=True)
class BatchCommitted:
    """One batch, committed. Passed to on_batch after the commit."""
    table: str
    batch: int           # this table's batch number in this run, from 1
    rows: int            # rows in this batch
    rows_done: int       # rows appended to this table so far in this run
    rows_total: int      # rows the run will append to this table
    snapshot_id: int


@dataclass(frozen=True, slots=True)
class TableReport:
    table: str
    existed: bool                    # the table was in the catalog when the run started
    created: bool                    # this run created it (write runs only)
    landed: int                      # unique changes the landing holds for this table
    rows_before: int                 # keys Bronze held for it at the start
    to_append: int                   # landed - rows_before
    objects_to_read: int             # landed objects that hold a key Bronze lacks
    batches: int                     # commits made by this run
    rows_appended: int
    snapshot_before: Optional[int]   # None: no table or no snapshot
    snapshot_after: Optional[int]
    metadata_location: Optional[str]  # of the table as the run ends; None if there is no table


@dataclass(frozen=True, slots=True)
class MarkerReport:
    newest: Optional[str]        # run_id of the newest marker when the run started; None if there was none
    current: bool                # the tables were at that marker's snapshots when the run started
    written: Optional[str]       # run_id of the marker this run wrote; None if it wrote none
    catalog_copied: bool         # this run replaced the catalog copy in the bucket


@dataclass(frozen=True, slots=True)
class RunReport:
    write: bool
    summary: Optional[IndexSummary]   # None when the index was skipped (index_skipped)
    tables: dict            # table -> TableReport, in TABLE_ORDER
    marker: MarkerReport
    index_skipped: bool = False       # a write run found the landing unchanged since the newest marker and read no object


@dataclass(slots=True)
class _Work:
    table: str
    bronze: Optional[Table]
    present: PresentKeys
    objects: list
    landed: int
    existed: bool
    created: bool = False


def _load(catalog: Catalog, name: str) -> Optional[Table]:
    try:
        return catalog.load_table(f"{NAMESPACE}.{name}")
    except NoSuchTableError:
        return None


def _snapshot_id(table: Optional[Table]) -> Optional[int]:
    snapshot = table.current_snapshot() if table is not None else None
    return snapshot.snapshot_id if snapshot is not None else None


def _unchanged_since_marker(reader: LandingReader, marker: Marker, existing: dict) -> Optional[list]:
    """The landed objects, if nothing new has landed since `marker`; otherwise None (do the full run).

    Nothing new means all of: the five tables exist and sit exactly at the marker's snapshots (not ahead of them,
    which is a run that crashed before its marker), the listing holds exactly as many objects as the marker's run
    indexed, and the highest last_lsn in the object names is the marker's highest_commit_lsn. Objects are never
    overwritten (put_if_absent) and the Bronze writer cannot write or delete in the landing, so the same count and
    the same highest LSN mean the same objects. That is a trust the full run does not need; bronze.check is the
    full proof. Any doubt gives None. The listing is one Class A operation per 1,000 names and every name is
    still checked against the object-name contract (LandingError).
    """
    if len(existing) != len(TABLE_ORDER):
        return None
    if not describes(marker, {name: _snapshot_id(table) for name, table in existing.items()}):
        return None
    listed = reader.list_keys()
    if len(listed) != marker.landed_objects:
        return None
    if max((obj.last_lsn for obj in listed), default=None) != marker.highest_commit_lsn:
        return None
    return listed


def _skipped_report(marker: Marker, existing: dict) -> RunReport:
    """The report of a write run that read nothing: the marker's row counts are the landing's, because a marker is
    written only when every table holds every key the landing holds."""
    tables = {}
    for name in TABLE_ORDER:
        table, rows = existing[name], marker.tables[name].rows
        tables[name] = TableReport(
            table=name, existed=True, created=False, landed=rows, rows_before=rows, to_append=0,
            objects_to_read=0, batches=0, rows_appended=0, snapshot_before=_snapshot_id(table),
            snapshot_after=_snapshot_id(table), metadata_location=table.metadata_location)
    return RunReport(write=True, summary=None, tables=tables,
                     marker=MarkerReport(marker.run_id, True, None, False), index_skipped=True)


def _plan(name: str, bronze: Optional[Table], index: Index, landed: int, *,
          existed: bool, created: bool = False) -> _Work:
    """What the run must do for one table: the keys Bronze holds and the objects that hold a key it lacks."""
    present = read_present_keys(name, bronze, index) if bronze is not None else PresentKeys()
    return _Work(name, bronze, present, index.objects_missing(name, present), landed, existed, created)


def _create(catalog: Catalog, name: str, *, source_prefix: str, source_db: str, location: str) -> tuple[Table, bool]:
    """Create the table exactly as the contract generates it. Returns (table, created). If it exists already
    it is loaded and checked instead, and `created` is False."""
    schema = build_schema(name)
    try:
        table = catalog.create_table(f"{NAMESPACE}.{name}", schema=schema,
                                     partition_spec=build_partition_spec(schema),
                                     properties=table_properties(source_prefix, source_db))
        return table, True
    except TableAlreadyExistsError:
        table = catalog.load_table(f"{NAMESPACE}.{name}")
        check_table(name, table, source_prefix=source_prefix, source_db=source_db, location=location)
        return table, False


def _read_ahead(reader: LandingReader, objects: list, workers: int) -> Iterator[tuple[LandedObject, bytes]]:
    """(object, bytes) in the order given, with the reads running in threads at most 2 x workers ahead of the
    consumer. Only bytes are held ahead; nothing is parsed off the main thread."""
    window = READ_AHEAD_PER_WORKER * workers
    pending: deque[tuple[LandedObject, Future]] = deque()
    remaining = iter(objects)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        try:
            while True:
                while len(pending) < window:
                    try:
                        obj = next(remaining)
                    except StopIteration:
                        break
                    pending.append((obj, pool.submit(reader.read, obj.key)))
                if not pending:
                    return
                obj, future = pending.popleft()
                yield obj, future.result()
        finally:
            for _, future in pending:
                future.cancel()


def _events_to_take(obj: LandedObject, data: bytes, index: Index, present: PresentKeys, table: str) -> list[dict]:
    """The events of this object that Bronze lacks and whose first copy is this object, each change once."""
    try:
        events = events_in(data)
    except (OSError, EOFError, zlib.error, ValueError) as error:
        raise LandingChanged(f"{table}: {obj.key} was valid in pass 1 and cannot be read as events now "
                             f"({type(error).__name__}: {error})") from error
    kept, taken = [], set()
    for event in events:
        try:
            k = key(event)
        except (KeyError, TypeError) as error:
            raise LandingChanged(f"{table}: {obj.key} holds an event with no key now, though it had one in "
                                 "pass 1") from error
        if k in present or k in taken:
            continue
        try:
            holder = index.object_of(k)
        except KeyError as error:
            raise LandingChanged(f"{table}: {obj.key} holds the change commit_lsn {k[0]:016x} position {k[1]}, "
                                 "which pass 1 did not see") from error
        if holder.key == obj.key:
            taken.add(k)
            kept.append(event)
    return kept


def _append_missing(work: _Work, index: Index, reader: LandingReader, *, source_db: str, workers: int,
                    batch_rows: int, on_batch: Optional[Callable[[BatchCommitted], None]]) -> tuple[int, int]:
    """Pass 2 for one table. Returns (batches committed, rows appended)."""
    bronze = work.bronze
    arrow_schema = arrow_schema_of(bronze.schema())
    batch = Batch(arrow_schema, batch_rows)
    total = work.landed - len(work.present)
    done = batches = 0

    def commit() -> None:
        nonlocal done, batches
        rows = batch.take()
        bronze.append(rows)
        batches += 1
        done += rows.num_rows
        if on_batch is not None:
            on_batch(BatchCommitted(work.table, batches, rows.num_rows, done, total, _snapshot_id(bronze)))

    for obj, data in _read_ahead(reader, work.objects, workers):
        kept = _events_to_take(obj, data, index, work.present, work.table)
        if kept:
            batch.add(events_to_arrow(kept, arrow_schema, source_db=source_db, label=obj.key))
        if batch.full:
            commit()
    if batch.rows:
        commit()
    if done != total:
        raise LandingChanged(
            f"{work.table}: the index said {total:,} rows were missing from Bronze and pass 2 found {done:,} in "
            f"{len(work.objects)} objects, so the landed objects are not what pass 1 read; {batches} "
            f"{'batch was' if batches == 1 else 'batches were'} committed before this was found")
    return batches, done


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def run(reader: LandingReader, catalog: Catalog, store: Store, *, catalog_file: Path, warehouse: str, source_db: str,
        write: bool, workers: int = 8, batch_rows: int = 100_000, clock: Callable[[], datetime] = _utc_now,
        progress: Optional[Callable[[int, int], None]] = None,
        on_batch: Optional[Callable[[BatchCommitted], None]] = None) -> RunReport:
    """Bring Bronze up to date with the landing, or, with write=False, say what that would take.

    `warehouse` is the root the catalog was made with (gs://fintech_bronze, or a local directory URI): a table's
    location must be <warehouse>/bronze/<table>. `source_db` is the database every landed event must come from.
    `store` is the Bronze bucket's own objects (the run marker and the catalog copy, bronze.store) and `catalog_file` the
    local SQLite file behind `catalog`, which is what is copied. `clock` gives the UTC time a marker is named for.
    `progress(done, total)` is called by pass 1 after each object; `on_batch` after each committed batch.
    """
    if workers < 1:
        raise ConfigError(f"workers must be at least 1, not {workers}")
    if batch_rows < 1:
        raise ConfigError(f"batch_rows must be at least 1, not {batch_rows}")
    source_prefix = reader.prefix
    root = warehouse.rstrip("/")

    def location(name: str) -> str:
        return f"{root}/{NAMESPACE}/{name}"

    # 1. existing tables against the contract, before pass 1 and before any write
    existing: dict[str, Table] = {}
    for name in TABLE_ORDER:
        bronze = _load(catalog, name)
        if bronze is not None:
            check_table(name, bronze, source_prefix=source_prefix, source_db=source_db, location=location(name))
            existing[name] = bronze
    marker_before = newest_marker(store)
    if marker_before is not None:
        check_against_marker(marker_before, existing)

    # 1b. a write run with nothing new in the landing reads no object (a plan always does the full check)
    if write and marker_before is not None and _unchanged_since_marker(reader, marker_before, existing) is not None:
        return _skipped_report(marker_before, existing)

    # 2. pass 1
    index = build_index(reader, workers, progress, source_db=source_db)
    index.require_complete()
    summary = index.summary()

    # 3. what Bronze holds and what is missing, for every table, before anything is written
    works = {name: _plan(name, existing.get(name), index, sum(summary.by_table[name].values()),
                         existed=name in existing)
             for name in TABLE_ORDER}
    before = {name: _snapshot_id(work.bronze) for name, work in works.items()}
    appended: dict[str, tuple[int, int]] = {name: (0, 0) for name in TABLE_ORDER}
    current_before = marker_before is not None and describes(marker_before, before)
    written: Optional[str] = None
    copied = False

    # 5 and 6. create what is missing, then append what is missing
    if write:
        catalog.create_namespace_if_not_exists(NAMESPACE)
        for name in TABLE_ORDER:
            work = works[name]
            if work.bronze is None:
                bronze, created = _create(catalog, name, source_prefix=source_prefix, source_db=source_db,
                                          location=location(name))
                works[name] = _plan(name, bronze, index, work.landed, existed=False, created=created)
                before[name] = _snapshot_id(bronze)
        for name in TABLE_ORDER:
            work = works[name]
            if work.objects:
                appended[name] = _append_missing(work, index, reader, source_db=source_db, workers=workers,
                                                 batch_rows=batch_rows, on_batch=on_batch)

        # 7. the marker, then the catalog copy
        after = {name: _snapshot_id(works[name].bronze) for name in TABLE_ORDER}
        if marker_before is None or not describes(marker_before, after):
            marker = build_marker(
                clock(), landed_objects=summary.objects, highest_commit_lsn=index.highest_commit_lsn(),
                tables={name: TableMark(after[name], len(works[name].present) + appended[name][1],
                                        works[name].bronze.metadata_location) for name in TABLE_ORDER})
            try:
                write_marker(store, marker)
            except SinkError as error:
                raise SinkError(f"the data is committed in every table, but the run marker could not be written "
                                f"({error}). Run again to write it.") from error
            written = marker.run_id
        if written:         # a run that created a table or committed a batch has new snapshots, so it wrote a marker
            try:
                copy_catalog(catalog_file, store)
            except SinkError as error:
                raise SinkError(f"the data and the run marker are committed, but the copy of the catalog could not "
                                f"be written ({error}). The catalog copy is not needed to rebuild: that uses the "
                                "marker.") from error
            copied = True

    tables = {}
    for name, work in works.items():
        batches, rows = appended[name]
        tables[name] = TableReport(
            table=name, existed=work.existed, created=work.created, landed=work.landed,
            rows_before=len(work.present), to_append=work.landed - len(work.present),
            objects_to_read=len(work.objects), batches=batches, rows_appended=rows,
            snapshot_before=before[name], snapshot_after=_snapshot_id(work.bronze),
            metadata_location=work.bronze.metadata_location if work.bronze is not None else None)
    return RunReport(write=write, summary=summary, tables=tables,
                     marker=MarkerReport(marker_before.run_id if marker_before else None, current_before, written, copied))
