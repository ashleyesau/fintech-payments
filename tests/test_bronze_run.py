"""tests/test_bronze_run.py: G3 step 3, the run function (bronze/run.py).

Everything runs on a local warehouse (a temporary SQLite catalog and directory) over a synthetic landing in memory
(tests/bronze_synth.py), so no bucket is touched. What is held here, each with a mutation that fails it (see the
commit message):
  - a first run creates the five tables as the contract generates them and appends every landed change once, in
    TABLE_ORDER, and the rows map back to exactly the landed events (loan_applications keeps both c and u);
  - a plan writes nothing, and says what a write would do;
  - a second run creates no snapshot, creates no table and changes no file, and reads each object once;
  - duplicates, overlapping objects, an event repeated inside one object and a late object are each absorbed;
  - one commit per batch, whole objects, a little over BRONZE_BATCH_ROWS, each batch sorted by key;
  - a crash after batch k, a failed read, an interrupt and a table with no snapshot each resume with only what is
    missing;
  - both integrity stops and a table that has drifted stop the run before any write (nothing created, no snapshot
    added to any table, and for drift not one object read);
  - pass 1 stops (incomplete, wrong value, wrong columns) leave no table behind;
  - what pass 2 reads must be what pass 1 indexed (LandingChanged);
  - a table that appears while the run is creating it is loaded, checked and not appended to twice.
"""
import hashlib
import threading
import time

import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table

from bronze.batches import arrow_schema_of, events_to_arrow
from bronze.check import exit_code
from bronze.errors import (ConfigError, ContractError, DuplicateBronzeKeys, EventError, IncompleteTransactions,
                           LandingChanged, LandingError, MappingError, TableDriftError, UnknownBronzeKeys)
from bronze.landing import LandingReader
from bronze.mapping import row_to_event
from bronze.run import NAMESPACE, TABLE_ORDER, BatchCommitted, run
from bronze.schema import build_schema, table_properties
from bronze.tables import table_differences
from cdc.columns import EXPECTED_COLUMNS
from cdc.dedup import dedup, events_in
from cdc.errors import SinkError
from cdc.landing import CLOCK_FIELDS
from tests.bronze_synth import DAY, PREFIX, SOURCE_DB, Landing, encode, event
from tests.fakes import SimulatedCrash

TABLES = tuple(EXPECTED_COLUMNS)
DATES = ["2025-02-27", "2025-03-01", "2025-04-02"]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
class Env:
    """A local warehouse: a SQLite catalog, a directory, and a run() bound to them."""

    def __init__(self, path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        self.dir = path
        self.uri = f"file://{path / 'warehouse'}"
        self.catalog = SqlCatalog("bronze", uri=f"sqlite:///{path / 'catalog.db'}", warehouse=self.uri)

    def run(self, reader, **kwargs):
        kwargs.setdefault("write", True)
        kwargs.setdefault("warehouse", self.uri)
        kwargs.setdefault("source_db", SOURCE_DB)
        return run(reader, self.catalog, **kwargs)

    def table(self, name: str) -> Table:
        return self.catalog.load_table(f"{NAMESPACE}.{name}")

    def has(self, name: str) -> bool:
        return self.catalog.table_exists(f"{NAMESPACE}.{name}")

    def snapshots(self) -> dict:
        """table -> every snapshot id it has, or None where the table does not exist."""
        return {name: ([s.snapshot_id for s in self.table(name).snapshots()] if self.has(name) else None)
                for name in TABLE_ORDER}

    def rows(self, name: str) -> list[dict]:
        return self.table(name).scan().to_arrow().to_pylist()

    def keys(self, name: str) -> list:
        return sorted((r["commit_lsn"], r["transaction_position"]) for r in self.rows(name))

    def files(self) -> dict:
        warehouse = self.dir / "warehouse"
        if not warehouse.exists():
            return {}
        return {str(p): (p.stat().st_size, p.stat().st_mtime_ns) for p in sorted(warehouse.rglob("*")) if p.is_file()}

    def catalog_digest(self) -> str:
        return hashlib.sha256((self.dir / "catalog.db").read_bytes()).hexdigest()


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


def busy_landing() -> Landing:
    """Six transactions over three days and two months (each flushed alone, so every table has several objects),
    then a loan application that is disbursed (a c and a u on loan_applications) with a repayment."""
    land = Landing()
    for date in DATES * 2:
        land.transaction(("merchants", "c"), ("merchant_transactions", "c"), ("merchant_transactions", "c"),
                         ("risk_events", "c"), date=date)
        land.flush()
    land.transaction(("loan_applications", "c"), date="2025-03-01")
    land.flush()
    land.transaction(("loan_applications", "u"), ("loan_repayments", "c"), date="2025-03-01")
    land.flush()
    return land


def landed(land: Landing) -> dict:
    """table -> the unique landed events (first copy, in listing order): an oracle that does not use bronze."""
    by_table = {table: [] for table in TABLES}
    for key in land.sink.list_keys(land.prefix):
        for e in events_in(land.sink.objects[key]):
            by_table[e["source"]["table"]].append(e)
    return {table: dedup(events) for table, events in by_table.items()}


def without_clock(e: dict) -> dict:
    return {k: v for k, v in e.items() if k not in CLOCK_FIELDS}


def sort_events(events: list[dict]) -> list[dict]:
    return sorted(events, key=lambda e: (e["source"]["commit_lsn"], e["transaction"]["position"]))


def bronze_events(env: Env, name: str) -> list[dict]:
    return sort_events([row_to_event(name, row, source_db=SOURCE_DB) for row in env.rows(name)])


class Spy:
    """A source over a Landing's sink that counts and can interfere: on_read(key, n) is called before the n-th
    read (from 1) of every key, may raise, and may return replacement bytes."""

    def __init__(self, sink, on_read=None) -> None:
        self.sink, self.on_read = sink, on_read
        self.listings, self.reads, self._seen = 0, [], {}
        self._lock = threading.Lock()

    def list_keys(self, prefix):
        self.listings += 1
        return self.sink.list_keys(prefix)

    def read(self, key):
        with self._lock:
            self.reads.append(key)
            n = self._seen[key] = self._seen.get(key, 0) + 1
        data = self.sink.read(key)
        if self.on_read is not None:
            replaced = self.on_read(key, n)
            if replaced is not None:
                return replaced
        return data

    def reader(self, prefix=PREFIX) -> LandingReader:
        return LandingReader(self, prefix)


# ---------------------------------------------------------------------------
# a first run
# ---------------------------------------------------------------------------
def test_a_first_run_creates_the_five_tables_and_appends_every_change_once(env):
    land = busy_landing()
    expected = landed(land)
    report = env.run(land.reader())
    assert report.write is True and tuple(report.tables) == TABLE_ORDER
    for name in TABLE_ORDER:
        t = report.tables[name]
        assert t.created and not t.existed
        assert t.landed == t.to_append == t.rows_appended == len(expected[name]) == len(env.rows(name))
        assert t.rows_before == 0 and t.snapshot_before is None
        assert env.keys(name) == sorted(set(env.keys(name)))          # one row per key
    assert {n: report.tables[n].landed for n in TABLE_ORDER} == {
        "merchants": 6, "loan_applications": 2, "loan_repayments": 1, "risk_events": 6, "merchant_transactions": 12}
    assert report.summary.unique_events == 27


def test_bronze_maps_back_to_exactly_the_landed_events_including_both_loan_application_rows(env):
    land = busy_landing()
    env.run(land.reader())
    for name, events in landed(land).items():
        assert bronze_events(env, name) == sort_events(events), name
    ops = sorted(r["op"] for r in env.rows("loan_applications"))
    assert ops == ["c", "u"]


def test_the_tables_are_exactly_what_the_contract_generates(env):
    land = busy_landing()
    env.run(land.reader())
    for name in TABLE_ORDER:
        table = env.table(name)
        assert table_differences(name, table, source_prefix=PREFIX, source_db=SOURCE_DB,
                                 location=f"{env.uri}/{NAMESPACE}/{name}") == []
        assert table.schema() == build_schema(name)
        assert table.properties["bronze.source-prefix"] == PREFIX


def test_every_table_report_carries_its_snapshot_and_metadata_location(env):
    land = busy_landing()
    report = env.run(land.reader())
    for name, t in report.tables.items():
        table = env.table(name)
        assert t.snapshot_after == table.current_snapshot().snapshot_id
        assert t.metadata_location == table.metadata_location


def test_tables_are_written_in_the_decided_order(env):
    land = busy_landing()
    seen = []
    env.run(land.reader(), on_batch=lambda b: seen.append(b.table))
    assert TABLE_ORDER == ("merchants", "loan_applications", "loan_repayments", "risk_events", "merchant_transactions")
    assert seen == list(TABLE_ORDER)
    assert set(TABLE_ORDER) == set(TABLES) and len(TABLE_ORDER) == 5


def test_the_catalog_gets_the_namespace_and_the_five_tables(env):
    env.run(busy_landing().reader())
    assert {t[1] for t in env.catalog.list_tables(NAMESPACE)} == set(TABLES)


# ---------------------------------------------------------------------------
# a plan
# ---------------------------------------------------------------------------
def test_a_plan_on_an_empty_warehouse_writes_nothing_and_says_what_a_write_would_do(env):
    land = busy_landing()
    report = env.run(land.reader(), write=False)
    assert report.write is False
    assert not (env.dir / "warehouse").exists()
    assert env.catalog.list_namespaces() == []
    for name in TABLE_ORDER:
        t = report.tables[name]
        assert (t.existed, t.created, t.rows_before, t.batches, t.rows_appended) == (False, False, 0, 0, 0)
        assert t.to_append == t.landed > 0 and t.objects_to_read > 0
        assert t.snapshot_before is None and t.snapshot_after is None and t.metadata_location is None
    # and the plan is exactly what the write then does
    written = env.run(land.reader())
    for name in TABLE_ORDER:
        assert written.tables[name].rows_appended == report.tables[name].to_append


def test_a_plan_changes_no_file_and_no_snapshot_when_tables_exist(env):
    land = busy_landing()
    env.run(land.reader())
    land.transaction(("merchants", "c"), date="2025-05-05")
    land.flush()
    files, snapshots, digest = env.files(), env.snapshots(), env.catalog_digest()
    report = env.run(land.reader(), write=False)
    assert (env.files(), env.snapshots(), env.catalog_digest()) == (files, snapshots, digest)
    assert report.tables["merchants"].to_append == 1 and report.tables["merchants"].objects_to_read == 1
    assert report.tables["merchants"].rows_before == 6
    assert all(report.tables[n].to_append == 0 for n in TABLE_ORDER if n != "merchants")
    assert all(report.tables[n].existed and not report.tables[n].created for n in TABLE_ORDER)


def test_a_plan_with_nothing_missing_says_so(env):
    land = busy_landing()
    env.run(land.reader())
    report = env.run(land.reader(), write=False)
    assert all(t.to_append == 0 and t.objects_to_read == 0 and t.rows_before == t.landed for t in report.tables.values())


# ---------------------------------------------------------------------------
# a second run
# ---------------------------------------------------------------------------
def test_a_second_run_creates_no_snapshot_no_table_and_changes_no_file(env):
    land = busy_landing()
    env.run(land.reader())
    snapshots, files, digest = env.snapshots(), env.files(), env.catalog_digest()
    again = env.run(land.reader())
    assert env.snapshots() == snapshots
    assert env.files() == files
    assert env.catalog_digest() == digest
    for name in TABLE_ORDER:
        t = again.tables[name]
        assert t.existed and not t.created
        assert (t.batches, t.rows_appended, t.objects_to_read, t.to_append) == (0, 0, 0, 0)
        assert t.rows_before == t.landed
        assert t.snapshot_before == t.snapshot_after == snapshots[name][-1]


def test_a_second_run_reads_each_object_once_and_a_first_run_twice(env):
    land = busy_landing()
    spy = Spy(land.sink)
    objects = len(land.sink.objects)
    env.run(spy.reader())
    assert spy.listings == 1 and len(spy.reads) == 2 * objects
    spy.reads.clear()
    env.run(spy.reader())
    assert spy.listings == 2 and len(spy.reads) == objects


def test_a_plan_reads_every_object_once(env):
    land = busy_landing()
    spy = Spy(land.sink)
    env.run(spy.reader(), write=False)
    assert len(spy.reads) == len(land.sink.objects) and len(set(spy.reads)) == len(spy.reads)


# ---------------------------------------------------------------------------
# duplicates, overlap, late objects
# ---------------------------------------------------------------------------
def test_a_replay_under_new_overlapping_names_adds_no_second_row(env):
    land = busy_landing()
    land.transaction(("merchants", "c"), ("risk_events", "c"), date="2025-05-05")
    land.flush(land.txs, clock=9)     # every transaction again, with another consumer clock, under new names
    assert len(land.sink.objects) > 21 + 2
    expected = landed(land)
    env.run(land.reader())
    for name in TABLE_ORDER:
        assert env.keys(name) == sorted(set(env.keys(name))) and len(env.rows(name)) == len(expected[name])
        # the copy taken is the first in the listing, so the clocks in Bronze are that copy's
        assert [without_clock(e) for e in bronze_events(env, name)] == \
               [without_clock(e) for e in sort_events(expected[name])]


def test_a_replay_under_the_same_names_changes_nothing(env):
    land = busy_landing()
    objects = dict(land.sink.objects)
    for tx in land.txs:               # each transaction cut again as it was the first time, with another clock
        land.flush([tx], clock=5)
    assert land.sink.objects == objects
    env.run(land.reader())
    snapshots = env.snapshots()
    for tx in land.txs:
        land.flush([tx], clock=6)
    env.run(land.reader())
    assert env.snapshots() == snapshots


def test_an_object_that_arrives_late_adds_only_its_own_rows(env):
    land = busy_landing()
    env.run(land.reader())
    snapshots, rows = env.snapshots(), {n: len(env.rows(n)) for n in TABLE_ORDER}
    land.transaction(("merchants", "c"), ("merchant_transactions", "c"), date="2025-05-05")
    new = land.flush()
    spy = Spy(land.sink)
    report = env.run(spy.reader())
    assert len(env.rows("merchants")) == rows["merchants"] + 1
    assert len(env.rows("merchant_transactions")) == rows["merchant_transactions"] + 1
    for name in ("loan_applications", "loan_repayments", "risk_events"):
        assert env.snapshots()[name] == snapshots[name] and len(env.rows(name)) == rows[name]
    for name in ("merchants", "merchant_transactions"):
        assert len(env.snapshots()[name]) == len(snapshots[name]) + 1
        assert report.tables[name].rows_appended == 1 and report.tables[name].objects_to_read == 1
    # pass 1 read everything, pass 2 only the two new objects
    assert len(spy.reads) == len(land.sink.objects) + len(new) == len(land.sink.objects) + 2
    assert [k for k in spy.reads if k in new].count(new[0]) == 2


def test_an_event_repeated_inside_one_object_is_appended_once(env):
    land = Landing()
    e = event("merchants", "c", 0x2B32560, 0, 1)
    land.put("merchants", DAY, [e, dict(e)])
    report = env.run(land.reader())
    assert report.summary.duplicates == 1
    assert len(env.rows("merchants")) == 1 and report.tables["merchants"].rows_appended == 1


def test_the_first_copy_in_the_listing_is_the_one_taken(env):
    land = Landing()
    first = event("merchants", "c", 0x2B32560, 0, 1, clock=1)
    later = event("merchants", "c", 0x2B32560, 0, 1, clock=2)
    land.put("merchants", DAY, [first], lsns=(0x2B32560, 0x2B32560))
    land.put("merchants", DAY, [later], lsns=(0x2B32550, 0x2B32560))     # a different name that sorts earlier
    keys = land.reader().list_keys()
    assert keys[0].last_lsn == 0x2B32560 and keys[0].first_lsn == 0x2B32550
    env.run(land.reader())
    (row,) = env.rows("merchants")
    assert row["ts_ms"] == later["ts_ms"]


# ---------------------------------------------------------------------------
# batches and commits
# ---------------------------------------------------------------------------
def spread_landing(objects=7) -> Landing:
    """One table, `objects` objects of two rows each, one transaction apiece."""
    land = Landing()
    for i in range(objects):
        land.transaction(("merchant_transactions", "c"), ("merchant_transactions", "c"), date=DATES[i % 3])
        land.flush()
    return land


def test_one_commit_per_batch_and_a_snapshot_for_each(env):
    land = spread_landing(7)
    batches = []
    report = env.run(land.reader(), batch_rows=1, on_batch=batches.append)
    t = report.tables["merchant_transactions"]
    assert t.batches == len(batches) == 7 and t.rows_appended == 14
    assert len(env.snapshots()["merchant_transactions"]) == 7


def test_a_batch_is_whole_objects_and_a_little_over_the_minimum(env):
    batches = []
    env.run(spread_landing(7).reader(), batch_rows=3, on_batch=batches.append)
    assert [b.rows for b in batches] == [4, 4, 4, 2]          # two rows an object: 3 rounds up to 4, the rest is last
    assert [b.batch for b in batches] == [1, 2, 3, 4]
    assert [b.rows_done for b in batches] == [4, 8, 12, 14] and {b.rows_total for b in batches} == {14}


def test_a_big_enough_minimum_makes_one_batch_for_the_table(env):
    batches = []
    env.run(spread_landing(7).reader(), batch_rows=1000, on_batch=batches.append)
    assert [(b.batch, b.rows) for b in batches] == [(1, 14)]


def test_each_batch_is_one_append_of_a_table_sorted_by_key_in_the_tables_schema(env, monkeypatch):
    # LSN order and listing order disagree (the listing sorts by date first), so the sort has work to do
    land = Landing()
    land.transaction(("merchant_transactions", "c"), ("merchant_transactions", "c"), date="2025-04-02")
    land.flush()
    land.transaction(("merchant_transactions", "c"), ("merchant_transactions", "c"), date="2025-03-01")
    land.flush()
    names = [o.key for o in land.reader().list_keys()]
    assert "2025-03-01" in names[0]                      # the higher LSN is listed first
    appended = []
    original = Table.append

    def spy(self, df, *args, **kwargs):
        appended.append((self.name(), df))
        return original(self, df, *args, **kwargs)

    monkeypatch.setattr(Table, "append", spy)
    env.run(land.reader(), batch_rows=1000)
    assert len(appended) == 1
    name, table = appended[0]
    assert name[-1] == "merchant_transactions"
    pairs = list(zip(table.column("commit_lsn").to_pylist(), table.column("transaction_position").to_pylist()))
    assert pairs == sorted(pairs) and len(pairs) == 4 and len(set(pairs)) == 4
    assert table.schema.equals(arrow_schema_of(build_schema("merchant_transactions")), check_metadata=True)


def test_the_progress_report_of_a_batch_matches_the_table_at_that_moment(env):
    seen = []

    def on_batch(batch):
        seen.append((batch, env.table(batch.table).current_snapshot().snapshot_id, len(env.rows(batch.table))))

    env.run(spread_landing(5).reader(), batch_rows=3, on_batch=on_batch)
    assert len(seen) == 3
    for batch, snapshot, rows_in_table in seen:
        assert isinstance(batch, BatchCommitted) and batch.snapshot_id == snapshot
        assert rows_in_table == batch.rows_done and batch.table == "merchant_transactions"


def test_pass_1_progress_is_passed_through(env):
    land = busy_landing()
    seen = []
    env.run(land.reader(), progress=lambda done, total: seen.append((done, total)))
    assert seen[-1] == (len(land.sink.objects),) * 2 and len(seen) == len(land.sink.objects)


@pytest.mark.parametrize("workers", [1, 8])
def test_the_result_does_not_depend_on_the_number_of_workers(env, workers):
    land = busy_landing()
    env.run(land.reader(), workers=workers, batch_rows=3)
    for name, events in landed(land).items():
        assert bronze_events(env, name) == sort_events(events)


def test_reads_run_ahead_but_never_more_than_two_per_worker(env):
    land = spread_landing(12)
    spy = Spy(land.sink)
    objects, window = len(land.sink.objects), 4
    at_commit = []

    def on_batch(batch):
        want = min(objects, batch.batch + window - 1)
        stop = time.monotonic() + 3
        while len(spy.reads) - objects < want and time.monotonic() < stop:
            time.sleep(0.01)              # the worker threads start their reads a moment after the submit
        at_commit.append((batch.batch, len(spy.reads) - objects))

    env.run(spy.reader(), workers=2, batch_rows=1, on_batch=on_batch)
    assert len(at_commit) == 12
    for k, pass_2_reads in at_commit:
        assert pass_2_reads == min(12, k + window - 1), (k, pass_2_reads)


# ---------------------------------------------------------------------------
# crashes
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("table,crash_after", [("merchant_transactions", 2), ("merchants", 1), ("risk_events", 3)])
def test_a_crash_after_batch_k_resumes_with_only_the_objects_still_missing(env, table, crash_after):
    land = busy_landing()
    for i in range(3):    # more objects for every table, so every table has several batches
        land.transaction(("merchants", "c"), ("merchant_transactions", "c"), ("risk_events", "c"), date=DATES[i])
        land.flush()

    def crash(batch):
        if batch.table == table and batch.batch == crash_after:
            raise SimulatedCrash(f"after batch {crash_after} of {table}")

    with pytest.raises(SimulatedCrash):
        env.run(land.reader(), batch_rows=1, on_batch=crash)
    done = len(env.rows(table))
    assert done > 0
    total = len(landed(land)[table])
    spy = Spy(land.sink)
    report = env.run(spy.reader(), batch_rows=1)
    t = report.tables[table]
    assert t.rows_before == done and t.rows_appended == total - done > 0
    assert len(env.rows(table)) == total and len(env.keys(table)) == len(set(env.keys(table)))
    pass_2_reads = len(spy.reads) - len(land.sink.objects)
    assert pass_2_reads == sum(r.objects_to_read for r in report.tables.values())
    in_table = sum(1 for k in land.sink.objects if f"/{table}/" in k)
    assert t.objects_to_read == in_table - crash_after   # the objects of batches 1..k are done, the rest are not
    for name, events in landed(land).items():
        assert bronze_events(env, name) == sort_events(events), name


def test_a_failed_read_in_pass_2_keeps_the_batches_before_it_and_resumes(env):
    land = spread_landing(6)
    objects = len(land.sink.objects)
    state = {"n": 0}

    def fail(key, n):
        state["n"] += 1
        if state["n"] == objects + 4:        # the fourth read of pass 2
            raise SinkError("injected: the bucket gave up")

    batches = []
    with pytest.raises(SinkError):
        env.run(Spy(land.sink, fail).reader(), workers=1, batch_rows=1, on_batch=batches.append)
    assert 0 < len(env.rows("merchant_transactions")) < 12
    assert len(batches) == len(env.snapshots()["merchant_transactions"])
    env.run(land.reader(), batch_rows=1)
    assert len(env.rows("merchant_transactions")) == 12 and len(set(env.keys("merchant_transactions"))) == 12


def test_a_crash_before_the_first_commit_leaves_tables_with_no_snapshot_and_the_rerun_fills_them(env):
    land = busy_landing()
    objects = len(land.sink.objects)

    def fail(key, n):
        if n == 2:
            raise SinkError("injected")

    with pytest.raises(SinkError):
        env.run(Spy(land.sink, fail).reader(), workers=1)
    assert all(env.has(n) for n in TABLE_ORDER)
    assert all(env.table(n).current_snapshot() is None for n in TABLE_ORDER)
    report = env.run(land.reader())
    assert all(report.tables[n].existed and not report.tables[n].created for n in TABLE_ORDER)
    for name, events in landed(land).items():
        assert bronze_events(env, name) == sort_events(events)


def test_an_interrupt_propagates_and_the_rerun_completes(env):
    land = spread_landing(5)

    def interrupt(batch):
        if batch.batch == 2:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        env.run(land.reader(), batch_rows=1, on_batch=interrupt)
    assert len(env.rows("merchant_transactions")) == 4
    env.run(land.reader(), batch_rows=1)
    assert len(env.rows("merchant_transactions")) == 10


def test_a_table_dropped_from_the_catalog_is_created_again_and_the_others_are_left_alone(env):
    land = busy_landing()
    env.run(land.reader())
    snapshots = env.snapshots()
    env.catalog.drop_table(f"{NAMESPACE}.risk_events")
    report = env.run(land.reader())
    assert report.tables["risk_events"].created and report.tables["risk_events"].rows_appended == 6
    assert len(env.rows("risk_events")) == 6
    for name in TABLE_ORDER:
        if name != "risk_events":
            assert env.snapshots()[name] == snapshots[name]
            assert report.tables[name].existed


# ---------------------------------------------------------------------------
# integrity stops and drift: before any write
# ---------------------------------------------------------------------------
def append_event(env, name, e):
    table = env.table(name)
    table.append(events_to_arrow([e], arrow_schema_of(table.schema()), source_db=SOURCE_DB))


def snapshots_unchanged_after(env, land, error, **kwargs):
    """Run, expecting `error`; nothing may be appended to any table by the failing run."""
    before, files = env.snapshots(), env.files()
    with pytest.raises(error) as stop:
        env.run(land.reader(), **kwargs)
    assert env.snapshots() == before and env.files() == files
    return stop.value


def test_a_key_held_twice_in_bronze_stops_the_run_before_any_table_is_written(env):
    land = busy_landing()
    env.run(land.reader())
    land.transaction(("merchants", "c"), date="2025-05-05")      # a late object for the FIRST table in the order
    land.flush()
    duplicate = landed(land)["merchant_transactions"][0]            # a second copy in the LAST table
    append_event(env, "merchant_transactions", duplicate)
    stop = snapshots_unchanged_after(env, land, DuplicateBronzeKeys)
    assert "merchant_transactions" in str(stop)
    assert len(env.rows("merchants")) == 6                          # the late merchants row was not written


def test_a_key_the_landing_does_not_hold_stops_the_run_before_any_table_is_written(env):
    land = busy_landing()
    env.run(land.reader())
    stray = event("merchant_transactions", "c", 0x7FFFFFF0, 0, 1)
    append_event(env, "merchant_transactions", stray)                # Bronze holds a key the landing never had
    land.transaction(("merchants", "c"), date="2025-05-05")
    land.flush()
    stop = snapshots_unchanged_after(env, land, UnknownBronzeKeys)
    assert "merchant_transactions" in str(stop)
    assert len(env.rows("merchants")) == 6


def test_a_landing_that_lost_an_object_is_refused_not_repaired(env):
    land = busy_landing()
    env.run(land.reader())
    victim = next(k for k in land.sink.objects if "/merchants/" in k)
    del land.sink.objects[victim]
    # the landing no longer holds a whole transaction AND Bronze holds keys it lacks, in the first table of the
    # order: the landing is judged first, before Bronze's keys are read
    snapshots_unchanged_after(env, land, IncompleteTransactions)


@pytest.mark.parametrize("what", ["another_prefix", "another_database", "another_warehouse", "a_new_column"])
def test_a_table_that_has_drifted_stops_the_run_before_a_single_object_is_read(env, what):
    land = busy_landing()
    env.run(land.reader())
    kwargs = {}
    prefix = PREFIX
    if what == "another_prefix":
        prefix = "cdc/cdc_r02/"
        other = Landing(prefix)
        other.transaction(("merchants", "c"))
        other.flush()
        source = other.sink
    else:
        source = land.sink
    if what == "another_database":
        kwargs["source_db"] = "fintech_other"
    if what == "another_warehouse":
        kwargs["warehouse"] = "file:///somewhere/else"
    if what == "a_new_column":
        from pyiceberg.types import StringType
        with env.table("risk_events").update_schema() as change:
            change.add_column("surprise", StringType())
    spy = Spy(source)
    before = env.snapshots()
    with pytest.raises(TableDriftError):
        env.run(spy.reader(prefix), **kwargs)
    assert spy.listings == 0 and spy.reads == []
    assert env.snapshots() == before


def test_an_incomplete_landing_leaves_no_table_behind(env):
    land = Landing()
    e = event("merchants", "c", 0x2B32560, 0, 2)               # position 1 of this transaction never landed
    land.put("merchants", DAY, [e])
    with pytest.raises(IncompleteTransactions):
        env.run(land.reader())
    assert env.catalog.list_namespaces() == [] and not (env.dir / "warehouse").exists()


def bad_value():
    land = Landing()
    e = event("merchants", "c", 0x2B32560, 0, 1)
    e["after"]["onboarded_at"] = "2025-03-01 10:00:00"
    land.put("merchants", DAY, [e])
    return land, MappingError


def bad_columns():
    land = Landing()
    e = event("merchants", "c", 0x2B32560, 0, 1)
    e["after"]["surprise"] = "x"
    land.put("merchants", DAY, [e])
    return land, EventError


def bad_version():
    land = Landing()
    e = event("merchants", "c", 0x2B32560, 0, 1)
    e["envelope_version"] = 2
    land.put("merchants", DAY, [e])
    return land, EventError


@pytest.mark.parametrize("make", [bad_value, bad_columns, bad_version])
def test_a_pass_1_stop_creates_nothing(env, make):
    land, error = make()
    with pytest.raises(error):
        env.run(land.reader())
    assert env.catalog.list_namespaces() == [] and not (env.dir / "warehouse").exists()


def test_a_pass_1_stop_adds_nothing_to_tables_that_exist(env):
    land = busy_landing()
    env.run(land.reader())
    bad = event("merchants", "c", 0x7FFFFF00, 0, 1, date="2025-05-05")
    bad["after"]["onboarded_at"] = "not a time"
    land.put("merchants", "2025-05-05", [bad])
    land.transaction(("risk_events", "c"), date="2025-05-05")
    land.flush()
    snapshots_unchanged_after(env, land, MappingError)


# ---------------------------------------------------------------------------
# the landing changing under the run
# ---------------------------------------------------------------------------
def spread_target(land):
    return next(k for k in sorted(land.sink.objects) if "/merchant_transactions/" in k)


def test_an_object_that_gives_fewer_events_in_pass_2_than_in_pass_1_is_refused(env):
    land = spread_landing(3)
    victim = spread_target(land)
    kept = events_in(land.sink.objects[victim])[:1]

    def alter(key, n):
        return encode(kept) if key == victim and n == 2 else None

    with pytest.raises(LandingChanged) as stop:
        env.run(Spy(land.sink, alter).reader(), batch_rows=100)
    assert "merchant_transactions" in str(stop.value)


def test_an_object_holding_a_change_pass_1_never_saw_is_refused(env):
    land = spread_landing(3)
    victim = spread_target(land)
    extra = event("merchant_transactions", "c", int(victim.split("lsn_")[1][:16], 16), 5, 9)

    def alter(key, n):
        return encode(events_in(land.sink.objects[victim]) + [extra]) if key == victim and n == 2 else None

    with pytest.raises(LandingChanged) as stop:
        env.run(Spy(land.sink, alter).reader())
    assert victim in str(stop.value)


def test_an_object_that_is_no_longer_events_in_pass_2_is_refused(env):
    land = spread_landing(3)
    victim = spread_target(land)

    def alter(key, n):
        return b"not gzip at all" if key == victim and n == 2 else None

    with pytest.raises(LandingChanged) as stop:
        env.run(Spy(land.sink, alter).reader())
    assert victim in str(stop.value)


def test_an_event_with_no_key_in_pass_2_is_refused(env):
    land = spread_landing(3)
    victim = spread_target(land)
    broken = [{"op": "c"}]

    def alter(key, n):
        return encode(broken) if key == victim and n == 2 else None

    with pytest.raises(LandingChanged) as stop:
        env.run(Spy(land.sink, alter).reader())
    assert victim in str(stop.value)


def test_a_landing_change_is_a_contract_stop_that_says_how_many_batches_were_committed(env):
    land = spread_landing(4)
    victim = sorted(k for k in land.sink.objects if "/merchant_transactions/" in k)[-1]
    kept = events_in(land.sink.objects[victim])[:1]

    def alter(key, n):
        return encode(kept) if key == victim and n == 2 else None

    with pytest.raises(LandingChanged) as stop:
        env.run(Spy(land.sink, alter).reader(), workers=1, batch_rows=1)
    assert isinstance(stop.value, ContractError) and not isinstance(stop.value, LandingError)
    assert exit_code(stop.value) == 2                         # a contract stop, as the command line will report it
    assert "4 batches were committed" in str(stop.value)       # found at the end of the table, after the last commit
    assert len(env.rows("merchant_transactions")) == 7


# ---------------------------------------------------------------------------
# a table that turns up while the run is creating it
# ---------------------------------------------------------------------------
class RacingCatalog:
    """A catalog in which another writer creates `target` (with `rows` events appended) just before our
    create_table call for it, so ours finds it already there."""

    def __init__(self, inner, target, populate, properties=None) -> None:
        self.inner, self.target, self.populate, self.properties, self.raced = inner, target, populate, properties, False

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def create_table(self, identifier, **kwargs):
        if identifier == f"{NAMESPACE}.{self.target}" and not self.raced:
            self.raced = True
            theirs = dict(kwargs)
            if self.properties is not None:
                theirs["properties"] = self.properties
            table = self.inner.create_table(identifier, **theirs)
            self.populate(table)
        return self.inner.create_table(identifier, **kwargs)


@pytest.mark.parametrize("target,other", [("merchants", "risk_events"), ("risk_events", "merchants")])
def test_a_table_created_by_someone_else_in_the_meantime_is_checked_and_not_appended_twice(env, target, other):
    land = busy_landing()
    theirs = landed(land)[target]

    def populate(table):
        table.append(events_to_arrow(theirs, arrow_schema_of(table.schema()), source_db=SOURCE_DB))

    catalog = RacingCatalog(env.catalog, target, populate)
    report = run(land.reader(), catalog, warehouse=env.uri, source_db=SOURCE_DB, write=True)
    t = report.tables[target]
    assert not t.created and not t.existed
    assert t.rows_before == 6 and t.rows_appended == 0 and t.objects_to_read == 0
    assert len(env.rows(target)) == 6 and len(set(env.keys(target))) == 6
    assert report.tables[other].created and len(env.rows(other)) == 6


@pytest.mark.parametrize("target", ["merchants", "risk_events"])
def test_a_table_created_by_someone_else_with_other_properties_is_a_drift_stop(env, target):
    land = busy_landing()
    catalog = RacingCatalog(env.catalog, target, lambda table: None,
                            properties=table_properties("cdc/cdc_r02/", SOURCE_DB))
    with pytest.raises(TableDriftError):
        run(land.reader(), catalog, warehouse=env.uri, source_db=SOURCE_DB, write=True)
    assert env.table(target).current_snapshot() is None
    assert all(env.table(n).current_snapshot() is None for n in TABLE_ORDER if env.has(n))


# ---------------------------------------------------------------------------
# odds and ends
# ---------------------------------------------------------------------------
def test_an_empty_landing_creates_five_empty_tables_and_a_second_run_changes_nothing(env):
    land = Landing()
    report = env.run(land.reader())
    assert all(t.created and t.landed == 0 and t.batches == 0 and t.snapshot_after is None
               for t in report.tables.values())
    assert all(env.table(n).current_snapshot() is None for n in TABLE_ORDER)
    files, digest = env.files(), env.catalog_digest()
    env.run(land.reader())
    assert (env.files(), env.catalog_digest()) == (files, digest)


def test_a_warehouse_root_with_a_trailing_slash_is_the_same_warehouse(env):
    land = busy_landing()
    env.run(land.reader())
    again = env.run(land.reader(), warehouse=env.uri + "/")
    assert all(t.existed for t in again.tables.values())


@pytest.mark.parametrize("kwargs", [{"workers": 0}, {"batch_rows": 0}, {"workers": -1}, {"batch_rows": -5}])
def test_workers_and_batch_rows_must_be_positive(env, kwargs):
    with pytest.raises(ConfigError):
        env.run(busy_landing().reader(), **kwargs)
    assert env.catalog.list_namespaces() == []


def test_the_load_helper_treats_only_a_missing_table_as_missing(env):
    from bronze.run import _load
    assert _load(env.catalog, "merchants") is None
    with pytest.raises(NoSuchTableError):
        env.catalog.load_table(f"{NAMESPACE}.merchants")


class Concurrency(Spy):
    """A Spy that records, for each read in the order they start, how many reads are running (each takes a few
    milliseconds). Pass 1 finishes before pass 2 starts, so the first N entries are pass 1 and the rest pass 2."""

    def __init__(self, sink) -> None:
        super().__init__(sink, self._slow)
        self.now, self.levels = 0, []
        self._count = threading.Lock()

    def _slow(self, key, n):
        with self._count:
            self.now += 1
            self.levels.append(self.now)
        time.sleep(0.02)
        with self._count:
            self.now -= 1


def test_the_number_of_workers_limits_the_reads_of_both_passes(env):
    land = busy_landing()
    objects = len(land.sink.objects)
    one = Concurrency(land.sink)
    env.run(one.reader(), workers=1)
    assert len(one.levels) == 2 * objects and max(one.levels) == 1
    many = Concurrency(land.sink)
    Env(env.dir / "other").run(many.reader(), workers=4)
    assert len(many.levels) == 2 * objects
    for phase in (many.levels[:objects], many.levels[objects:]):
        assert 1 < max(phase) <= 4


def test_pass_1_holds_the_events_to_the_source_database_it_was_given(env):
    land = busy_landing()
    with pytest.raises(MappingError):
        env.run(land.reader(), source_db="fintech_other")
    assert env.catalog.list_namespaces() == []


def test_an_overlapping_object_listed_before_the_old_ones_does_not_append_what_bronze_holds(env):
    land = busy_landing()
    env.run(land.reader())
    old = next(e for e in landed(land)["merchants"])
    again = event("merchants", "c", old["source"]["commit_lsn"], 0, old["transaction"]["size"],
                  date=old["event_date"], clock=50, serial=0)
    new = event("merchants", "c", 0x7FFFFF00, 0, 1, date=old["event_date"], serial=900)
    low = old["source"]["commit_lsn"] - 0x10
    listed = land.put("merchants", old["event_date"], [again, new], lsns=(low, 0x7FFFFF00))
    that_day = [o.key for o in land.reader().list_keys() if f"/merchants/event_date={old['event_date']}/" in o.key]
    assert that_day[0] == listed and len(that_day) > 1          # it is listed before the objects Bronze came from
    report = env.run(land.reader())
    assert report.tables["merchants"].rows_appended == 1
    assert len(env.rows("merchants")) == 7 and len(set(env.keys("merchants"))) == 7


def test_a_wrong_value_in_pass_2_names_the_object_it_came_from(env):
    land = spread_landing(3)
    victim = spread_target(land)
    events = events_in(land.sink.objects[victim])
    events[0]["after"]["created_at"] = "yesterday"

    def alter(key, n):
        return encode(events) if key == victim and n == 2 else None

    with pytest.raises(MappingError) as stop:
        env.run(Spy(land.sink, alter).reader())
    assert str(stop.value).startswith(victim)


def test_a_filter_that_let_a_change_through_twice_would_stop_the_run(env, monkeypatch):
    import bronze.run as module
    real = module._events_to_take
    monkeypatch.setattr(module, "_events_to_take", lambda *args: real(*args) * 2)
    with pytest.raises(LandingChanged) as stop:
        env.run(spread_landing(2).reader())
    assert "4 rows were missing" in str(stop.value) and "found 8" in str(stop.value)


class Watching:
    """A catalog stand-in that remembers every call made on it."""

    def __init__(self, inner, fail=None) -> None:
        self.inner, self.calls, self.fail = inner, [], fail

    def __getattr__(self, name):
        target = getattr(self.inner, name)
        if not callable(target):
            return target

        def call(*args, **kwargs):
            self.calls.append(name)
            if self.fail and name == self.fail[0]:
                raise self.fail[1]
            return target(*args, **kwargs)
        return call


@pytest.mark.parametrize("kwargs", [{"workers": 0}, {"batch_rows": 0}])
def test_a_bad_setting_is_refused_before_the_catalog_or_the_landing_is_touched(env, kwargs):
    land = busy_landing()
    spy, catalog = Spy(land.sink), Watching(env.catalog)
    with pytest.raises(ConfigError):
        run(spy.reader(), catalog, warehouse=env.uri, source_db=SOURCE_DB, write=True, **kwargs)
    assert catalog.calls == [] and spy.listings == 0


def test_a_catalog_error_other_than_no_such_table_stops_the_run(env):
    land = busy_landing()
    spy = Spy(land.sink)
    catalog = Watching(env.catalog, fail=("load_table", OSError("the metadata file cannot be read")))
    with pytest.raises(OSError):
        run(spy.reader(), catalog, warehouse=env.uri, source_db=SOURCE_DB, write=True)
    assert spy.listings == 0 and env.catalog.list_namespaces() == []
