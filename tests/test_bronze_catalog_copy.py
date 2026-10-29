"""tests/test_bronze_catalog_copy.py: G3 step 4, the catalog copy and the rebuild (bronze/catalog_copy.py).

What is held here, each with a mutation that fails it (see the commit message):
  - the copy is a consistent SQLite file, taken without changing the catalog file and while PyIceberg has it open, and
    it replaces the bucket's copy;
  - a lost catalog is rebuilt from the newest marker: the exact metadata files it names are registered (not the newest
    by name), each table is checked against the marker's snapshot and row count, and a mismatch leaves no table
    registered and deletes no file;
  - a rebuild needs a marker and an empty catalog;
  - after a rebuild a normal run changes nothing; after a rebuild from a marker older than the tables it appends only
    what is missing and the tables hold each change once.
"""
import json
import sqlite3

import pytest

from bronze.catalog_copy import catalog_bytes, copy_catalog, rebuild_catalog
from bronze.errors import ConfigError, MarkerError
from bronze.marker import TABLES, encode, newest_marker
from bronze.run import NAMESPACE, TABLE_ORDER
from bronze.store import CATALOG_KEY
from cdc.errors import SinkError
from tests.bronze_env import Env, Spy, bronze_events, busy_landing, landed, sort_events
from tests.bronze_synth import Landing
from tests.fakes import MemorySink, SimulatedCrash


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


def rows_of(data: bytes, tmp_path, name="copy.db"):
    path = tmp_path / name
    path.write_bytes(data)
    connection = sqlite3.connect(path)
    try:
        return sorted(connection.execute(
            "select table_namespace, table_name, metadata_location from iceberg_tables").fetchall())
    finally:
        connection.close()


def locations(env):
    return sorted((NAMESPACE, name, env.table(name).metadata_location) for name in TABLE_ORDER)


# ---------------------------------------------------------------------------
# the copy
# ---------------------------------------------------------------------------

def test_the_copy_is_a_sqlite_file_with_every_tables_current_metadata_location(env, tmp_path):
    env.run(busy_landing().reader())
    assert rows_of(catalog_bytes(env.catalog_file), tmp_path) == locations(env)


def test_taking_the_copy_changes_nothing_and_leaves_no_file_beside_the_catalog(env):
    env.run(busy_landing().reader())
    before = env.catalog_digest()
    names = sorted(p.name for p in env.dir.iterdir())
    catalog_bytes(env.catalog_file)
    assert env.catalog_digest() == before and sorted(p.name for p in env.dir.iterdir()) == names


def test_the_copy_is_consistent_while_another_connection_is_writing_to_the_catalog(env, tmp_path):
    env.run(busy_landing().reader())
    writer = sqlite3.connect(env.catalog_file, timeout=5, isolation_level=None)
    writer.execute("BEGIN IMMEDIATE")
    try:
        rows = rows_of(catalog_bytes(env.catalog_file), tmp_path)
    finally:
        writer.execute("ROLLBACK")
        writer.close()
    assert rows == locations(env)


def test_the_copy_is_a_whole_database_that_passes_sqlites_own_check(env, tmp_path):
    env.run(busy_landing().reader())
    path = tmp_path / "check.db"
    path.write_bytes(catalog_bytes(env.catalog_file))
    connection = sqlite3.connect(path)
    try:
        assert connection.execute("pragma integrity_check").fetchall() == [("ok",)]
    finally:
        connection.close()


def test_a_catalog_file_that_is_not_there_is_a_config_error_that_names_it_and_is_not_created(tmp_path):
    with pytest.raises(ConfigError) as stop:
        catalog_bytes(tmp_path / "nowhere.db")
    assert "nowhere.db" in str(stop.value) and "nothing to copy" in str(stop.value)
    assert list(tmp_path.iterdir()) == []


def test_the_copy_includes_what_a_wal_catalog_has_committed_but_not_yet_checkpointed(env, tmp_path):
    # Why the backup API and not a read of the file: in WAL mode a committed change can sit in the -wal file, and the
    # main file alone does not have it. (The default journal mode does not do this; the copy must not depend on that.)
    env.run(busy_landing().reader())
    writer = sqlite3.connect(env.catalog_file, isolation_level=None)
    writer.execute("pragma journal_mode=wal")
    writer.execute("pragma wal_autocheckpoint=0")
    try:
        writer.execute("insert into iceberg_namespace_properties values ('bronze', 'bronze', 'marker_test', 'x')")
        names = {r[0] for r in sqlite3.connect(env.catalog_file).execute("select property_key from iceberg_namespace_properties")}
        assert "marker_test" in names                              # the writer's commit is real
        assert b"marker_test" not in env.catalog_file.read_bytes()    # and the main file alone does not have it
        copy = tmp_path / "wal_copy.db"
        copy.write_bytes(catalog_bytes(env.catalog_file))
        copied = {r[0] for r in sqlite3.connect(copy).execute("select property_key from iceberg_namespace_properties")}
        assert "marker_test" in copied
    finally:
        writer.close()


def test_the_copy_replaces_the_bucket_object_each_time(env, tmp_path):
    land = busy_landing()
    env.run(land.reader())
    first = env.store.objects[CATALOG_KEY]
    land.transaction(("merchants", "c"), date="2025-05-05")
    land.flush()
    env.run(land.reader())
    second = env.store.objects[CATALOG_KEY]
    assert first != second and rows_of(second, tmp_path) == locations(env)
    assert env.store.overwrites == 2


def test_copy_catalog_writes_the_one_key_the_store_allows(env):
    env.run(busy_landing().reader())
    store = MemorySink()
    copy_catalog(env.catalog_file, store)
    assert list(store.objects) == [CATALOG_KEY] and store.overwrites == 1


def test_a_copy_that_fails_is_the_stores_error(env):
    env.run(busy_landing().reader())
    store = MemorySink()
    store.fail_overwrite = lambda n, key: True
    with pytest.raises(SinkError):
        copy_catalog(env.catalog_file, store)


# ---------------------------------------------------------------------------
# the rebuild
# ---------------------------------------------------------------------------

def test_a_lost_catalog_is_rebuilt_from_the_newest_marker(env):
    land = busy_landing()
    env.run(land.reader())
    marker = newest_marker(env.store)
    lost = env.lose_catalog()
    assert rebuild_catalog(lost.catalog, lost.store) == marker
    for name in TABLE_ORDER:
        assert lost.table(name).metadata_location == marker.tables[name].metadata_location == env.table(name).metadata_location
        assert lost.table(name).current_snapshot().snapshot_id == marker.tables[name].snapshot_id
        assert len(lost.rows(name)) == marker.tables[name].rows
    for name, events in landed(land).items():
        assert bronze_events(lost, name) == sort_events(events)


def test_after_a_rebuild_a_normal_run_changes_nothing(env):
    land = busy_landing()
    env.run(land.reader())
    store_before = dict(env.store.objects)
    lost = env.lose_catalog()
    rebuild_catalog(lost.catalog, lost.store)
    files, snapshots = lost.files(), lost.snapshots()
    report = lost.run(land.reader())
    assert lost.files() == files and lost.snapshots() == snapshots
    assert env.store.objects == store_before
    assert report.marker.current is True and report.marker.written is None and report.marker.catalog_copied is False
    assert all(t.existed and t.rows_appended == 0 for t in report.tables.values())


def test_a_rebuild_registers_the_exact_file_in_the_marker_not_the_newest_file(env):
    land = busy_landing()
    # a run that commits a late object and dies before it can write its marker
    env.run(land.reader())
    land.transaction(("merchants", "c"), ("merchant_transactions", "c"), date="2025-05-05")
    land.flush()

    def die(batch):
        if batch.table == "merchant_transactions":
            raise SimulatedCrash("after the last commit, before the marker")

    with pytest.raises(SimulatedCrash):
        env.run(land.reader(), on_batch=die)
    marker = newest_marker(env.store)
    newer = env.table("merchant_transactions").metadata_location
    assert newer != marker.tables["merchant_transactions"].metadata_location
    assert len(env.rows("merchant_transactions")) == marker.tables["merchant_transactions"].rows + 1

    lost = env.lose_catalog()
    rebuild_catalog(lost.catalog, lost.store)
    assert lost.table("merchant_transactions").metadata_location == marker.tables["merchant_transactions"].metadata_location
    assert len(lost.rows("merchant_transactions")) == marker.tables["merchant_transactions"].rows

    # the next run appends what the marker's view lacks, once, and writes the marker it never wrote
    report = lost.run(land.reader())
    assert report.tables["merchants"].rows_appended == 1 and report.tables["merchant_transactions"].rows_appended == 1
    assert report.marker.written is not None
    for name, events in landed(land).items():
        assert bronze_events(lost, name) == sort_events(events), name
        keys = lost.keys(name)
        assert keys == sorted(set(keys))


def test_the_rebuild_of_tables_that_never_had_a_snapshot_works(env):
    land = Landing()
    env.run(land.reader())
    lost = env.lose_catalog()
    rebuild_catalog(lost.catalog, lost.store)
    assert all(lost.has(n) and lost.table(n).current_snapshot() is None for n in TABLE_ORDER)
    report = lost.run(land.reader())
    assert report.marker.current is True and report.marker.written is None


def test_a_rebuild_without_a_marker_is_refused(env):
    env.run(busy_landing().reader())
    lost = env.lose_catalog()
    for key in [k for k in lost.store.objects if k.startswith("_runs/")]:
        del lost.store.objects[key]
    with pytest.raises(ConfigError) as stop:
        rebuild_catalog(lost.catalog, lost.store)
    assert "no run marker" in str(stop.value)
    assert not any(lost.has(n) for n in TABLE_ORDER)


@pytest.mark.parametrize("present", [["merchants"], ["risk_events"], list(TABLE_ORDER)])
def test_a_rebuild_over_tables_the_catalog_already_has_is_refused_and_touches_none_of_them(env, present):
    env.run(busy_landing().reader())
    lost = env.lose_catalog()
    first = lost.catalog
    first.create_namespace_if_not_exists(NAMESPACE)
    for name in present:
        first.register_table(f"{NAMESPACE}.{name}", env.table(name).metadata_location)
    before = {name: lost.table(name).metadata_location for name in present}
    with pytest.raises(ConfigError) as stop:
        rebuild_catalog(lost.catalog, lost.store)
    for name in present:
        assert name in str(stop.value)
    assert {n for n in TABLE_ORDER if lost.has(n)} == set(present)
    assert {name: lost.table(name).metadata_location for name in present} == before


def tamper(env, change):
    """Rewrite the newest marker in the store."""
    key = sorted(k for k in env.store.objects if k.startswith("_runs/"))[-1]
    body = json.loads(env.store.objects[key])
    change(body)
    env.store.objects[key] = json.dumps(body).encode()


@pytest.mark.parametrize("what,change", [
    ("a snapshot id that is not the file's", lambda b: b["tables"]["risk_events"].update(snapshot_id=5)),
    ("a row count that is not the table's", lambda b: b["tables"]["risk_events"].update(rows=b["tables"]["risk_events"]["rows"] + 1)),
    ("a snapshot where the file has none", lambda b: b["tables"]["loan_repayments"].update(snapshot_id=None)),
])
def test_a_table_that_does_not_match_the_marker_stops_the_rebuild_and_registers_none(env, what, change):
    env.run(busy_landing().reader())
    tamper(env, change)
    files = env.files()
    lost = env.lose_catalog()
    with pytest.raises(MarkerError) as stop:
        rebuild_catalog(lost.catalog, lost.store)
    assert "registered from" in str(stop.value)
    assert not any(lost.has(n) for n in TABLE_ORDER)           # what was registered is dropped again
    assert env.files() == files                                  # and dropping a catalog entry deletes no file


def test_a_marker_that_names_a_metadata_file_that_is_not_there_stops_the_rebuild_and_registers_none(env):
    env.run(busy_landing().reader())
    tamper(env, lambda b: b["tables"]["risk_events"].update(
        metadata_location=b["tables"]["risk_events"]["metadata_location"].replace("00001-", "00099-")))
    lost = env.lose_catalog()
    with pytest.raises(Exception):
        rebuild_catalog(lost.catalog, lost.store)
    assert not any(lost.has(n) for n in TABLE_ORDER)


class FailingRegister:
    """A catalog that fails before it has recorded anything for one table, as a catalog can."""

    def __init__(self, inner, fail_at):
        self.inner, self.fail_at, self.calls = inner, fail_at, 0

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def register_table(self, identifier, metadata_location):
        self.calls += 1
        if self.calls == self.fail_at:
            raise RuntimeError("injected: the catalog failed before recording this table")
        return self.inner.register_table(identifier, metadata_location)


def test_a_rebuild_that_fails_before_a_table_is_recorded_drops_the_tables_it_did_register_and_says_why(env):
    env.run(busy_landing().reader())
    lost = env.lose_catalog()
    with pytest.raises(RuntimeError) as stop:
        rebuild_catalog(FailingRegister(lost.catalog, fail_at=3), lost.store)
    assert "injected" in str(stop.value)
    assert not any(lost.has(n) for n in TABLE_ORDER)


def test_a_rebuild_does_not_touch_the_store(env):
    env.run(busy_landing().reader())
    before, listings = dict(env.store.objects), env.store.listings
    lost = env.lose_catalog()
    rebuild_catalog(lost.catalog, lost.store)
    assert env.store.objects == before and env.store.listings == listings + 1 and env.store.overwrites == 1
