"""tests/test_bronze_orphans.py: G4 step 3, python -m bronze.orphans: what is in the Bronze bucket that nothing uses, what
something uses that is not there, the check against the newest run marker and the catalog copy, and the guarded delete.

The bucket is a directory that is also the warehouse root (tests/bronze_bucket.py), so the files are the ones PyIceberg
wrote, and BronzeStore, the real guard, the real marker code and the real catalog copy code run over it. Nothing here
touches GCS; the real calls are tested against a fake client in tests/test_cdc_gcs.py. Held here, each with a mutation
that fails it (see the commit message):
  - what a table uses is asked of PyIceberg and equals the files on disk after many commits; a clean write has no orphans
    and every object in the bucket is accounted for;
  - each kind of leftover a crash makes (a data file, a manifest, a manifest list, a metadata file with the number of the
    real one) is found, belongs to its table, and is never confused with a used file;
  - a list deletes nothing; --delete deletes only the old, unused table files, keeps every used file, marker and the
    copy byte for byte, and keeps the young ones; the age is a setting and the default is 24 hours;
  - a catalog behind the newest marker stops the command before the bucket is listed and nothing is deleted; any problem
    (a used file missing, an unexpected object, a marker or copy that does not fit) means nothing is deleted;
  - the catalog copy and the marker are checked against each other;
  - every exit code with one error line, and how many files a failed delete had removed;
  - the real command refuses to start without credentials.
"""
import io
import os
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyiceberg.catalog.sql import SqlCatalog

import bronze.orphans as orphans
from bronze.config import CREDENTIALS_VARIABLE, BronzeSettings
from bronze.errors import ConfigError
from bronze.marker import TableMark, newest_marker, write_marker
from bronze.orphans import Orphan, Scan, copy_differences, deletable, execute, main, render, scan, used_by
from bronze.run import NAMESPACE, TABLE_ORDER
from bronze.store import CATALOG_KEY
from cdc.errors import SinkError
from cdc.gcs import ObjectInfo
from tests.bronze_bucket import age, bucket_env, leave, table_dir
from tests.bronze_env import busy_landing
from tests.bronze_synth import SOURCE_DB

ROOT = Path(__file__).resolve().parent.parent
DAY = timedelta(days=1)


class SpyStore:
    """The real store, with every call recorded, and a failure that can be scripted."""

    def __init__(self, store, bucket, fail=None) -> None:
        self.store, self.bucket, self.calls, self.fail = store, bucket, [], fail or {}

    def _go(self, name, *args):
        self.calls.append((name,) + args)
        if name in self.fail:
            outcome = self.fail[name]
            if isinstance(outcome, tuple):                        # (error, number of calls to let through first)
                error, allowed = outcome
                if sum(1 for c in self.calls if c[0] == name) > allowed:
                    raise error
            else:
                raise outcome
        return getattr(self.store, name)(*args)

    def list_keys(self, prefix):
        return self._go("list_keys", prefix)

    def read(self, key):
        return self._go("read", key)

    def list_objects(self, prefix):
        return self._go("list_objects", prefix)

    def delete(self, key):
        return self._go("delete", key)

    def names(self, name):
        return [c[1:] for c in self.calls if c[0] == name]


@pytest.fixture
def world(tmp_path, monkeypatch):
    """Bronze written from a busy landing in several commits, on a directory bucket, with a marker and a catalog copy."""
    env, bucket, store = bucket_env(tmp_path)
    land = busy_landing()
    env.run(land.reader(), batch_rows=3)
    for variable in ("BRONZE_BUCKET", "BRONZE_PROJECT", "BRONZE_CATALOG_PATH", "BRONZE_READ_WORKERS", "BRONZE_BATCH_ROWS",
                     "CDC_GCS_BUCKET", "CDC_SLOT_NAME", "CDC_DB"):
        monkeypatch.delenv(variable, raising=False)
    credentials = tmp_path / "bronze-writer.json"
    credentials.write_text("{}")

    class World:
        pass

    w = World()
    w.env, w.bucket, w.store, w.land, w.tmp = env, bucket, store, land, tmp_path
    w.spy = SpyStore(store, bucket)
    w.settings = BronzeSettings(_env_file=None, catalog_path=str(env.catalog_file))
    w.environ = {CREDENTIALS_VARIABLE: str(credentials)}
    w.opened = []

    def run(**kwargs):
        out, err = io.StringIO(), io.StringIO()
        override = kwargs.pop("catalog", None)

        def store_factory(settings, environ):
            w.opened.append("store")
            return w.spy

        def catalog_factory(settings):
            w.opened.append("catalog")
            return override or env.catalog

        kwargs.setdefault("environ", w.environ)
        code = execute(w.settings, store_factory=store_factory, catalog_factory=catalog_factory, out=out, err=err, **kwargs)
        return code, out.getvalue(), err.getvalue()

    w.run = run
    w.keys = lambda: bucket.list_keys("")
    w.snapshot = lambda: {k: (bucket.directory / k).read_bytes() for k in bucket.list_keys("")}
    return w


def errors(err):
    return [l for l in err.splitlines() if l.startswith("error:")]


def problems(err):
    return [l for l in err.splitlines() if l.startswith("PROBLEM:")]


def table_keys(w, table):
    return {k for k in w.keys() if k.startswith(f"bronze/{table}/")}


# ---------------------------------------------------------------------------
# what a table uses, and a clean bucket
# ---------------------------------------------------------------------------

def test_what_each_table_uses_equals_the_files_on_disk_after_several_commits(world):
    w = world
    root = w.env.catalog.properties["warehouse"].rstrip("/") + "/"
    assert max(len(w.env.snapshots()[t]) for t in TABLE_ORDER) > 2                    # more than one commit somewhere
    for table in TABLE_ORDER:
        paths = used_by(w.env.table(table))
        assert all(p.startswith(root) for p in paths)
        assert {p[len(root):] for p in paths} == table_keys(w, table)


def test_used_by_names_every_kind_of_file_including_statistics_files():
    """A table that has every kind of file, built by hand: PyIceberg writes statistics files only when asked to."""
    from types import SimpleNamespace as N

    def listing(**columns):
        return N(to_pylist=lambda: [dict(zip(columns, row)) for row in zip(*columns.values())])

    fake = N(metadata_location="loc://current.json",
             metadata=N(metadata_log=[N(metadata_file="loc://old1.json"), N(metadata_file="loc://old2.json")],
                        statistics=[N(statistics_path="loc://stats.puffin")],
                        partition_statistics=[N(statistics_path="loc://pstats.parquet")]),
             snapshots=lambda: [N(manifest_list="loc://snap-1.avro"), N(manifest_list="loc://snap-2.avro"), N(manifest_list=None)],
             inspect=N(all_manifests=lambda: listing(path=["loc://m0.avro", "loc://m1.avro", "loc://m0.avro"]),
                       all_files=lambda: listing(file_path=["loc://d0.parquet", "loc://d1.parquet", "loc://del0.parquet"])))
    assert used_by(fake) == {"loc://current.json", "loc://old1.json", "loc://old2.json", "loc://snap-1.avro", "loc://snap-2.avro",
                             "loc://m0.avro", "loc://m1.avro", "loc://d0.parquet", "loc://d1.parquet", "loc://del0.parquet",
                             "loc://stats.puffin", "loc://pstats.parquet"}


def test_a_clean_write_has_no_orphans_and_every_object_is_accounted_for(world):
    w = world
    code, out, err = w.run()
    assert code == 0 and errors(err) == [] and problems(err) == []
    markers, copies = len(w.bucket.list_keys("_runs/")), len(w.bucket.list_keys("_catalog/"))
    tables = sum(len(table_keys(w, t)) for t in TABLE_ORDER)
    assert markers == 1 and copies == 1
    assert out.splitlines()[1] == f"{tables + markers + copies} objects in gs://fintech_bronze: {tables} table files, 1 run markers, 1 catalog copy"
    assert "orphans: none" in out and "OK: every object in the bucket is accounted for; there are no orphans" in out
    assert "catalog copy: agrees with the newest marker" in out and "the tables are at it" in out


def test_the_table_shows_used_in_bucket_and_orphans_for_each_table(world):
    w = world
    leave(w.env, "risk_events", "data")
    out = w.run()[1]
    for table in TABLE_ORDER:
        line = next(l for l in out.splitlines() if l.startswith(table + " "))
        used, in_bucket, found = (int(x.replace(",", "")) for x in line.split()[1:])
        assert used == len(used_by(w.env.table(table))) and in_bucket == len(table_keys(w, table))
        assert found == (1 if table == "risk_events" else 0)
    assert next(l for l in out.splitlines() if l.startswith("risk_events ")).split()[1:] == [
        str(len(used_by(w.env.table("risk_events")))), str(len(table_keys(w, "risk_events"))), "1"]


def test_the_listing_is_one_call_over_the_whole_bucket_and_nothing_else_is_listed(world):
    w = world
    w.run()
    assert w.spy.names("list_objects") == [("",)]
    assert w.spy.names("list_keys") == [("_runs/",)]                 # the marker lookup
    assert w.spy.names("delete") == []


# ---------------------------------------------------------------------------
# the leftovers of a crash
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("kind", ["data", "manifest", "manifest list", "metadata"])
@pytest.mark.parametrize("table", ["merchant_transactions", "merchants"])
def test_each_kind_of_leftover_is_found_under_its_own_table_and_nothing_used_is_called_one(world, kind, table):
    w = world
    key = leave(w.env, table, kind)
    found = scan(w.env.catalog, w.spy, clock=lambda: datetime.now(timezone.utc))
    assert [o.key for o in found.orphans] == [key] and found.orphans[0].table == table
    assert found.orphans[0].kind == {"data": "data file", "manifest": "manifest", "manifest list": "manifest list",
                                     "metadata": "metadata file"}[kind]
    assert found.ok and found.orphans[0].age >= 2 * DAY and found.orphans[0].size > 0


def test_a_metadata_file_with_the_number_of_the_real_one_is_told_apart_by_what_the_table_uses(world):
    w = world
    key = leave(w.env, "merchants", "metadata")
    real = w.env.table("merchants").metadata_location.split("/")[-1]
    assert key.split("/")[-1].split("-")[0] == real.split("-")[0] and key.split("/")[-1] != real
    found = scan(w.env.catalog, w.spy, clock=lambda: datetime.now(timezone.utc))
    assert [o.key for o in found.orphans] == [key]                    # the real file with the same number is not touched


def test_a_list_prints_the_orphans_deletes_nothing_and_exits_0(world):
    w = world
    keys = [leave(w.env, "merchants", "data"), leave(w.env, "risk_events", "manifest")]
    before = w.snapshot()
    code, out, err = w.run()
    assert code == 0 and errors(err) == []
    assert "orphans: 2 files" in out and all(k in out for k in keys) and "OK: every object in the bucket is accounted for; 2 orphans are listed above" in out
    assert w.snapshot() == before and w.spy.names("delete") == []


def test_several_leftovers_of_one_kind_are_counted_per_table(world):
    w = world
    for _ in range(3):
        leave(w.env, "merchant_transactions", "data")
    leave(w.env, "merchants", "data")
    out = w.run()[1]
    assert next(l for l in out.splitlines() if l.startswith("merchant_transactions ")).split()[-1] == "3"
    assert next(l for l in out.splitlines() if l.startswith("merchants ")).split()[-1] == "1"
    assert "orphans: 4 files" in out


# ---------------------------------------------------------------------------
# --delete
# ---------------------------------------------------------------------------

def test_delete_removes_the_old_unused_files_and_changes_nothing_else_byte_for_byte(world):
    w = world
    old = [leave(w.env, "merchants", "data"), leave(w.env, "merchant_transactions", "manifest list"),
           leave(w.env, "risk_events", "metadata")]
    before = w.snapshot()
    code, out, err = w.run(delete=True)
    assert code == 0 and errors(err) == []
    after = w.snapshot()
    assert set(before) - set(after) == set(old) and set(after) <= set(before)
    assert all(after[k] == before[k] for k in after)                  # everything left is byte for byte what it was
    assert sorted(k for (k,) in w.spy.names("delete")) == sorted(old)
    assert "--delete: deleting 3 files" in out and "deleted 3 files" in out and "OK: deleted 3 orphans" in out
    for key in old:
        assert f"deleted {key}" in err


def test_delete_keeps_every_used_file_the_marker_and_the_copy(world):
    w = world
    leave(w.env, "merchants", "data")
    used = {k for t in TABLE_ORDER for k in table_keys(w, t)} - {o.key for o in scan(w.env.catalog, w.spy, clock=lambda: datetime.now(timezone.utc)).orphans}
    w.run(delete=True)
    assert used <= set(w.keys()) and len(w.bucket.list_keys("_runs/")) == 1 and w.bucket.list_keys("_catalog/") == [CATALOG_KEY]


def test_delete_with_nothing_to_delete_says_so(world):
    code, out, err = world.run(delete=True)
    assert code == 0 and "--delete: nothing to delete" in out and "OK: deleted 0 orphans" in out
    assert world.spy.names("delete") == []


def test_a_young_orphan_survives_a_delete_and_the_old_one_does_not(world):
    w = world
    old = leave(w.env, "merchants", "data", old=timedelta(hours=25))
    young = leave(w.env, "merchants", "manifest", old=timedelta(hours=23))
    code, out, err = w.run(delete=True)
    assert code == 0 and old not in w.keys() and young in w.keys()
    assert "(too young to delete)" in out and f"{young}" in out
    assert "--delete: deleting 1 files" in out


def test_all_orphans_young_means_nothing_is_deleted_and_the_count_is_said(world):
    w = world
    for _ in range(2):
        leave(w.env, "merchants", "data", old=timedelta(minutes=5))
    code, out, err = w.run(delete=True)
    assert code == 0 and "--delete: nothing to delete (2 orphans are too young)" in out and w.spy.names("delete") == []


def test_the_minimum_age_is_a_setting_and_zero_deletes_a_young_orphan(world):
    w = world
    young = leave(w.env, "merchants", "data", old=timedelta(minutes=5))
    assert w.run(delete=True)[0] == 0 and young in w.keys()
    assert w.run(delete=True, min_age_hours=0)[0] == 0 and young not in w.keys()


def test_the_minimum_age_is_compared_in_hours_with_the_age_at_the_edge_counting(world):
    w = world
    key = leave(w.env, "merchants", "data", old=timedelta(hours=10))
    assert w.run(delete=True, min_age_hours=11)[0] == 0 and key in w.keys()
    assert w.run(delete=True, min_age_hours=9.9)[0] == 0 and key not in w.keys()


def test_a_negative_minimum_age_is_a_usage_error_before_anything_is_opened(world):
    code, out, err = world.run(min_age_hours=-1)
    assert code == 1 and world.opened == [] and "--min-age-hours cannot be negative" in err and len(errors(err)) == 1


def test_a_second_delete_finds_nothing_left(world):
    w = world
    leave(w.env, "merchants", "data")
    assert w.run(delete=True)[0] == 0
    code, out, err = w.run(delete=True)
    assert code == 0 and "orphans: none" in out and "nothing to delete" in out


def test_the_default_minimum_age_is_24_hours():
    assert orphans.DEFAULT_MIN_AGE_HOURS == 24
    flag = next(p for p in orphans.cli.params if p.name == "min_age_hours")
    assert flag.default == 24.0 and next(p for p in orphans.cli.params if p.name == "do_delete").is_flag


def test_deletable_needs_a_table_file_that_is_unused_and_old_enough(world):
    w = world
    found = scan(w.env.catalog, w.spy, clock=lambda: datetime.now(timezone.utc))
    used_key = next(iter(found.used["merchants"]))
    long_ago = timedelta(days=9)
    assert deletable(Orphan("bronze/merchants/data/x.parquet", "merchants", 1, long_ago), found, DAY)
    assert deletable(Orphan("bronze/merchants/data/x.parquet", "merchants", 1, DAY), found, DAY)             # exactly old enough
    assert not deletable(Orphan("bronze/merchants/data/x.parquet", "merchants", 1, DAY - timedelta(seconds=1)), found, DAY)
    assert not deletable(Orphan("bronze/merchants/data/x.parquet", "merchants", 1, timedelta(hours=1)), found, DAY)
    assert not deletable(Orphan(used_key, "merchants", 1, long_ago), found, DAY)              # used
    assert not deletable(Orphan("_runs/20261029T153000123456Z.json", "merchants", 1, long_ago), found, DAY)
    assert not deletable(Orphan("_catalog/catalog.db", "merchants", 1, long_ago), found, DAY)
    assert not deletable(Orphan("bronze/merchants/notes.txt", "merchants", 1, long_ago), found, DAY)


def test_the_delete_guard_is_the_real_one_so_a_non_table_name_could_never_reach_the_bucket(world):
    w = world
    for key in ("_catalog/catalog.db", w.bucket.list_keys("_runs/")[0], "bronze/merchants/notes.txt"):
        with pytest.raises(ConfigError):
            w.store.delete(key)
    assert CATALOG_KEY in w.keys() and len(w.bucket.list_keys("_runs/")) == 1


# ---------------------------------------------------------------------------
# a catalog behind the marker, and every other problem, delete nothing
# ---------------------------------------------------------------------------

def behind_catalog(w):
    """A catalog from before the last run, with a marker that is newer than it. Returns that catalog."""
    state_one = w.tmp / "state_one.db"
    shutil.copy(w.env.catalog_file, state_one)
    w.land.transaction(("risk_events", "c"), date="2025-04-02")
    w.land.flush()
    w.env.run(w.land.reader())
    return SqlCatalog(NAMESPACE, uri=f"sqlite:///{state_one}", warehouse=w.env.uri)


def test_a_catalog_behind_the_marker_stops_the_command_before_the_bucket_is_listed(world):
    w = world
    stale = behind_catalog(w)
    code, out, err = w.run(catalog=stale)
    assert code == 2 and len(errors(err)) == 1 and "python -m bronze.main --rebuild-catalog" in err
    assert w.spy.names("list_objects") == [] and out.count("objects in gs://") == 0


def test_a_catalog_behind_the_marker_deletes_nothing_even_with_an_old_orphan_and_delete(world):
    w = world
    stale = behind_catalog(w)
    leave(w.env, "merchants", "data")
    before = w.snapshot()
    code, out, err = w.run(catalog=stale, delete=True)
    assert code == 2 and w.snapshot() == before and w.spy.names("delete") == []


def test_a_lost_catalog_with_a_marker_is_the_same_stop(world):
    w = world
    lost = w.env.lose_catalog()
    leave(w.env, "merchants", "data")
    before = w.snapshot()
    code, out, err = w.run(catalog=lost.catalog, delete=True)
    assert code == 2 and "does not have it" in err and w.snapshot() == before and w.spy.names("delete") == []


def test_a_used_file_missing_from_the_bucket_is_a_problem_and_nothing_is_deleted(world):
    w = world
    gone = sorted(k for k in table_keys(w, "merchants") if k.endswith(".parquet"))[0]
    (w.bucket.directory / gone).unlink()
    old = leave(w.env, "risk_events", "data")
    before = w.snapshot()
    code, out, err = w.run(delete=True)
    assert code == 2 and w.snapshot() == before and old in w.keys()
    assert problems(err)[0].startswith("PROBLEM: 1 files a table uses are not in the bucket (data loss), for example " + gone)
    assert errors(err) == ["error: NOT CLEAN: 1 problems; nothing was deleted"] and "--delete" not in out.replace("--delete to remove", "")


def test_a_used_file_missing_is_exit_2_even_without_delete(world):
    w = world
    (w.bucket.directory / sorted(k for k in table_keys(w, "risk_events") if k.endswith(".parquet"))[0]).unlink()
    code, out, err = w.run()
    assert code == 2 and errors(err) == ["error: NOT CLEAN: 1 problems"]


def test_an_unexpected_object_is_a_problem_is_never_deleted_and_blocks_a_delete(world):
    w = world
    strays = ["stray.txt", "bronze/merchants/notes.txt", "bronze/other_table/data/x.parquet", "cdc/cdc_r01/x.ndjson.gz"]
    for key in strays:
        path = w.bucket.directory / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")
        age(path, 5 * DAY)
    old = leave(w.env, "merchants", "data")
    code, out, err = w.run(delete=True)
    assert code == 2 and all(k in w.keys() for k in strays) and old in w.keys()
    assert problems(err) == [f"PROBLEM: 4 objects are in the bucket that belong to nothing Bronze writes, for example "
                             f"bronze/merchants/notes.txt (never deleted by this command)"]


def test_a_marker_that_names_a_file_the_catalog_does_not_use_is_a_problem(world):
    w = world
    stray = leave(w.env, "merchants", "metadata")
    root = w.env.catalog.properties["warehouse"].rstrip("/") + "/"
    marker = newest_marker(w.store)
    newer = replace(marker, run_id="20991231T000000000000Z",
                    tables={**marker.tables, "merchants": TableMark(marker.tables["merchants"].snapshot_id,
                                                                   marker.tables["merchants"].rows, root + stray)})
    write_marker(w.store, newer)
    code, out, err = w.run()
    assert code == 2
    assert problems(err)[0] == f"PROBLEM: the newest marker names {root + stray} for merchants, which the catalog does not use"
    assert len(problems(err)) == 2 and "the catalog copy points at" in problems(err)[1]     # the copy still names the old file


def test_a_marker_that_names_a_file_outside_the_warehouse_is_a_problem(world):
    w = world
    marker = newest_marker(w.store)
    newer = replace(marker, run_id="20991231T000000000000Z",
                    tables={**marker.tables, "risk_events": TableMark(marker.tables["risk_events"].snapshot_id,
                                                                     marker.tables["risk_events"].rows, "gs://elsewhere/x.json")})
    write_marker(w.store, newer)
    code, out, err = w.run()
    assert code == 2 and "PROBLEM: the newest marker names gs://elsewhere/x.json for risk_events, which is not in this warehouse" in err


def test_a_current_metadata_file_missing_from_the_bucket_is_data_loss_exit_2_and_nothing_is_deleted(world):
    w = world
    marker = newest_marker(w.store)
    root = w.env.catalog.properties["warehouse"].rstrip("/") + "/"
    (w.bucket.directory / marker.tables["risk_events"].metadata_location[len(root):]).unlink()
    old = leave(w.env, "merchants", "data")
    before = w.snapshot()
    code, out, err = w.run(delete=True)
    assert code == 2 and len(errors(err)) == 1 and "table risk_events cannot be read" in err and "data loss" in err
    assert w.snapshot() == before and old in w.keys() and w.spy.names("list_objects") == []


@pytest.mark.parametrize("which", ["manifest", "manifest list"])
def test_a_manifest_or_manifest_list_missing_from_the_bucket_is_data_loss_exit_2_not_a_bucket_failure(world, which):
    w = world
    metadata = table_dir(w.env, "merchants", "metadata")
    victim = sorted(p for p in metadata.glob("*.avro") if p.name.startswith("snap-") == (which == "manifest list"))[0]
    victim.unlink()
    code, out, err = w.run(delete=True)
    assert code == 2 and "table merchants cannot be read" in err and w.spy.names("delete") == []


def test_an_older_metadata_file_the_marker_names_that_is_gone_is_reported_twice_as_missing_and_as_the_markers(world):
    w = world
    marker = newest_marker(w.store)
    table = w.env.table("risk_events")
    table.append(table.scan(limit=1).to_arrow())                  # the catalog moves on: the marker's file is now an older one
    root = w.env.catalog.properties["warehouse"].rstrip("/") + "/"
    location = marker.tables["risk_events"].metadata_location
    assert location != w.env.table("risk_events").metadata_location
    (w.bucket.directory / location[len(root):]).unlink()
    code, out, err = w.run()
    assert code == 2
    assert problems(err) == [f"PROBLEM: 1 files a table uses are not in the bucket (data loss), for example {location[len(root):]}",
                             f"PROBLEM: the newest marker names {location} for risk_events, which is not in the bucket"]


def test_a_warehouse_with_a_trailing_slash_is_the_same_warehouse(world):
    w = world
    slashed = SqlCatalog(NAMESPACE, uri=f"sqlite:///{w.env.catalog_file}", warehouse=w.env.uri + "/")
    found = scan(slashed, w.spy, clock=lambda: datetime.now(timezone.utc))
    assert found.ok and found.outside == [] and found.orphans == [] and found.root == w.env.uri + "/"


def test_a_file_used_by_another_table_is_never_an_orphan_wherever_it_sits(world, monkeypatch):
    w = world
    stray = leave(w.env, "merchants", "data")                          # sits under merchants/ and nothing in merchants uses it
    root = w.env.catalog.properties["warehouse"].rstrip("/") + "/"
    real = orphans.used_by
    monkeypatch.setattr(orphans, "used_by", lambda table: real(table) | ({root + stray} if "risk_events" in table.metadata_location else set()))
    found = scan(w.env.catalog, w.spy, clock=lambda: datetime.now(timezone.utc))
    assert found.orphans == [] and stray in found.used["risk_events"]


def test_a_used_file_outside_the_warehouse_is_reported(world, monkeypatch):
    w = world
    real = orphans.used_by
    monkeypatch.setattr(orphans, "used_by", lambda table: real(table) | {"gs://elsewhere/x.avro"})
    code, out, err = w.run()
    root = w.env.catalog.properties["warehouse"].rstrip("/") + "/"
    assert code == 2 and f"PROBLEM: 5 files a table uses are outside the warehouse {root}, for example gs://elsewhere/x.avro" in err


# ---------------------------------------------------------------------------
# the run marker and the catalog copy
# ---------------------------------------------------------------------------

def test_a_catalog_ahead_of_the_marker_is_said_and_is_not_a_problem(world):
    w = world
    table = w.env.table("risk_events")
    table.append(table.scan(limit=1).to_arrow())                 # a commit by a run that died before its marker
    code, out, err = w.run()
    assert code == 0 and "AHEAD of it" in out and "the next run writes a new one" in out
    assert "orphans: none" in out                                # the new files are used by the catalog: not orphans


def test_a_catalog_ahead_of_the_marker_does_not_make_its_new_files_orphans_and_delete_leaves_them(world):
    w = world
    table = w.env.table("risk_events")
    table.append(table.scan(limit=1).to_arrow())
    for path in w.bucket.directory.rglob("*"):
        if path.is_file():
            age(path, 9 * DAY)
    before = w.snapshot()
    assert w.run(delete=True)[0] == 0 and w.snapshot() == before


def test_a_bucket_with_tables_but_no_marker_says_none_yet_and_checks_nothing_against_it(world):
    w = world
    for key in w.bucket.list_keys("_runs/") + w.bucket.list_keys("_catalog/"):
        (w.bucket.directory / key).unlink()
    code, out, err = w.run()
    assert code == 0 and "run marker: none yet" in out and "no marker yet" in out and "0 run markers, 0 catalog copy" in out


def test_an_empty_bucket_with_an_empty_catalog_is_clean(tmp_path, monkeypatch):
    env, bucket, store = bucket_env(tmp_path)
    credentials = tmp_path / "c.json"
    credentials.write_text("{}")
    out, err = io.StringIO(), io.StringIO()
    code = execute(BronzeSettings(_env_file=None, catalog_path=str(env.catalog_file)), store_factory=lambda s, e: store,
                   catalog_factory=lambda s: env.catalog, environ={CREDENTIALS_VARIABLE: str(credentials)}, out=out, err=err)
    assert code == 0 and "0 objects in gs://fintech_bronze" in out.getvalue() and "orphans: none" in out.getvalue()


def test_the_copy_must_point_at_the_files_the_marker_names(world):
    w = world
    marker = newest_marker(w.store)
    assert copy_differences((w.bucket.directory / CATALOG_KEY).read_bytes(), marker) == []


def test_a_copy_from_before_the_last_run_differs_and_the_difference_names_the_table(world):
    w = world
    state_one = w.tmp / "state_one.db"
    shutil.copy(w.env.catalog_file, state_one)
    w.land.transaction(("risk_events", "c"), date="2025-04-02")
    w.land.flush()
    w.env.run(w.land.reader())                                   # a new marker and a new copy ...
    (w.bucket.directory / CATALOG_KEY).write_bytes(state_one.read_bytes())    # ... and then the old copy back
    code, out, err = w.run()
    assert code == 2 and "catalog copy: DIFFERS from the newest marker" in out
    (line,) = problems(err)
    assert line.startswith("PROBLEM: risk_events: the catalog copy points at ") and ", the newest marker names " in line


def test_a_missing_catalog_copy_is_a_problem_when_there_is_a_marker(world):
    w = world
    (w.bucket.directory / CATALOG_KEY).unlink()
    code, out, err = w.run()
    assert code == 2 and "catalog copy: MISSING" in out
    assert problems(err) == [f"PROBLEM: the bucket has no catalog copy ({CATALOG_KEY}) though the marker "
                             f"{newest_marker(w.store).run_id} exists"]


def test_a_catalog_copy_that_is_not_a_catalog_is_a_problem_not_a_crash(world):
    w = world
    (w.bucket.directory / CATALOG_KEY).write_bytes(b"this is not sqlite")
    code, out, err = w.run()
    assert code == 2 and len(problems(err)) == 1 and "the catalog copy is not a readable catalog" in problems(err)[0]


def test_a_catalog_copy_without_the_tables_names_each_missing_table(world, tmp_path):
    marker = newest_marker(world.store)
    empty = tmp_path / "empty.db"
    with sqlite3.connect(empty) as conn:
        conn.execute("CREATE TABLE iceberg_tables (catalog_name TEXT, table_namespace TEXT, table_name TEXT, metadata_location TEXT)")
        conn.execute("INSERT INTO iceberg_tables VALUES ('bronze', 'bronze', 'merchants', ?)", (marker.tables["merchants"].metadata_location,))
    differences = copy_differences(empty.read_bytes(), marker)
    assert differences == [f"the catalog copy has no table {t}" for t in TABLE_ORDER if t != "merchants"]


def test_a_copy_for_another_namespace_has_none_of_the_tables(world, tmp_path):
    marker = newest_marker(world.store)
    other = tmp_path / "other.db"
    with sqlite3.connect(other) as conn:
        conn.execute("CREATE TABLE iceberg_tables (catalog_name TEXT, table_namespace TEXT, table_name TEXT, metadata_location TEXT)")
        conn.execute("INSERT INTO iceberg_tables VALUES ('bronze', 'elsewhere', 'merchants', 'x')")
    assert copy_differences(other.read_bytes(), marker) == [f"the catalog copy has no table {t}" for t in TABLE_ORDER]


# ---------------------------------------------------------------------------
# the report
# ---------------------------------------------------------------------------

def make_scan(orphan_count, **changes):
    found = Scan(objects=10, used={t: {f"bronze/{t}/data/u"} for t in TABLE_ORDER}, in_bucket={t: 1 for t in TABLE_ORDER},
                 copy_listed=True, markers=1)
    found.orphans = [Orphan(f"bronze/merchants/data/o{n:03}.parquet", "merchants", 10 + n, timedelta(hours=30)) for n in range(orphan_count)]
    for name, value in changes.items():
        setattr(found, name, value)
    return found


def test_only_the_first_fifty_orphans_are_listed_and_the_rest_counted(monkeypatch):
    text = render(make_scan(7), DAY, "b")
    assert text.count("bronze/merchants/data/o") == 7
    monkeypatch.setattr(orphans, "SHOWN", 3)
    text = render(make_scan(7), DAY, "b")
    assert text.count("bronze/merchants/data/o") == 3 and "(and 4 more)" in text and "o002" in text and "o003" not in text


def test_the_report_gives_the_count_the_size_the_kind_and_the_age_of_each_orphan():
    found = make_scan(0, orphans=[Orphan("bronze/merchants/data/e/x.parquet", "merchants", 5_000, timedelta(hours=30)),
                                 Orphan("bronze/merchants/metadata/snap-1-2.avro", "merchants", 20_000_000, timedelta(days=3)),
                                 Orphan("bronze/merchants/metadata/a-m0.avro", "merchants", 1, timedelta(minutes=30)),
                                 Orphan("bronze/merchants/metadata/0007-x.metadata.json", "merchants", 2_000, timedelta(hours=47))])
    text = render(found, DAY, "b")
    assert "orphans: 4 files, 20.0 MB; 3 are at least 24 hours old" in text
    assert "5,000 B  data file  30.0 hours old" in text and "20.0 MB  manifest list  3.0 days old" in text
    assert "1 B  manifest  0.5 hours old  (too young to delete)" in text and "2,000 B  metadata file  47.0 hours old" in text


def test_sizes_are_shown_in_bytes_then_kilobytes_then_megabytes():
    sizes = {0: "0 B", 9_999: "9,999 B", 10_000: "10.0 KB", 15_000: "15.0 KB", 9_999_999: "10,000.0 KB", 10_000_000: "10.0 MB",
             123_456_789: "123.5 MB"}
    assert {n: orphans._size(n) for n in sizes} == sizes


def test_ages_are_shown_in_hours_up_to_two_days_and_in_days_after():
    assert orphans._age(timedelta(minutes=30)) == "0.5 hours" and orphans._age(timedelta(hours=47.9)) == "47.9 hours"
    assert orphans._age(timedelta(hours=48)) == "2.0 days" and orphans._age(timedelta(days=10)) == "10.0 days"


def test_the_kind_of_an_orphan_is_told_from_its_name():
    kinds = {"bronze/t/data/event_month=2025-03/00000-0-x.parquet": "data file", "bronze/t/metadata/00003-x.metadata.json": "metadata file",
             "bronze/t/metadata/snap-123-1-x.avro": "manifest list", "bronze/t/metadata/abc-m0.avro": "manifest"}
    for key, kind in kinds.items():
        assert Orphan(key, "t", 1, DAY).kind == kind


def test_the_report_says_when_the_tables_are_at_the_marker_and_when_there_is_no_marker(world):
    w = world
    found = scan(w.env.catalog, w.spy, clock=lambda: datetime.now(timezone.utc))
    assert found.at_marker is True and found.marker is not None and found.markers == 1 and found.copy_listed
    text = render(found, DAY, "fintech_bronze")
    assert f"newest {found.marker.run_id}; the tables are at it" in text
    assert "run marker: none yet" in render(make_scan(0), DAY, "b")


def test_the_scan_is_ok_without_problems_and_not_ok_with_one():
    assert Scan().ok and not Scan(problems=["x"]).ok


# ---------------------------------------------------------------------------
# exit codes, one line each
# ---------------------------------------------------------------------------

def test_no_credentials_means_nothing_is_opened_and_exit_1(world):
    code, out, err = world.run(environ={})
    assert code == 1 and world.opened == [] and len(errors(err)) == 1 and "GOOGLE_APPLICATION_CREDENTIALS" in err


def test_a_bucket_that_fails_to_list_is_exit_3(world):
    w = world
    w.spy.fail = {"list_objects": SinkError("listing gave up after 5 attempts")}
    code, out, err = w.run()
    assert code == 3 and errors(err) == ["error: listing gave up after 5 attempts"]


def test_a_delete_that_fails_part_way_says_how_many_files_it_had_deleted(world):
    w = world
    for _ in range(3):
        leave(w.env, "merchants", "data")
    w.spy.fail = {"delete": (SinkError("could not delete x"), 1)}          # the first delete goes through, the second fails
    code, out, err = w.run(delete=True)
    assert code == 3
    assert errors(err) == ["error: could not delete x. 1 files had been deleted before it stopped; run it again to continue "
                           "(a file that is already gone is not an error)."]
    assert len(table_keys(w, "merchants")) == len(used_by(w.env.table("merchants"))) + 2


def test_a_file_that_is_already_gone_is_not_an_error_and_is_not_counted(world, monkeypatch):
    w = world
    leave(w.env, "merchants", "data")
    leave(w.env, "merchants", "data")
    real = w.store.delete
    deletes = []

    def vanishing(key):
        deletes.append(key)
        if len(deletes) == 1:
            (w.bucket.directory / key).unlink()                     # someone else deleted it first
        return real(key)

    monkeypatch.setattr(w.spy, "store", type("S", (), {"__getattr__": lambda self, n: getattr(w.store, n),
                                                       "delete": staticmethod(vanishing)})())
    code, out, err = w.run(delete=True)
    assert code == 0 and "deleted 1 files (1 were already gone)" in out


@pytest.mark.parametrize("error, code", [(KeyboardInterrupt(), 130), (SinkError("x"), 3), (OSError("disk"), 3), (ValueError("bug"), 1)])
def test_every_way_out_is_an_exit_code_and_one_error_line(world, error, code):
    world.spy.fail = {"list_objects": error}
    got, out, err = world.run()
    assert got == code and len(errors(err)) == 1


def test_a_bug_prints_its_traceback_and_a_bronze_error_does_not(world):
    world.spy.fail = {"list_objects": ValueError("boom")}
    assert "Traceback" in world.run()[2]
    world.spy.fail = {"list_objects": SinkError("tidy")}
    assert "Traceback" not in world.run()[2]


def test_a_system_exit_is_not_swallowed(world):
    world.spy.fail = {"list_objects": SystemExit(9)}
    with pytest.raises(SystemExit):
        world.run()


def test_the_report_ends_with_the_time_and_the_memory(world):
    out = world.run()[1]
    assert "min" in out and "peak memory" in out and "on this machine" in out


# ---------------------------------------------------------------------------
# the command line
# ---------------------------------------------------------------------------

def test_the_flags_reach_execute_and_default_to_a_list(monkeypatch):
    seen = []
    monkeypatch.setattr(orphans, "execute", lambda settings, **kwargs: seen.append(kwargs) or 0)
    assert main([]) == 0 and main(["--delete"]) == 0 and main(["--delete", "--min-age-hours", "0.5"]) == 0
    assert seen == [{"delete": False, "min_age_hours": 24.0}, {"delete": True, "min_age_hours": 24.0},
                    {"delete": True, "min_age_hours": 0.5}]


def test_a_usage_error_is_exit_1_not_clicks_2(capsys):
    assert main(["--min-age-hours", "soon"]) == 1 and main(["--nonsense"]) == 1
    assert capsys.readouterr().err.count("error: ") == 2


def test_the_help_names_both_flags_and_the_warning_about_soft_delete(capsys):
    assert main(["--help"]) == 0
    text = " ".join(capsys.readouterr().out.split())
    assert "--delete" in text and "--min-age-hours" in text and "Soft delete is off" in text and "however young" in text


@pytest.mark.parametrize("arguments", [[], ["--delete"]])
def test_the_real_command_refuses_to_start_without_credentials(arguments):
    environ = {k: v for k, v in os.environ.items() if k != CREDENTIALS_VARIABLE}
    done = subprocess.run([sys.executable, "-m", "bronze.orphans", *arguments], cwd=ROOT, env=environ,
                          capture_output=True, text=True, timeout=120)
    assert done.returncode == 1 and "GOOGLE_APPLICATION_CREDENTIALS" in done.stderr and done.stdout == ""
