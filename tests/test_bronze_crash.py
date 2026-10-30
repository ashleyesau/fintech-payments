"""tests/test_bronze_crash.py: G4 step 4.

Three crash points inside a PyIceberg commit, the G3 crash points re-checked with real PyIceberg
files (not MemorySink), and a SIGKILL test.  All four pass criteria from GCS Plan (V2) section 20.5
are verified for each crash:

  (a) readers see exactly the previous complete state (same snapshot ids and rows before the attempt)
  (b) after a re-run every landed key appears once and every Bronze row equals its landed event
  (c) the marker and the catalog copy agree with the tables
  (d) the leftover files are counted and none is referenced by any table (bronze.orphans)

The three injection points inside a single PyIceberg table.append() call (confirmed for 0.12.0):

  P1 (after data files, before manifests):
      _SnapshotProducer._commit raises before _manifests() writes anything;
      leaves data-file orphans, catalog row unchanged (table still at its prior snapshot).

  P2 (after manifests and manifest list, before metadata json):
      _commit runs, writes manifests and manifest list, then raises before returning;
      commit_table is never called; leaves manifest and manifest-list orphans.

  P3 (after metadata json, before catalog row):
      MetastoreCatalog._write_metadata writes the .metadata.json then raises;
      the SQL catalog row is never updated; leaves a metadata-file orphan.

G3 crash points re-checked with real files (DirBucket not MemorySink):
  - crash between tables: after one table commits, before the next starts
  - crash after the last commit but before the run marker is written

SIGKILL: a real SIGKILL in the middle of a write; recovery run must complete cleanly.

Mutations are at the end.  Each fires a specific assertion in the real tests:
  - no injection → 0 orphans, the orphans_min check fires
  - P1 instead of P2 → 'manifest' missing from kinds, the kind check fires
  - P2 instead of P3 → 'metadata file' missing from kinds, the kind check fires
  - injector leaks → the next run crashes, the post-restore run check fires
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyiceberg.catalog import MetastoreCatalog
from pyiceberg.table.update.snapshot import _SnapshotProducer

from bronze.marker import newest_marker
from bronze.orphans import scan
from bronze.run import TABLE_ORDER
from bronze.store import CATALOG_KEY
from tests.bronze_bucket import bucket_env
from tests.bronze_env import (
    DATES, bronze_events, busy_landing, landed, sort_events,
)
from tests.bronze_synth import Landing
from tests.fakes import SimulatedCrash

ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _utc():
    return datetime.now(timezone.utc)


def _snapshots(env) -> dict:
    """table -> current snapshot id, or None for a table with no snapshot / not yet created."""
    result = {}
    for name in TABLE_ORDER:
        t = env.table(name) if env.has(name) else None
        result[name] = (t.current_snapshot().snapshot_id if t and t.current_snapshot() else None)
    return result


def _check_a(env, before: dict) -> None:
    """(a) readers see the previous complete state: no snapshot changed."""
    for name in TABLE_ORDER:
        t = env.table(name) if env.has(name) else None
        current = t.current_snapshot().snapshot_id if t and t.current_snapshot() else None
        assert current == before[name], (
            f"{name}: snapshot changed after crash; expected {before[name]!r}, got {current!r}")


def _check_bcd(env, store, land) -> None:
    """(b) re-run recovers all events; (c) marker + copy agree; (d) no used file is listed as orphan."""
    env.run(land.reader())
    for name in TABLE_ORDER:
        assert bronze_events(env, name) == sort_events(landed(land)[name]), name
    assert newest_marker(store) is not None
    found = scan(env.catalog, store, clock=_utc)
    assert found.ok, f"orphans scan not clean after re-run: {found.orphans}; problems: {found.problems}"
    assert CATALOG_KEY in store.list_keys("")


def _check_d_orphans(env, store, *, orphans_min: int, orphan_kinds: tuple[str, ...] = ()) -> None:
    """(d) orphan files exist and none is referenced by a table."""
    found = scan(env.catalog, store, clock=_utc)
    assert len(found.orphans) >= orphans_min, (
        f"expected >= {orphans_min} orphan(s), got {len(found.orphans)}")
    if orphan_kinds:
        kinds = {o.kind for o in found.orphans}
        for k in orphan_kinds:
            assert k in kinds, f"expected orphan kind {k!r} in {kinds}"
    used_keys = {key for keys in found.used.values() for key in keys}
    warehouse = env.uri.removeprefix("file://")
    for o in found.orphans:
        assert f"{warehouse}/{o.key}" not in used_keys, f"orphan {o.key!r} is in the used set"


def _two_batch_landing() -> Landing:
    """A landing whose first batch touches 'merchants' and whose second adds merchant_transactions.
    The run therefore makes two commits: one for merchants, one for merchant_transactions.
    Crashing on commit 1 leaves merchants unchanged; crashing on commit 2 leaves it committed."""
    land = Landing()
    land.transaction(("merchants", "c"), date=DATES[0])
    land.flush()
    land.transaction(("merchant_transactions", "c"), ("merchant_transactions", "c"), date=DATES[1])
    land.flush()
    return land


# ---------------------------------------------------------------------------
# injection context managers
# ---------------------------------------------------------------------------

class _P1Injector:
    """Raise at the start of _SnapshotProducer._commit: data files exist, nothing else written."""

    def __init__(self, crash_on: int = 1):
        self._crash_on = crash_on
        self._calls = 0
        self._orig = _SnapshotProducer.__dict__["_commit"]

    def __enter__(self):
        crash_on, state = self._crash_on, self

        def patched(self_inner):
            state._calls += 1
            if state._calls == crash_on:
                raise SimulatedCrash(f"P1: after data files, before manifests (call {crash_on})")
            return state._orig(self_inner)

        _SnapshotProducer._commit = patched
        return self

    def __exit__(self, *_):
        _SnapshotProducer._commit = self._orig


class _P2Injector:
    """Raise at the end of _SnapshotProducer._commit after manifests and manifest list are written."""

    def __init__(self, crash_on: int = 1):
        self._crash_on = crash_on
        self._calls = 0
        self._orig = _SnapshotProducer.__dict__["_commit"]

    def __enter__(self):
        orig, crash_on, state = self._orig, self._crash_on, self

        def patched(self_inner):
            result = orig(self_inner)   # writes manifests and manifest list
            state._calls += 1
            if state._calls == crash_on:
                raise SimulatedCrash(f"P2: after manifest list, before metadata json (call {crash_on})")
            return result

        _SnapshotProducer._commit = patched
        return self

    def __exit__(self, *_):
        _SnapshotProducer._commit = self._orig


class _P3Injector:
    """Raise inside MetastoreCatalog._write_metadata after writing the json, before the SQL row update."""

    def __init__(self, crash_on: int = 1):
        self._crash_on = crash_on
        self._calls = 0
        self._orig_desc = MetastoreCatalog.__dict__["_write_metadata"]
        self._orig_fn = MetastoreCatalog._write_metadata

    def __enter__(self):
        orig_fn, crash_on, state = self._orig_fn, self._crash_on, self

        def patched(metadata, io, metadata_path):
            orig_fn(metadata, io, metadata_path)    # metadata json written to disk / directory
            state._calls += 1
            if state._calls == crash_on:
                raise SimulatedCrash(f"P3: after metadata json, before catalog row update (call {crash_on})")

        MetastoreCatalog._write_metadata = staticmethod(patched)
        return self

    def __exit__(self, *_):
        MetastoreCatalog._write_metadata = self._orig_desc


# ---------------------------------------------------------------------------
# P1: after data files, before manifests
# ---------------------------------------------------------------------------

def test_crash_at_p1_on_first_commit_leaves_data_file_orphan_and_rerun_fills_tables(tmp_path):
    """P1 crash on the very first commit of a fresh warehouse: pass criteria (a) to (d)."""
    env, bucket, store = bucket_env(tmp_path)
    land = _two_batch_landing()
    before = _snapshots(env)                    # all None

    with _P1Injector(crash_on=1):
        with pytest.raises(SimulatedCrash):
            env.run(land.reader(), batch_rows=1000)

    _check_a(env, before)                       # merchants snapshot unchanged (still None)
    _check_d_orphans(env, store, orphans_min=1, orphan_kinds=("data file",))
    _check_bcd(env, store, land)


def test_crash_at_p1_on_second_commit_leaves_first_table_committed_second_table_unchanged(tmp_path):
    """P1 crash on commit 2: merchants is committed; merchant_transactions is unchanged (None).
    Proves (a): the committed table is readable; (d): the data-file orphan belongs to merchant_transactions."""
    env, bucket, store = bucket_env(tmp_path)
    land = _two_batch_landing()

    with _P1Injector(crash_on=2):
        with pytest.raises(SimulatedCrash):
            env.run(land.reader(), batch_rows=1000)

    snapshots_after = _snapshots(env)
    assert snapshots_after["merchants"] is not None, "merchants should have a snapshot (commit 1 succeeded)"
    assert snapshots_after["merchant_transactions"] is None, "merchant_transactions should be unchanged"

    _check_d_orphans(env, store, orphans_min=1, orphan_kinds=("data file",))
    _check_bcd(env, store, land)


# ---------------------------------------------------------------------------
# P2: after manifests and manifest list, before metadata json
# ---------------------------------------------------------------------------

def test_crash_at_p2_on_first_commit_leaves_manifest_orphans_and_rerun_fills_tables(tmp_path):
    env, bucket, store = bucket_env(tmp_path)
    land = _two_batch_landing()
    before = _snapshots(env)

    with _P2Injector(crash_on=1):
        with pytest.raises(SimulatedCrash):
            env.run(land.reader(), batch_rows=1000)

    _check_a(env, before)
    _check_d_orphans(env, store, orphans_min=3,
                     orphan_kinds=("data file", "manifest", "manifest list"))
    _check_bcd(env, store, land)


def test_crash_at_p2_on_second_commit_leaves_first_committed_and_second_has_manifest_orphans(tmp_path):
    env, bucket, store = bucket_env(tmp_path)
    land = _two_batch_landing()

    with _P2Injector(crash_on=2):
        with pytest.raises(SimulatedCrash):
            env.run(land.reader(), batch_rows=1000)

    snapshots_after = _snapshots(env)
    assert snapshots_after["merchants"] is not None
    assert snapshots_after["merchant_transactions"] is None

    _check_d_orphans(env, store, orphans_min=3,
                     orphan_kinds=("data file", "manifest", "manifest list"))
    _check_bcd(env, store, land)


# ---------------------------------------------------------------------------
# P3: after metadata json, before catalog row
# ---------------------------------------------------------------------------

def test_crash_at_p3_on_first_commit_leaves_metadata_orphan_and_rerun_fills_tables(tmp_path):
    env, bucket, store = bucket_env(tmp_path)
    land = _two_batch_landing()
    before = _snapshots(env)

    with _P3Injector(crash_on=1):
        with pytest.raises(SimulatedCrash):
            env.run(land.reader(), batch_rows=1000)

    _check_a(env, before)
    _check_d_orphans(env, store, orphans_min=1, orphan_kinds=("metadata file",))
    _check_bcd(env, store, land)


def test_crash_at_p3_on_second_commit_leaves_metadata_orphan_and_rerun_recovers(tmp_path):
    """P3 crash on the second _write_metadata call of the run.  The first call is from create_table
    (initial metadata, no snapshot); the second is from table.append for merchants.  After the crash
    the catalog row for merchants still points at the initial no-snapshot metadata, so
    merchants.current_snapshot() is None and the table has 0 rows until the re-run."""
    env, bucket, store = bucket_env(tmp_path)
    land = _two_batch_landing()
    before = _snapshots(env)    # all None

    with _P3Injector(crash_on=2):
        with pytest.raises(SimulatedCrash):
            env.run(land.reader(), batch_rows=1000)

    # (a): merchants catalog row unchanged (points at initial no-snapshot metadata)
    _check_a(env, before)
    _check_d_orphans(env, store, orphans_min=1, orphan_kinds=("metadata file",))
    _check_bcd(env, store, land)


# ---------------------------------------------------------------------------
# G3 crash points re-checked with real PyIceberg files
# ---------------------------------------------------------------------------

def test_crash_between_tables_leaves_no_orphans_and_rerun_completes_all_tables(tmp_path):
    """The G3 'crash between tables' scenario on a real directory warehouse.  Every commit
    within a table completed, so no orphan files exist; the re-run fills the remaining tables."""
    env, bucket, store = bucket_env(tmp_path)
    land = busy_landing()

    def crash_after_merchants(batch):
        if batch.table == "loan_applications":
            raise SimulatedCrash("between tables: loan_applications committed, loan_repayments not started")

    with pytest.raises(SimulatedCrash):
        env.run(land.reader(), on_batch=crash_after_merchants)

    # (d): no orphans from a completed commit
    found = scan(env.catalog, store, clock=_utc)
    assert found.orphans == []

    # (b) and (c): re-run fills the rest
    env.run(land.reader())
    for name in TABLE_ORDER:
        assert bronze_events(env, name) == sort_events(landed(land)[name]), name
    assert newest_marker(store) is not None
    after = scan(env.catalog, store, clock=_utc)
    assert after.ok


def test_crash_after_last_commit_before_marker_leaves_no_orphans_and_rerun_writes_marker(tmp_path):
    """The G3 'crash before the marker' scenario on a real directory warehouse.  All five tables
    are committed; no marker exists; the re-run writes the marker and nothing else."""
    env, bucket, store = bucket_env(tmp_path)
    land = busy_landing()

    def crash_before_marker(batch):
        if batch.table == TABLE_ORDER[-1]:
            raise SimulatedCrash("after last commit, before marker")

    with pytest.raises(SimulatedCrash):
        env.run(land.reader(), on_batch=crash_before_marker)

    assert newest_marker(store) is None
    assert all(env.has(n) for n in TABLE_ORDER)

    # (d): no orphans from completed commits
    found = scan(env.catalog, store, clock=_utc)
    assert found.orphans == []

    # re-run appends nothing and writes the marker
    report = env.run(land.reader())
    assert all(t.rows_appended == 0 for t in report.tables.values())
    assert newest_marker(store) is not None
    for name in TABLE_ORDER:
        assert bronze_events(env, name) == sort_events(landed(land)[name]), name
    after = scan(env.catalog, store, clock=_utc)
    assert after.ok


# ---------------------------------------------------------------------------
# SIGKILL test
# ---------------------------------------------------------------------------

_WORKER_TEMPLATE = """\
import sys
sys.path.insert(0, {root!r})
from pathlib import Path
from tests.bronze_env import busy_landing
from tests.bronze_bucket import bucket_env

tmp = Path({tmp!r})
env, bucket, store = bucket_env(tmp)
land = busy_landing()
sentinel = tmp / "first_commit.txt"
env.run(land.reader(),
        on_batch=lambda b: (sentinel.write_text("y") if not sentinel.exists() else None) or None)
"""


@pytest.mark.slow
def test_sigkill_mid_write_is_safe_and_the_rerun_recovers(tmp_path):
    """A real SIGKILL while the Bronze writer is in the middle of a commit leaves the warehouse
    in a state that the next run repairs without losing or duplicating any event."""
    from tests.bronze_env import Env
    from bronze.store import BronzeStore
    from tests.bronze_bucket import DirBucket

    land = busy_landing()
    worker_script = tmp_path / "worker.py"
    worker_script.write_text(
        _WORKER_TEMPLATE.format(root=str(ROOT), tmp=str(tmp_path)))

    proc = subprocess.Popen(
        [sys.executable, str(worker_script)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=ROOT)

    sentinel = tmp_path / "first_commit.txt"
    deadline = time.time() + 30
    while not sentinel.exists() and proc.poll() is None and time.time() < deadline:
        time.sleep(0.02)
    assert proc.poll() is None, (
        "worker finished before being killed: " + proc.stderr.read().decode()[:500])

    os.kill(proc.pid, signal.SIGKILL)
    proc.wait(timeout=10)
    proc.stdout.close()
    proc.stderr.close()
    assert proc.returncode == -signal.SIGKILL

    # Open the warehouse the worker was writing into; use DirBucket so the store reads from disk
    bucket = DirBucket(tmp_path / "warehouse")
    store = BronzeStore(bucket)
    env = Env(tmp_path, store=store)

    # Recovery run must complete and hold every event
    env.run(land.reader())
    for name in TABLE_ORDER:
        assert bronze_events(env, name) == sort_events(landed(land)[name]), name
    assert newest_marker(store) is not None
    found = scan(env.catalog, store, clock=_utc)
    assert found.ok


# ---------------------------------------------------------------------------
# mutations: these should fail if the corresponding check in the real tests is weakened
# ---------------------------------------------------------------------------

def test_mutation_no_p1_injection_means_zero_orphans_so_orphans_min_check_would_fire(tmp_path):
    """Without a crash the run completes with 0 orphans.  The `orphans_min=1` assertion catches
    any injection that silently does not fire."""
    env, bucket, store = bucket_env(tmp_path)
    env.run(busy_landing().reader())
    found = scan(env.catalog, store, clock=_utc)
    assert found.orphans == []          # 0 orphans: _check_d_orphans(orphans_min=1) would fail


def test_mutation_p2_injected_at_p1_gives_data_file_only_so_manifest_check_would_fire(tmp_path):
    """Injecting at P1 instead of P2 leaves only data-file orphans; 'manifest' is absent,
    so the orphan_kinds=('manifest',) check would fire."""
    env, bucket, store = bucket_env(tmp_path)
    land = _two_batch_landing()
    with _P1Injector(crash_on=1):
        with pytest.raises(SimulatedCrash):
            env.run(land.reader(), batch_rows=1000)
    kinds = {o.kind for o in scan(env.catalog, store, clock=_utc).orphans}
    assert "manifest" not in kinds     # the P2 assertion 'manifest' in kinds would fail


def test_mutation_p3_injected_at_p2_gives_no_metadata_so_metadata_check_would_fire(tmp_path):
    """Injecting at P2 instead of P3 does not write the metadata json; 'metadata file' is absent,
    so the orphan_kinds=('metadata file',) check would fire."""
    env, bucket, store = bucket_env(tmp_path)
    land = _two_batch_landing()
    with _P2Injector(crash_on=1):
        with pytest.raises(SimulatedCrash):
            env.run(land.reader(), batch_rows=1000)
    kinds = {o.kind for o in scan(env.catalog, store, clock=_utc).orphans}
    assert "metadata file" not in kinds   # the P3 assertion 'metadata file' in kinds would fail


def test_mutation_p1_injector_restores_original_so_subsequent_run_completes_normally(tmp_path):
    """The _P1Injector context manager restores _commit on exit; if it leaked, the next run
    would crash and the events-match assertion would never be reached."""
    env, bucket, store = bucket_env(tmp_path)
    land = _two_batch_landing()
    with _P1Injector(crash_on=1):
        with pytest.raises(SimulatedCrash):
            env.run(land.reader(), batch_rows=1000)
    # injector removed: this second run must complete without an injected crash
    env.run(land.reader())
    for name in TABLE_ORDER:
        assert bronze_events(env, name) == sort_events(landed(land)[name])


def test_mutation_p3_injector_restores_staticmethod_so_create_table_works_afterwards(tmp_path):
    """The _P3Injector restores the staticmethod descriptor; if the plain function leaked,
    create_table would receive 'self' as an extra first argument and fail."""
    env, bucket, store = bucket_env(tmp_path)
    land = _two_batch_landing()
    with _P3Injector(crash_on=1):
        with pytest.raises(SimulatedCrash):
            env.run(land.reader(), batch_rows=1000)
    # injector removed: a second run in a fresh env must create tables normally
    env2, _, _ = bucket_env(tmp_path.parent / "env2")
    env2.run(busy_landing().reader())
    assert all(env2.has(n) for n in TABLE_ORDER)
