"""
scripts/cloud_crash_run.py: G4 step 8, the cloud crash run on the throwaway bucket.

    PYTHONPATH=. python scripts/cloud_crash_run.py --work /tmp/cloud_crash_run

Run as bronze-writer with GOOGLE_APPLICATION_CREDENTIALS set and BRONZE_BUCKET pointing at the
throwaway bucket (payments-512910-bronze-crash-test). Never set BRONZE_BUCKET to fintech_bronze
(the script refuses to start if you do).

What it does, in order:
  1. First write: a synthetic landing of 27 events across all five tables written to the throwaway
     bucket. Establishes the baseline digests.
  2. P1 crash + recovery: crash before the manifests are written (data-file orphans); recovery run
     appends only the missed rows; orphans found and all pass criteria met.
  3. P2 crash + recovery: crash after manifests + manifest list; recovery; orphans found.
  4. P3 crash + recovery: crash after the metadata json, before the catalog row; recovery; orphan found.
  5. Late-object write: a new transaction is added to the landing; a second run appends only its rows
     and writes a new marker; the old marker is kept.
  6. Rebuild + verify: the catalog is lost (a fresh SQLite file) and rebuilt from the newest marker;
     bronze.replay.check confirms the digests match the baseline.
  7. Refused-creates sample: the newest marker is written a second time (the name already exists);
     GCS returns 412 FAILED_PRECONDITION; put_if_absent returns False; the step costs nothing.

Exits 0 if every step passes, 1 if any step fails or the safety check fires. A P2 run of
scripts/storage_request_counts.py before and after this script measures the real cost.
"""
from __future__ import annotations

import argparse
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bronze.catalog_copy import rebuild_catalog
from bronze.config import BronzeSettings, require_credentials
from bronze.main import open_catalog
from bronze.marker import newest_marker
from bronze.orphans import scan
from bronze.replay import check
from bronze.run import TABLE_ORDER, run
from bronze.store import open_store
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from tests.bronze_env import busy_landing, landed, bronze_events, sort_events, SOURCE_DB, DATES
from tests.fakes import SimulatedCrash

import pyiceberg.table.update.snapshot as snap
from pyiceberg.catalog import MetastoreCatalog


# ---------------------------------------------------------------------------
# injection helpers (same as tests/test_bronze_crash.py)
# ---------------------------------------------------------------------------

class _P1Injector:
    def __init__(self, crash_on=1):
        self._crash_on, self._calls = crash_on, 0
        self._orig = snap._SnapshotProducer.__dict__["_commit"]

    def __enter__(self):
        orig, crash_on, state = self._orig, self._crash_on, self
        def patched(self_inner):
            state._calls += 1
            if state._calls == crash_on:
                raise SimulatedCrash(f"P1: after data files, before manifests")
            return orig(self_inner)
        snap._SnapshotProducer._commit = patched
        return self

    def __exit__(self, *_):
        snap._SnapshotProducer._commit = self._orig


class _P2Injector:
    def __init__(self, crash_on=1):
        self._crash_on, self._calls = crash_on, 0
        self._orig = snap._SnapshotProducer.__dict__["_commit"]

    def __enter__(self):
        orig, crash_on, state = self._orig, self._crash_on, self
        def patched(self_inner):
            result = orig(self_inner)
            state._calls += 1
            if state._calls == crash_on:
                raise SimulatedCrash(f"P2: after manifest list, before metadata json")
            return result
        snap._SnapshotProducer._commit = patched
        return self

    def __exit__(self, *_):
        snap._SnapshotProducer._commit = self._orig


class _P3Injector:
    def __init__(self, crash_on=1):
        self._crash_on, self._calls = crash_on, 0
        self._orig_desc = MetastoreCatalog.__dict__["_write_metadata"]
        self._orig_fn = MetastoreCatalog._write_metadata

    def __enter__(self):
        orig_fn, crash_on, state = self._orig_fn, self._crash_on, self
        def patched(metadata, io, metadata_path):
            orig_fn(metadata, io, metadata_path)
            state._calls += 1
            if state._calls == crash_on:
                raise SimulatedCrash(f"P3: after metadata json, before catalog row update")
        MetastoreCatalog._write_metadata = staticmethod(patched)
        return self

    def __exit__(self, *_):
        MetastoreCatalog._write_metadata = self._orig_desc


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _utc():
    return datetime.now(timezone.utc)


def _open(catalog_path: Path, settings: BronzeSettings) -> tuple:
    """(catalog, store) for the current catalog_path."""
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog(
        "bronze",
        uri=f"sqlite:///{catalog_path}",
        warehouse=f"gs://{settings.bucket}",
    )
    store = open_store(settings)
    return catalog, store, catalog_path


def _run(catalog: Catalog, store, land, settings: BronzeSettings,
         catalog_path: Path = None, **kwargs):
    kwargs.setdefault("write", True)
    kwargs.setdefault("warehouse", f"gs://{settings.bucket}")
    kwargs.setdefault("source_db", SOURCE_DB)
    if catalog_path is not None:
        kwargs.setdefault("catalog_file", catalog_path)
    return run(land.reader(), catalog, store, **kwargs)


def _verify(catalog: Catalog, land, *, label: str) -> bool:
    r = check(catalog, land.reader(), None, source_db=SOURCE_DB)
    if r.ok:
        print(f"  verify [{label}]: PASSED — {sum(d.rows for d in r.landed.values())} events, "
              f"digests match")
    else:
        print(f"  verify [{label}]: FAILED — {r.problems[:3]}")
    return r.ok


def _check_orphans(catalog, store, *, label: str, expect_orphans: bool,
                   bucket_name: str = "") -> bool:
    found = scan(catalog, store, clock=_utc)
    n = len(found.orphans)
    ok = (n > 0) if expect_orphans else True
    used_keys = {key for keys in found.used.values() for key in keys}
    warehouse = f"gs://{bucket_name}/"
    bad = [o.key for o in found.orphans if warehouse + o.key in used_keys]
    status = "OK" if (ok and not bad and found.ok) else "FAILED"
    print(f"  orphans [{label}]: {status} — {n} orphan(s), "
          f"{'none referenced' if not bad else 'SOME REFERENCED: ' + str(bad)}, "
          f"scan clean={found.ok}")
    return status == "OK"


def _say(msg: str):
    print(f"    {msg}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--work", required=True, type=Path,
                        help="directory for the catalog file; existing directory is reused")
    args = parser.parse_args()

    args.work.mkdir(parents=True, exist_ok=True)
    catalog_path = args.work / "catalog.db"

    settings = BronzeSettings()

    # Safety: refuse to run against the real Bronze bucket
    if settings.bucket == "fintech_bronze":
        print("error: BRONZE_BUCKET is fintech_bronze; set it to the throwaway bucket "
              "(payments-512910-bronze-crash-test) before running this script", file=sys.stderr)
        sys.exit(1)

    credentials = require_credentials()
    print(f"cloud crash run on gs://{settings.bucket} (credentials {credentials})")
    print(f"catalog: {catalog_path}")
    print()

    all_ok = True
    t_start = time.monotonic()

    catalog, store, catalog_path = _open(catalog_path, settings)

    # -----------------------------------------------------------------------
    # step 1: baseline write
    # -----------------------------------------------------------------------
    print("step 1: baseline write")
    land = busy_landing()
    report = _run(catalog, store, land, settings, catalog_path=catalog_path)
    total = sum(t.rows_appended for t in report.tables.values())
    print(f"  {total} rows appended in "
          f"{sum(t.batches for t in report.tables.values())} batch(es) "
          f"({'first write' if total > 0 else 'no-op resume'})")
    baseline_ok = _verify(catalog, land, label="baseline")
    m1 = newest_marker(store)
    print(f"  marker M1: {m1.run_id}")
    all_ok = all_ok and baseline_ok

    # -----------------------------------------------------------------------
    # steps 2-4: P1, P2, P3 crash + recovery
    # Each extends the landing with one new transaction so there is exactly one
    # new commit for the injector to crash on.
    # -----------------------------------------------------------------------
    for step_num, (label, Injector) in enumerate([
        ("P1 crash (after data files, before manifests)", _P1Injector),
        ("P2 crash (after manifest list, before metadata json)", _P2Injector),
        ("P3 crash (after metadata json, before catalog row update)", _P3Injector),
    ], start=2):
        print(f"\nstep {step_num}: {label}")
        # New transaction so there is always one commit to crash on
        land.transaction(("merchants", "c"), ("merchant_transactions", "c"),
                         ("risk_events", "c"), date=DATES[step_num % len(DATES)])
        land.flush()

        with Injector(crash_on=1):
            try:
                _run(catalog, store, land, settings, catalog_path=catalog_path)
                print(f"  FAILED: no crash raised")
                all_ok = False
            except SimulatedCrash as e:
                print(f"  crash: {e}")

        p_orphans_ok = _check_orphans(catalog, store, label=f"{label[:2]} before recovery",
                                      expect_orphans=True, bucket_name=settings.bucket)
        _run(catalog, store, land, settings, catalog_path=catalog_path)
        p_verify_ok = _verify(catalog, land, label=f"{label[:2]} after recovery")
        p_orphans_clean = _check_orphans(catalog, store, label=f"{label[:2]} after recovery",
                                         expect_orphans=False, bucket_name=settings.bucket)
        all_ok = all_ok and p_orphans_ok and p_verify_ok and p_orphans_clean

    # -----------------------------------------------------------------------
    # step 5: late object
    # -----------------------------------------------------------------------
    print("\nstep 5: late object")
    land.transaction(("merchants", "c"), ("merchant_transactions", "c"),
                     ("merchant_transactions", "c"), ("risk_events", "c"), date=DATES[0])
    land.flush()
    m_before_late = newest_marker(store)
    report = _run(catalog, store, land, settings, catalog_path=catalog_path)
    appended = {t: report.tables[t].rows_appended for t in TABLE_ORDER
                if report.tables[t].rows_appended > 0}
    m_after_late = newest_marker(store)
    all_markers = store.list_keys("_runs/")
    late_ok = (m_after_late.run_id != m_before_late.run_id
               and any(m_before_late.run_id in k for k in all_markers))
    print(f"  late rows appended: {appended}")
    print(f"  old marker kept: {any(m_before_late.run_id in k for k in all_markers)}, "
          f"new marker written: {m_after_late.run_id != m_before_late.run_id}, "
          f"total markers: {len(all_markers)}")
    late_verify_ok = _verify(catalog, land, label="late object")
    all_ok = all_ok and late_ok and late_verify_ok

    # -----------------------------------------------------------------------
    # step 6: rebuild + verify
    # -----------------------------------------------------------------------
    print("\nstep 6: rebuild from newest marker + verify")
    scratch_catalog_path = args.work / "scratch_catalog.db"
    if scratch_catalog_path.exists():
        scratch_catalog_path.unlink()
    scratch_catalog = SqlCatalog(
        "bronze",
        uri=f"sqlite:///{scratch_catalog_path}",
        warehouse=f"gs://{settings.bucket}",
    )
    marker_used = rebuild_catalog(scratch_catalog, store)
    print(f"  rebuilt from marker: {marker_used.run_id}")
    rebuild_verify_ok = _verify(scratch_catalog, land, label="rebuilt catalog")
    r_rebuilt = check(scratch_catalog, land.reader(), None, source_db=SOURCE_DB)
    r_original = check(catalog, land.reader(), None, source_db=SOURCE_DB)
    digests_match = all(r_rebuilt.bronze[t].total == r_original.bronze[t].total
                        for t in TABLE_ORDER)
    print(f"  rebuilt digests match original catalog: {digests_match}")
    all_ok = all_ok and rebuild_verify_ok and digests_match

    # -----------------------------------------------------------------------
    # step 7: refused-creates sample
    # -----------------------------------------------------------------------
    print("\nstep 7: refused-creates sample")
    existing_marker_key = store.list_keys("_runs/")[-1]
    result = store.put_if_absent(existing_marker_key, b'{"test": "refused_create_sample"}')
    print(f"  put_if_absent on existing key {existing_marker_key!r}: "
          f"{'created (UNEXPECTED)' if result else 'refused (expected)'}")
    refused_ok = not result
    all_ok = all_ok and refused_ok

    # -----------------------------------------------------------------------
    # summary
    # -----------------------------------------------------------------------
    elapsed = time.monotonic() - t_start
    print(f"\n{'PASSED' if all_ok else 'FAILED'}: all steps in "
          f"{int(elapsed // 60)} min {int(elapsed % 60)} s")
    print(f"work directory: {args.work} (remove when done)")
    print(f"bucket: gs://{settings.bucket}")
    print("run scripts/storage_request_counts.py after this script to record the cost")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
