"""
scripts/rehearse_bronze.py: the P4 rehearsal of the Bronze write, at full size, in the sandbox (no bucket, no Google).

    PYTHONPATH=. python scripts/rehearse_bronze.py --work /tmp/p4_rehearsal

It reruns the definition-of-done run of the CDC part and then everything the GCS phase does with it, on a local
warehouse, so the first real write finds its round-trip bugs here and not on the bucket (GCS Plan (V2) section 19, P4):

  1. a new database from the migrations and a replication slot, made BEFORE any data (a slot only sees what comes after it);
  2. the generator: seed 42, 100 merchants, 2025-01-01 to 2026-01-01 (the counts of the M6 run);
  3. the real consumer, drained once into a directory laid out like the bucket;
  4. cdc.reconcile on that directory: the landed objects equal the source;
  5. bronze.run.run on a local warehouse: the first run, then a second that must create no snapshot, write no marker
     and change no file or catalog byte;
  6. bronze.replay.check: every landed event equals the Bronze row mapped back, and the Bronze events replayed into a
     blank database equal the source, table by table, by the digest cdc.reconcile uses.

It prints one line per stage with its time and the process's peak memory so far, then a table, then PASS or FAIL
(exit 0 or 1). It never deletes anything: it refuses to start if the work directory, the database or the slot
exists, and it leaves them behind for you to look at. The commands to remove them are printed at the end. The
database and the slot live in the scratch Postgres of whoever runs it: do not point it at the fintech database.

The figures a pass must show are the M6 run's: 1,017,902 events (1,017,884 inserts and 18 updates): merchants 20,078;
merchant_transactions 997,650; risk_events 43; loan_applications 37 c and 18 u; loan_repayments 76.
"""
from __future__ import annotations

import argparse
import os
import resource
import subprocess
import sys
import time
from pathlib import Path

import psycopg
from psycopg import conninfo

from bronze.landing import LandingReader
from bronze.replay import check
from cdc.reconcile import MIGRATIONS
from simulator.config import settings
from tests.bronze_env import Env
from tests.file_sink import FileSink

EXPECTED = {"merchants": 20_078, "merchant_transactions": 997_650, "risk_events": 43, "loan_applications": 55,
            "loan_repayments": 76}
EXPECTED_TOTAL = 1_017_902


def peak_megabytes(who: int = resource.RUSAGE_SELF) -> float:
    peak = resource.getrusage(who).ru_maxrss      # kilobytes on Linux, bytes on macOS
    return peak / (1024 * 1024 if sys.platform == "darwin" else 1024)


class Rehearsal:
    def __init__(self, args) -> None:
        self.args = args
        self.work = Path(args.work)
        self.admin_dsn = conninfo.make_conninfo(settings.dsn, dbname="postgres")
        self.source_dsn = conninfo.make_conninfo(settings.dsn, dbname=args.database)
        self.stages: list[tuple[str, float, float, float]] = []
        self.failures: list[str] = []

    def stage(self, name):
        rehearsal = self

        class Timer:
            def __enter__(self):
                self.start = time.monotonic()
                print(f"== {name}", flush=True)

            def __exit__(self, kind, error, trace):
                took = time.monotonic() - self.start
                rehearsal.stages.append((name, took, peak_megabytes(), peak_megabytes(resource.RUSAGE_CHILDREN)))
                print(f"   {name}: {took:,.1f} s; peak so far: this process {peak_megabytes():,.0f} MB, "
                      f"largest child {peak_megabytes(resource.RUSAGE_CHILDREN):,.0f} MB", flush=True)

        return Timer()

    def fail(self, message: str) -> None:
        self.failures.append(message)
        print(f"FAIL: {message}", flush=True)

    def subprocess(self, name: str, command: list[str], env: dict) -> str:
        log = self.work / f"{name}.log"
        with open(log, "w") as out:
            code = subprocess.run(command, env={**os.environ, **env}, stdout=out, stderr=subprocess.STDOUT).returncode
        tail = log.read_text().splitlines()[-3:]
        print("   " + "\n   ".join(tail), flush=True)
        if code != 0:
            self.fail(f"{name} exited {code} (see {log})")
        return log.read_text()

    def preflight(self) -> bool:
        if self.work.exists():
            print(f"error: {self.work} exists; choose a new --work directory (nothing is deleted)", file=sys.stderr)
            return False
        with psycopg.connect(self.admin_dsn, autocommit=True) as admin:
            if admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.args.database,)).fetchone():
                print(f"error: database {self.args.database} exists; choose another --database", file=sys.stderr)
                return False
            if admin.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (self.args.slot,)).fetchone():
                print(f"error: slot {self.args.slot} exists; choose another --slot", file=sys.stderr)
                return False
        return True

    def create_database_and_slot(self) -> None:
        with psycopg.connect(self.admin_dsn, autocommit=True) as admin:
            admin.execute(f'CREATE DATABASE "{self.args.database}"')
        with psycopg.connect(self.source_dsn, autocommit=True) as conn:
            for path in MIGRATIONS:
                conn.execute(path.read_text())
            conn.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (self.args.slot,))
        print(f"   database {self.args.database} from {len(MIGRATIONS)} migrations, slot {self.args.slot} made before any data")

    def generate(self) -> None:
        self.subprocess("generator", [
            sys.executable, "-m", "simulator.main", "--mode", "backfill", "--start-date", "2025-01-01",
            "--end-date", "2026-01-01", "--merchant-count", str(self.args.merchants), "--seed", str(self.args.seed),
            "--checkpoint", str(self.work / "checkpoint.json")], {"POSTGRES_DB": self.args.database})

    def consume(self) -> None:
        self.subprocess("consumer", [sys.executable, "-m", "tests.consume_with_file_sink", str(self.work / "landing")],
                        {"CDC_DB": self.args.database, "CDC_SLOT_NAME": self.args.slot, "FILE_SINK_FOLLOW": "0"})

    def reconcile(self) -> None:
        text = self.subprocess("reconcile", [sys.executable, "-m", "cdc.reconcile", "--directory", str(self.work / "landing")],
                               {"CDC_DB": self.args.database, "CDC_SLOT_NAME": self.args.slot})
        if "RECONCILED:" not in text:
            self.fail("cdc.reconcile did not print RECONCILED")

    def bronze(self):
        prefix = f"cdc/{self.args.slot}/"
        reader = LandingReader(FileSink(self.work / "landing"), prefix)
        env = Env(self.work / "bronze")
        with self.stage("bronze.run, first run (write)"):
            first = env.run(reader, source_db=self.args.database, progress=None,
                            on_batch=lambda b: print(f"   {b.table}: batch {b.batch} committed, {b.rows:,} rows ({b.rows_done:,} of {b.rows_total:,})", flush=True))
        for name, report in first.tables.items():
            print(f"   {name}: landed {report.landed:,}, appended {report.rows_appended:,} in {report.batches} commits")
        before = (env.files(), env.snapshots(), env.catalog_digest(), dict(env.store.objects))
        with self.stage("bronze.run, second run (must change nothing)"):
            second = env.run(reader, source_db=self.args.database)
        after = (env.files(), env.snapshots(), env.catalog_digest(), dict(env.store.objects))
        if not first.marker.written:
            self.fail("the first run wrote no marker")
        if second.marker.written or second.marker.catalog_copied:
            self.fail("the second run wrote a marker or copied the catalog")
        if any(t.rows_appended or t.batches or t.created for t in second.tables.values()):
            self.fail("the second run appended rows or created a table")
        for label, a, b in zip(("files", "snapshots", "catalog bytes", "bucket objects"), before, after):
            if a != b:
                self.fail(f"the second run changed the {label}")
        print(f"   second run: appended {sum(t.rows_appended for t in second.tables.values())} rows, "
              f"marker written {second.marker.written}, objects landed {second.summary.objects:,}")
        return env, reader, first

    def run(self) -> int:
        if not self.preflight():
            return 1
        self.work.mkdir(parents=True)
        with self.stage("database and slot"):
            self.create_database_and_slot()
        with self.stage("generator (seed %d, %d merchants)" % (self.args.seed, self.args.merchants)):
            self.generate()
        if self.failures:
            return self.finish(None, None)
        with self.stage("consumer, drained once into a directory"):
            self.consume()
        with self.stage("cdc.reconcile on the directory"):
            self.reconcile()
        if self.failures:
            return self.finish(None, None)
        env, reader, first = self.bronze()
        with self.stage("bronze.replay.check (landed vs Bronze, Bronze replayed vs source)"):
            report = check(env.catalog, reader, self.source_dsn, source_db=self.args.database,
                           say=lambda message: print(f"   {message}", flush=True))
        for problem in report.problems:
            self.fail(problem)
        return self.finish(first, report)

    def finish(self, first, report) -> int:
        exact = self.args.merchants == 100 and self.args.seed == 42
        if report is not None:
            print("\ntable                  expected   landed   in Bronze  source rows  replayed rows")
            for table, expected in EXPECTED.items():
                source, replayed = report.source.get(table, (0, 0))[0], report.replayed.get(table, (0, 0))[0]
                print(f"{table:<22} {expected:>8,} {report.landed[table].rows:>8,} {report.bronze[table].rows:>10,} {source:>12,} {replayed:>14,}")
                if exact and report.landed[table].rows != expected:
                    self.fail(f"{table}: {report.landed[table].rows:,} changes landed, the M6 run had {expected:,}")
            total = sum(d.rows for d in report.landed.values())
            print(f"total events           {EXPECTED_TOTAL:>8,} {total:>8,} {sum(d.rows for d in report.bronze.values()):>10,};  updates found {report.updates}")
            if exact and total != EXPECTED_TOTAL:
                self.fail(f"{total:,} events landed, the M6 run had {EXPECTED_TOTAL:,}")
            if exact and report.updates != 18:
                self.fail(f"{report.updates} updates, the M6 run had 18")
            if not exact:
                print("(not the 100 merchant, seed 42 run: the M6 figures were not enforced)")
        print("\nstage                                                       seconds   this process MB   largest child MB")
        print("(peaks are the highest so far, not per stage; Bronze and the check run in this process, the others are children)")
        for name, took, peak, child in self.stages:
            print(f"{name:<58} {took:>9,.1f} {peak:>15,.0f} {child:>18,.0f}")
        print(f"\n{'PASS' if not self.failures else 'FAIL: ' + str(len(self.failures)) + ' problems'}")
        print("\nLeft behind (nothing was deleted). To remove them, in this order:")
        print(f"  psql -d postgres -c 'DROP DATABASE \"{self.args.database}\" WITH (FORCE)'   (also drops its slot {self.args.slot})")
        print(f"  rm -r {self.work}")
        return 0 if not self.failures else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--work", required=True, help="a new directory for the landing, the warehouse and the logs")
    parser.add_argument("--database", default="fintech_bronze_rehearsal", help="a new database, made from the migrations")
    parser.add_argument("--slot", default="cdc_rehearsal02", help="a new slot; its name is the landing prefix cdc/<slot>/")
    parser.add_argument("--merchants", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    return Rehearsal(parser.parse_args()).run()


if __name__ == "__main__":
    sys.exit(main())
