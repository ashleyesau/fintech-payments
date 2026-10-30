"""
scripts/measure_bronze_scale.py: P6, the Bronze write at 11M events, measured in the sandbox on a generated landing.

    PYTHONPATH=. python -u scripts/measure_bronze_scale.py --work /tmp/p6_scale

A 1,000-merchant year is about 11M events, which the real bucket cannot take within the free tier, so this measures
the same code over bronze.run.run on a local warehouse and a generated landing (tests/bronze_synth.py LazyLanding:
5,500 transactions of 2,000 changes, 11,000 objects by default, built on demand, nothing on disk). It makes the
write, a second run, and the landed-against-Bronze half of bronze.replay.check (no database), each with its time and
the process's memory sampled every few seconds, and the seconds of every Table.append (the commit).

HARD checks (the script exits 1 if one fails): every count is what the landing holds and what is read back; the
second run creates no snapshot, writes no marker and changes no file, catalog byte or bucket object; the check finds
no problem. FLAGS (reported, never a failure; a flag is a finding for a person to read): commits slow down by more
than 2x from the first tenth to the last, process memory grows by more than 15 MB per million rows appended during
the largest table's pass 2, the peak is above 4,000 MB (half the Air's 8 GiB), the write needs more files than the
free tier's Class A allowance leaves (each file the write creates is one Class A operation on GCS).

Honest limits. The rows are generated and repetitive, so Parquet sizes and compression time are not those of real
data (time and memory per event should be); one scale point (the 1.2M point is in DECISION_LOG); the Air is
estimated at 3.1 times the sandbox's seconds (P5: 13 min 25 s against 4 min 19 s), which is an estimate. It
deletes nothing: it refuses an existing work directory and prints the command to remove its own.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import threading
import time
from pathlib import Path
from typing import Callable, Optional

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table

from bronze.check import peak_memory_bytes
from bronze.landing import LandingReader
from bronze.replay import check
from bronze.run import NAMESPACE, TABLE_ORDER, run
from tests.bronze_synth import PREFIX, SOURCE_DB, LazyLanding
from tests.fakes import MemorySink

COMMIT_SLOWDOWN_FLAG = 2.0           # last tenth of the commits against the first tenth
GROWTH_FLAG_MB_PER_MILLION = 15.0    # a dict-row leak would be about 600
PEAK_FLAG_MB = 4_000                 # half the Air's 8 GiB
CLASS_A_LIMIT = 5_000                # the second-hand free-tier figure
AIR_SLOWDOWN = 3.1                   # P5: 13 min 25 s on the Air against 4 min 19 s in the sandbox (an estimate)
BIG_TABLE = "merchant_transactions"
MIN_SLOPE_SPAN = 1.0                 # million rows: a slope over a shorter stretch of the table is noise


# ---------------------------------------------------------------------------
# the small statistics
# ---------------------------------------------------------------------------

def slope(points: list[tuple[float, float]]) -> Optional[float]:
    """The least squares slope of y on x, or None if there are fewer than two different x."""
    if len({x for x, _ in points}) < 2:
        return None
    n = len(points)
    mean_x, mean_y = sum(x for x, _ in points) / n, sum(y for _, y in points) / n
    spread = sum((x - mean_x) ** 2 for x, _ in points)
    return sum((x - mean_x) * (y - mean_y) for x, y in points) / spread


def decile_ratio(values: list[float]) -> Optional[float]:
    """The mean of the last tenth of the values over the mean of the first tenth, or None below 10 values."""
    if len(values) < 10:
        return None
    tenth = len(values) // 10
    first, last = sum(values[:tenth]) / tenth, sum(values[-tenth:]) / tenth
    return None if first == 0 else last / first


def current_memory_bytes() -> int:
    """What the process holds now (VmRSS where /proc exists), else the peak so far."""
    try:
        with open("/proc/self/status") as status:
            for line in status:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return peak_memory_bytes()


# ---------------------------------------------------------------------------
# the measurement
# ---------------------------------------------------------------------------

class Sampler:
    """Records (seconds, phase, memory in bytes, rows of the big table appended so far) every `interval` seconds."""

    def __init__(self, interval: float) -> None:
        self.interval, self.phase, self.rows_done, self.samples = interval, "start", 0, []
        self._stop, self._started = threading.Event(), time.monotonic()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def _take(self) -> None:
        self.samples.append((round(time.monotonic() - self._started, 2), self.phase, current_memory_bytes(), self.rows_done))

    def _loop(self) -> None:
        while not self._stop.wait(self.interval):
            self._take()

    def __enter__(self) -> "Sampler":
        self._take()
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._thread.join()
        self._take()


def _tree(directory: Path) -> dict:
    return {str(p): (p.stat().st_size, p.stat().st_mtime_ns) for p in sorted(directory.rglob("*")) if p.is_file()}


def _snapshots(catalog) -> dict:
    return {name: [s.snapshot_id for s in catalog.load_table(f"{NAMESPACE}.{name}").snapshots()] for name in TABLE_ORDER}


def measure(transactions: int, work: Path, *, batch_rows: int = 100_000, sample_seconds: float = 10.0,
            say: Callable[[str], None] = lambda message: None) -> dict:
    """Make the measurements and return the facts (everything `evaluate` needs, all JSON-able)."""
    if work.exists():
        raise FileExistsError(f"{work} exists; choose a new --work directory (nothing is deleted)")
    work.mkdir(parents=True)
    lazy = LazyLanding(transactions)
    reader = LandingReader(lazy, PREFIX)
    warehouse_dir = work / "warehouse"
    warehouse = f"file://{warehouse_dir}"
    catalog_file = work / "catalog.db"
    catalog = SqlCatalog("bronze", uri=f"sqlite:///{catalog_file}", warehouse=warehouse)
    store = MemorySink()
    appends: list[dict] = []
    original_append = Table.append

    def timed_append(self, df, *args, **kwargs):
        started = time.monotonic()
        result = original_append(self, df, *args, **kwargs)
        appends.append({"table": self.name()[-1], "rows": df.num_rows, "seconds": round(time.monotonic() - started, 3)})
        return result

    facts: dict = {"transactions": transactions, "tx_size": lazy.tx_size, "merchants_per_tx": lazy.merchants,
                   "events_landed": lazy.events, "objects_landed": len(lazy._names), "batch_rows": batch_rows,
                   "phases": {}}

    def phase_end(name: str, started: float) -> None:
        facts["phases"][name] = {"seconds": round(time.monotonic() - started, 1),
                                 "peak_mb_so_far": round(peak_memory_bytes() / 1e6)}
        say(f"   {name}: {facts['phases'][name]['seconds']:,} s, peak so far {facts['phases'][name]['peak_mb_so_far']:,} MB")

    Table.append = timed_append
    try:
        with Sampler(sample_seconds) as sampler:
            pass_one_end: list[float] = []
            write_started = time.monotonic()

            def progress(done: int, total: int) -> None:
                if done == total and not pass_one_end:
                    pass_one_end.append(time.monotonic())
                    sampler.phase = "write: pass 2"

            def on_batch(batch) -> None:
                if batch.table == BIG_TABLE:
                    sampler.rows_done = batch.rows_done

            say("== write")
            sampler.phase = "write: pass 1"
            first = run(reader, catalog, store, catalog_file=catalog_file, warehouse=warehouse, source_db=SOURCE_DB,
                        write=True, batch_rows=batch_rows, progress=progress, on_batch=on_batch)
            facts["phases"]["write: pass 1"] = {"seconds": round(pass_one_end[0] - write_started, 1),
                                                "peak_mb_so_far": None}
            facts["phases"]["write: pass 2"] = {"seconds": round(time.monotonic() - pass_one_end[0], 1),
                                                "peak_mb_so_far": round(peak_memory_bytes() / 1e6)}
            phase_end("write (both passes)", write_started)
            facts["appended"] = {name: t.rows_appended for name, t in first.tables.items()}
            facts["batches"] = {name: t.batches for name, t in first.tables.items()}
            facts["landed"] = {name: t.landed for name, t in first.tables.items()}
            facts["marker_written"] = first.marker.written is not None
            facts["files_created"] = len(_tree(warehouse_dir))

            sampler.phase = "read back"
            started = time.monotonic()
            facts["read_back"] = {name: catalog.load_table(f"{NAMESPACE}.{name}").scan(selected_fields=("commit_lsn",))
                                  .to_arrow().num_rows for name in TABLE_ORDER}
            phase_end("read back", started)

            before = (_tree(warehouse_dir), _snapshots(catalog), hashlib.sha256(catalog_file.read_bytes()).hexdigest(),
                      dict(store.objects))
            say("== second run")
            sampler.phase = "second run"
            started = time.monotonic()
            second = run(reader, catalog, store, catalog_file=catalog_file, warehouse=warehouse, source_db=SOURCE_DB,
                         write=True, batch_rows=batch_rows)
            phase_end("second run", started)
            after = (_tree(warehouse_dir), _snapshots(catalog), hashlib.sha256(catalog_file.read_bytes()).hexdigest(),
                     dict(store.objects))
            facts["second_run"] = {
                "appended": sum(t.rows_appended for t in second.tables.values()),
                "created": sum(1 for t in second.tables.values() if t.created),
                "marker_written": second.marker.written is not None, "catalog_copied": second.marker.catalog_copied,
                "unchanged": dict(zip(("files", "snapshots", "catalog_bytes", "bucket_objects"),
                                      (a == b for a, b in zip(before, after))))}

            say("== replay check, landed against Bronze (no database)")
            sampler.phase = "check"
            started = time.monotonic()
            report = check(catalog, reader, None, source_db=SOURCE_DB, say=lambda m: say(f"   {m}"))
            phase_end("check", started)
            facts["check"] = {"problems": report.problems, "rows": {t: d.rows for t, d in report.bronze.items()}}
    finally:
        Table.append = original_append

    facts["peak_mb"] = round(peak_memory_bytes() / 1e6)
    facts["appends"] = appends
    facts["samples"] = [list(s) for s in sampler.samples]
    facts["work"] = str(work)
    return facts


# ---------------------------------------------------------------------------
# what the facts mean
# ---------------------------------------------------------------------------

def evaluate(facts: dict) -> dict:
    """The hard checks (name to bool) and the flags and figures (name to text or None) of a measurement."""
    expected_big = facts["transactions"] * (facts["tx_size"] - facts["merchants_per_tx"])
    expected_merchants = facts["transactions"] * facts["merchants_per_tx"]
    expected = {"merchants": expected_merchants, "loan_applications": 0, "loan_repayments": 0, "risk_events": 0,
                BIG_TABLE: expected_big}
    second = facts["second_run"]
    hard = {
        "every landed change was appended": facts["appended"] == expected == facts["landed"],
        "the appended rows are the landed events": sum(facts["appended"].values()) == facts["events_landed"],
        "the rows read back equal the rows appended": facts["read_back"] == facts["appended"],
        "the first run wrote a marker": bool(facts["marker_written"]),
        "the second run appended nothing and created nothing": second["appended"] == 0 and second["created"] == 0,
        "the second run wrote no marker and copied no catalog": not second["marker_written"] and not second["catalog_copied"],
        "the second run changed no file": bool(second["unchanged"]["files"]),
        "the second run changed no snapshot": bool(second["unchanged"]["snapshots"]),
        "the second run changed no catalog byte": bool(second["unchanged"]["catalog_bytes"]),
        "the second run changed no bucket object": bool(second["unchanged"]["bucket_objects"]),
        "the check found no problem": facts["check"]["problems"] == [],
        "the check counted every landed event": facts["check"]["rows"] == expected,
    }

    commits = [a["seconds"] for a in facts["appends"] if a["table"] == BIG_TABLE]
    ratio = decile_ratio(commits)
    points = [(rows / 1e6, memory / 1e6) for _, phase, memory, rows in facts["samples"]
              if phase == "write: pass 2" and rows > 0]
    growth = slope(points) if points and max(x for x, _ in points) - min(x for x, _ in points) >= MIN_SLOPE_SPAN else None
    classes = facts["files_created"] + 3          # plus the marker, the catalog copy and a listing of _runs/ (the landing is not Bronze's)
    figures = {
        "commit seconds, first tenth against last tenth (merchant_transactions)": None if ratio is None else f"x{ratio:.2f}",
        "memory growth in the big table's pass 2, MB per million rows": None if growth is None else f"{growth:.1f}",
        "peak process memory, MB": f"{facts['peak_mb']:,}",
        "files the write created (Class A on GCS), with the marker, the copy and a listing": f"{classes:,} of {CLASS_A_LIMIT:,}",
        "the write on the Air (estimate, sandbox seconds x 3.1), minutes": f"{(facts['phases']['write: pass 1']['seconds'] + facts['phases']['write: pass 2']['seconds']) * AIR_SLOWDOWN / 60:.0f}",
    }
    flags = []
    if ratio is not None and ratio > COMMIT_SLOWDOWN_FLAG:
        flags.append(f"commits slow down: the last tenth takes x{ratio:.2f} the first (flag above x{COMMIT_SLOWDOWN_FLAG:g})")
    if growth is not None and growth > GROWTH_FLAG_MB_PER_MILLION:
        flags.append(f"memory grows with the table: {growth:.1f} MB per million rows (flag above {GROWTH_FLAG_MB_PER_MILLION:g})")
    if facts["peak_mb"] > PEAK_FLAG_MB:
        flags.append(f"peak memory {facts['peak_mb']:,} MB is above {PEAK_FLAG_MB:,} MB, half of the Air's 8 GiB")
    if classes > CLASS_A_LIMIT:
        flags.append(f"the write needs about {classes:,} Class A operations, more than the {CLASS_A_LIMIT:,} a month")
    return {"hard": hard, "figures": figures, "flags": flags}


def render(facts: dict, verdict: dict) -> str:
    lines = [f"{facts['events_landed']:,} events in {facts['objects_landed']:,} objects ({facts['transactions']:,} transactions)", "",
             "phase                       seconds   peak MB so far"]
    for name, phase in facts["phases"].items():
        peak = "" if phase["peak_mb_so_far"] is None else f"{phase['peak_mb_so_far']:,}"
        lines.append(f"{name:<27} {phase['seconds']:>8,.1f} {peak:>16}")
    lines += ["", "table                    landed   appended  commits"]
    for name in TABLE_ORDER:
        lines.append(f"{name:<22} {facts['landed'][name]:>9,} {facts['appended'][name]:>10,} {facts['batches'][name]:>8,}")
    lines += ["", "figures (not gates)"] + [f"  {name}: {value if value is not None else 'too few points'}"
                                              for name, value in verdict["figures"].items()]
    lines += ["", "hard checks"] + [f"  {'ok  ' if ok else 'FAIL'} {name}" for name, ok in verdict["hard"].items()]
    lines += ["", "flags"] + ([f"  {flag}" for flag in verdict["flags"]] or ["  none"])
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--work", required=True, help="a new directory for the warehouse, the catalog and report.json")
    parser.add_argument("--transactions", type=int, default=5_500, help="transactions of 2,000 changes (5,500 is 11M events)")
    parser.add_argument("--batch-rows", type=int, default=100_000)
    parser.add_argument("--sample-seconds", type=float, default=10.0)
    args = parser.parse_args(argv)
    work = Path(args.work)
    try:
        facts = measure(args.transactions, work, batch_rows=args.batch_rows, sample_seconds=args.sample_seconds,
                        say=lambda message: print(message, flush=True))
    except FileExistsError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    verdict = evaluate(facts)
    (work / "report.json").write_text(json.dumps({"facts": facts, "verdict": verdict}, indent=1))
    print("\n" + render(facts, verdict))
    ok = all(verdict["hard"].values())
    print(f"\n{'PASS' if ok else 'FAIL'}: {sum(verdict['hard'].values())} of {len(verdict['hard'])} hard checks; "
          f"{len(verdict['flags'])} flags")
    print(f"\nLeft behind (nothing was deleted): rm -r {work}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
