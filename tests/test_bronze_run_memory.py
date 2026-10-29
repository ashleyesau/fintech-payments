"""tests/test_bronze_run_memory.py: G3 step 3, pass 2 holds a fixed working set, not the table.

The whole write run is made in its own process (tests/bronze_run_probe.py) over a generated landing, into a local
warehouse, and the process's peak memory comes from the operating system. A watchdog ends it as soon as it passes
the ceiling, so a run that holds the rows fails in seconds. The big run is slow: 1.2M events, more than the real
run's 1,017,902, with the largest table at 1.17M rows against 997,650.

What was measured before the build (DECISION_LOG, "G3 question 1 evidence", sandbox, Python 3.11): pass 2 at
100,000-row batches with the index live peaks about 670 MB above the start; one batch of the whole table would
need about 5.0 GB as dict rows and 1.6 GB as Arrow. The ceiling is 1,000 MB (G3 question 1, Ashley): 50% above
that measurement, below one Arrow batch of the whole table, and a long way below dict rows.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
CEILING_MB = 1000
REAL_RUN_EVENTS, REAL_RUN_OBJECTS = 1_017_902, 908
TX_SIZE, MERCHANTS = 2000, 50                    # LazyLanding: 1,950 merchant_transactions and 50 merchants a transaction

# Started through a tiny process for the reason given in tests/test_bronze_memory.py: on Linux a child starts with
# its parent's peak memory as its floor, and the pytest process is big.
LAUNCHER = ("import subprocess, sys; "
            "r = subprocess.run([sys.executable, '-W', 'ignore', '-m', 'tests.bronze_run_probe', *sys.argv[1:]], "
            "capture_output=True, text=True); "
            "sys.stdout.write(r.stdout); sys.stderr.write(r.stderr); sys.exit(r.returncode)")


def probe(transactions: int, ceiling_mb: float, batch_rows: int, timeout: int, tmpdir=None):
    env = None if tmpdir is None else {**os.environ, "TMPDIR": str(tmpdir)}
    return subprocess.run([sys.executable, "-W", "ignore", "-c", LAUNCHER, str(transactions), str(ceiling_mb),
                           str(batch_rows)], cwd=ROOT, capture_output=True, text=True, timeout=timeout, env=env)


def test_the_probe_reports_what_the_run_wrote_and_removes_its_warehouse(tmp_path):
    run = probe(5, 5000, 3000, 300, tmpdir=tmp_path)
    assert run.returncode == 0, run.stderr
    report = json.loads(run.stdout)
    assert (report["objects"], report["events"]) == (10, 10_000)
    transactions_rows, merchants_rows = 5 * (TX_SIZE - MERCHANTS), 5 * MERCHANTS
    assert report["appended"] == {"merchants": merchants_rows, "loan_applications": 0, "loan_repayments": 0,
                                  "risk_events": 0, "merchant_transactions": transactions_rows}
    assert report["read_back"] == report["appended"]
    assert report["batches"]["merchants"] == 1 and report["batches"]["merchant_transactions"] == 3     # 1,950-row objects, two close a 3,000-row batch
    assert 0 < report["base"] < report["peak"]
    assert list(tmp_path.iterdir()) == []


def test_the_watchdog_ends_a_run_that_passes_the_ceiling():
    run = probe(20, 20, 100_000, 300)
    assert run.returncode == 7, (run.returncode, run.stdout, run.stderr)
    assert "ceiling exceeded" in run.stderr and "ceiling 20 MB" in run.stderr
    assert run.stdout == ""


@pytest.mark.slow
def test_a_write_larger_than_the_real_run_stays_inside_the_ceiling():
    run = probe(600, CEILING_MB, 100_000, 3000)
    assert run.returncode == 0, run.stderr
    report = json.loads(run.stdout)
    assert report["events"] > REAL_RUN_EVENTS and report["objects"] > REAL_RUN_OBJECTS
    assert report["appended"]["merchant_transactions"] == 600 * (TX_SIZE - MERCHANTS) > 997_650
    assert report["appended"]["merchants"] == 600 * MERCHANTS
    assert report["read_back"] == report["appended"]
    assert sum(report["appended"].values()) == report["events"]
    held = (report["peak"] - report["base"]) / 1_000_000
    assert held < CEILING_MB, held
    print(f"\n1.2M events, {report['objects']} objects, batches {report['batches']}: {held:.0f} MB above the start, "
          f"{report['seconds']} s")
