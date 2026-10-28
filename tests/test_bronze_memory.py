"""tests/test_bronze_memory.py: G1 step 4, pass 1 holds a fixed working set, not the run.

The index is built in its own process (tests/bronze_memory_probe.py) over a generated landing, and the
process's peak memory comes from the operating system. A watchdog in the probe ends it as soon as it
passes the ceiling, so an index that holds events fails in seconds. The big run is slow: 1.2M events,
more than the real run's 1,017,902.

What was measured (sandbox, Python 3.11, 8 workers, 2,000-change transactions): the peak is about 225 MB
above the start with 100,000 events and 253 MB with 1,200,000, so nearly all of it is the objects being
read (8 at a time) and the index adds about 25 MB over a million events. The ceiling is 400 MB: 58% above
what was measured, and below what a dict of tuples per event would need (about 330 MB more).
"""
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
CEILING_MB = 400
REAL_RUN_EVENTS, REAL_RUN_OBJECTS = 1_017_902, 908


def probe(transactions: int, ceiling_mb: float, timeout: int):
    return subprocess.run([sys.executable, "-W", "ignore", "-m", "tests.bronze_memory_probe", str(transactions), str(ceiling_mb)],
                          cwd=ROOT, capture_output=True, text=True, timeout=timeout)


def test_the_probe_reports_what_it_built():
    run = probe(5, 1000, 120)
    assert run.returncode == 0, run.stderr
    report = json.loads(run.stdout)
    assert (report["objects"], report["events"], report["unique"], report["incomplete"]) == (10, 10_000, 10_000, 0)
    assert 0 < report["base"] < report["peak"]


def test_the_watchdog_ends_a_run_that_passes_the_ceiling():
    run = probe(20, 20, 120)
    assert run.returncode == 7, (run.returncode, run.stdout, run.stderr)
    assert "ceiling exceeded" in run.stderr and "ceiling 20 MB" in run.stderr
    assert run.stdout == ""


@pytest.mark.slow
def test_a_set_larger_than_the_real_run_is_read_within_the_ceiling():
    run = probe(600, CEILING_MB, 1500)
    assert run.returncode == 0, run.stderr
    report = json.loads(run.stdout)
    assert report["events"] == report["unique"] > REAL_RUN_EVENTS
    assert report["objects"] > REAL_RUN_OBJECTS and report["incomplete"] == 0
    held = (report["peak"] - report["base"]) / 1_000_000
    assert held < CEILING_MB, held
    print(f"\n1.2M events, {report['objects']} objects: {held:.0f} MB above the start, {report['seconds']} s")
