"""tests/test_bronze_verify_step.py: the bronze_verify step of the DAG (scripts/verify_unless_unchanged.sh).

The step runs `python -m bronze.check` unless the newest run marker is the one already recorded as verified. These
tests run the real script with stand-ins for the pipeline interpreter and for `airflow`, so nothing touches GCS or
Airflow. They hold:
  - a recorded marker equal to the newest one skips the check (exit 99) and records nothing;
  - a newer marker, an empty record, or no marker at all runs the check;
  - force_verify runs the check even when the marker was already verified;
  - the marker is recorded only after the check passes, and a failed check or a failed lookup stops the step;
  - a failure to record the marker does not fail the step (the next run just checks again).
"""
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "verify_unless_unchanged.sh"
OLD, NEW = "20261107T170724234344Z", "20261112T180000000000Z"

STUB_PYTHON = """#!/usr/bin/env bash
echo "python $*" >> "$STUB_LOG"
if [ "$2" = "bronze.check" ] && [ "${3:-}" = "--newest-marker" ]; then
    [ "${STUB_NEWEST_EXIT:-0}" = "0" ] || exit "$STUB_NEWEST_EXIT"
    [ -z "${STUB_NEWEST:-}" ] || echo "$STUB_NEWEST"
    exit 0
fi
exit "${STUB_CHECK_EXIT:-0}"
"""
STUB_AIRFLOW = """#!/usr/bin/env bash
echo "airflow $*" >> "$STUB_LOG"
exit "${STUB_AIRFLOW_EXIT:-0}"
"""


@pytest.fixture
def step(tmp_path):
    for name, body in (("python", STUB_PYTHON), ("airflow", STUB_AIRFLOW)):
        stub = tmp_path / name
        stub.write_text(body)
        stub.chmod(0o755)
    log = tmp_path / "calls.log"

    def run(newest=NEW, last="", force="0", **extra):
        env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "BRONZE_PYTHON": str(tmp_path / "python"),
               "STUB_LOG": str(log), "STUB_NEWEST": newest, "LAST_VERIFIED": last, "FORCE_VERIFY": force,
               **{key.upper(): str(value) for key, value in extra.items()}}
        done = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=30)
        calls = log.read_text().splitlines() if log.exists() else []
        return done, calls

    return run


CHECK = "python -m bronze.check"
LOOKUP = "python -m bronze.check --newest-marker"


def test_a_marker_that_already_passed_skips_the_check(step):
    done, calls = step(newest=NEW, last=NEW)
    assert done.returncode == 99
    assert calls == [LOOKUP]                        # only the lookup: no check, nothing recorded
    assert NEW in done.stdout and "force_verify" in done.stdout


def test_a_newer_marker_runs_the_check_and_records_it(step):
    done, calls = step(newest=NEW, last=OLD)
    assert done.returncode == 0
    assert calls == [LOOKUP, CHECK, f"airflow variables set bronze_last_verified_marker {NEW}"]


def test_with_nothing_recorded_the_check_runs(step):
    done, calls = step(newest=NEW, last="")
    assert done.returncode == 0 and CHECK in calls
    assert calls[-1] == f"airflow variables set bronze_last_verified_marker {NEW}"


def test_force_runs_the_check_even_when_the_marker_already_passed(step):
    done, calls = step(newest=NEW, last=NEW, force="1")
    assert done.returncode == 0
    assert calls == [LOOKUP, CHECK, f"airflow variables set bronze_last_verified_marker {NEW}"]
    assert "force_verify is set" in done.stdout


def test_with_no_marker_at_all_the_check_runs_and_nothing_is_recorded(step):
    done, calls = step(newest="", last="")
    assert done.returncode == 0
    assert calls == [LOOKUP, CHECK]                 # an empty id never counts as "already verified"


def test_a_failed_check_fails_the_step_and_records_nothing(step):
    done, calls = step(newest=NEW, last=OLD, stub_check_exit=2)
    assert done.returncode == 2                     # the check's own exit code comes through
    assert calls == [LOOKUP, CHECK]


def test_a_failed_marker_lookup_fails_the_step_before_any_check(step):
    done, calls = step(newest=NEW, last=OLD, stub_newest_exit=3)
    assert done.returncode == 1
    assert calls == [LOOKUP]


def test_failing_to_record_the_marker_does_not_fail_the_step(step):
    done, calls = step(newest=NEW, last=OLD, stub_airflow_exit=1)
    assert done.returncode == 0
    assert "could not record" in done.stderr and NEW in done.stderr
    assert CHECK in calls


def test_the_script_is_executable_and_valid_bash():
    assert os.access(SCRIPT, os.X_OK)
    assert subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True).returncode == 0
