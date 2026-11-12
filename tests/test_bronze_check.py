"""tests/test_bronze_check.py: G1 step 4, python -m bronze.check, its output and its exit codes.

execute() takes a reader factory, so the exit codes are driven with synthetic landings over the in-memory
sink. Nothing here touches GCS; the real bucket is step 6. One test runs the real command as a subprocess
to show it refuses to start without credentials.
"""
import io
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import bronze.check as check
from bronze.check import (
    EXIT_BUCKET, EXIT_CONTRACT, EXIT_ERROR, EXIT_INTERRUPTED, EXIT_OK, exit_code, execute, machine_memory_bytes, main,
    peak_memory_bytes, print_newest_marker, reason, render,
)
from bronze.config import CREDENTIALS_VARIABLE, BronzeSettings
from bronze.errors import ConfigError, DisagreementError, EventError, IncompleteTransactions, LandingError
from bronze.index import IndexSummary, build_index
from bronze.marker import TABLES, TableMark, build_marker, encode
from cdc.errors import ObjectMismatch, SinkError
from tests.bronze_synth import DAY, SOURCE_DB, Landing, event
from tests.fakes import MemorySink

ROOT = Path(__file__).resolve().parent.parent
M, T, A, R, K = "merchants", "merchant_transactions", "loan_applications", "loan_repayments", "risk_events"


@pytest.fixture
def creds(tmp_path):
    file = tmp_path / "bronze_credentials.json"
    file.write_text('{"refresh_token": "SECRET"}')
    return {CREDENTIALS_VARIABLE: str(file)}, file


@pytest.fixture(autouse=True)
def default_settings(monkeypatch):
    for key in ("CDC_GCS_BUCKET", "CDC_SLOT_NAME", "BRONZE_READ_WORKERS", "BRONZE_BUCKET"):
        monkeypatch.delenv(key, raising=False)


def run(land=None, environ=None, factory=None, clock=None):
    out, err = io.StringIO(), io.StringIO()
    settings = BronzeSettings(_env_file=None)
    factory = factory or (lambda s: land.reader())
    kwargs = {"clock": clock} if clock else {}
    code = execute(settings, reader_factory=factory, environ=environ, out=out, err=err, **kwargs)
    return code, out.getvalue(), err.getvalue()


def small_run():
    land = Landing()
    land.transaction((M, "c"), (M, "c"), (K, "c"))
    land.transaction((T, "c"), (T, "c"))
    land.transaction((A, "c"))
    land.transaction((A, "u"))
    land.flush()
    return land


# ---------------------------------------------------------------------------
# a good run
# ---------------------------------------------------------------------------

def test_a_good_run_prints_the_counts_and_exits_zero(creds):
    environ, file = creds
    code, out, err = run(small_run(), environ)
    assert code == EXIT_OK and err.strip().endswith("read 4 of 4 objects")
    lines = out.splitlines()
    assert lines[0] == f"reading gs://cdc_landing/cdc/cdc_r01/ with 8 reads at a time (credentials file {file})"
    assert lines[1] == "mapping every event to its Bronze row, source database fintech_cdc"
    assert "objects        4" in lines
    assert "events         7  (7 unique, 0 duplicates)" in lines
    assert "transactions   4  (0 incomplete)" in lines
    table = {line.split()[0]: line.split()[1:] for line in lines if line.split() and line.split()[0] in
             (M, T, A, R, K, "total")}
    assert table == {M: ["2", "0"], T: ["2", "0"], A: ["1", "1"], R: ["0", "0"], K: ["1", "0"], "total": ["6", "1"]}
    assert lines[-1] == "OK: every object is valid, every event maps to a Bronze row and every transaction is complete"
    assert "SECRET" not in out + err


def test_the_command_reads_and_never_writes(creds):
    class Trap(MemorySink):
        def put_if_absent(self, key, data):
            raise AssertionError("bronze.check wrote")

    land = small_run()
    trap = Trap()
    trap.objects = land.sink.objects
    from bronze.landing import LandingReader
    code, _, _ = run(environ=creds[0], factory=lambda s: LandingReader(trap, land.prefix))
    assert code == EXIT_OK and trap.attempts == 0 and trap.listings == 1 and trap.reads == 4


def test_the_workers_come_from_the_settings(creds, monkeypatch):
    monkeypatch.setenv("BRONZE_READ_WORKERS", "3")
    _, out, _ = run(small_run(), creds[0])
    assert "with 3 reads at a time" in out.splitlines()[0]


def test_the_workers_setting_is_what_the_index_is_built_with(creds, monkeypatch):
    monkeypatch.setenv("BRONZE_READ_WORKERS", "3")
    seen = []
    real = check.build_index
    monkeypatch.setattr(check, "build_index", lambda reader, workers, progress, source_db: seen.append(workers) or real(reader, workers, progress, source_db=source_db))
    run(small_run(), creds[0])
    assert seen == [3]


def test_the_landing_bucket_and_prefix_are_named_from_the_cdc_settings(creds, monkeypatch):
    monkeypatch.setenv("CDC_GCS_BUCKET", "elsewhere")
    land = Landing(prefix="cdc/cdc_r02/")
    code, out, _ = run(land, creds[0])
    assert code == EXIT_OK and out.splitlines()[0].startswith("reading gs://elsewhere/cdc/cdc_r02/ with")


def test_progress_goes_to_stderr_every_hundred_objects_and_at_the_end(creds):
    land = Landing()
    for _ in range(250):
        land.transaction((M, "c"))
        land.flush()
    code, out, err = run(land, creds[0])
    assert code == EXIT_OK
    assert err.splitlines() == ["  read 100 of 250 objects", "  read 200 of 250 objects", "  read 250 of 250 objects"]
    assert "read " not in out


def test_the_time_and_memory_line_uses_the_clock_and_the_operating_system(creds):
    ticks = iter([100.0, 321.4])
    code, out, _ = run(small_run(), creds[0], clock=lambda: next(ticks))
    line = next(l for l in out.splitlines() if l.startswith("time "))
    assert line.startswith("time 3 min 41 s, peak memory ") and " MB of " in line and line.endswith(" on this machine")


def test_an_empty_landing_is_a_clean_run_that_says_zero(creds):
    code, out, _ = run(Landing(), creds[0])
    assert code == EXIT_OK and "objects        0" in out.splitlines() and "events         0  (0 unique, 0 duplicates)" in out.splitlines()


# ---------------------------------------------------------------------------
# every way to stop
# ---------------------------------------------------------------------------

def test_missing_credentials_stop_the_command_before_any_connection():
    made = []
    code, out, err = run(small_run(), environ={}, factory=lambda s: made.append(1))
    assert code == EXIT_ERROR and made == [] and out == ""
    assert err.startswith("error: GOOGLE_APPLICATION_CREDENTIALS is not set") and err.count("\n") == 1


def test_a_credentials_file_that_does_not_exist_stops_the_command(tmp_path):
    code, _, err = run(small_run(), environ={CREDENTIALS_VARIABLE: str(tmp_path / "nope.json")})
    assert code == EXIT_ERROR and "does not exist" in err


def test_a_bad_event_is_a_contract_stop_and_names_the_object(creds):
    land = small_run()
    bad = event(M, "c", 0x3000000, 0, 1)
    bad["envelope_version"] = 2
    key = land.put(M, DAY, [bad])
    code, out, err = run(land, creds[0])
    assert code == EXIT_CONTRACT and key in err and "envelope_version" in err and err.count("error:") == 1
    assert "total" not in out


def test_two_copies_that_disagree_are_a_contract_stop(creds):
    land = Landing()
    tx = land.transaction((M, "c"))
    land.flush()
    other = tx.events(clock=9)[0]
    other["after"]["merchant_id"] = "other"
    land.put(M, DAY, [other], lsns=(tx.commit_lsn - 1, tx.commit_lsn))
    code, _, err = run(land, creds[0])
    assert code == EXIT_CONTRACT and "landed twice with different content" in err


def test_an_incomplete_transaction_is_a_contract_stop_after_the_summary_is_printed(creds):
    land = Landing()
    land.transaction((M, "c"), (T, "c"))
    land.flush()
    del land.sink.objects[next(k for k in land.sink.objects if f"/{T}/" in k)]
    code, out, err = run(land, creds[0])
    assert code == EXIT_CONTRACT
    assert "transactions   1  (1 incomplete)" in out.splitlines()
    assert "1 of 1 transactions are incomplete" in err and "nothing was committed" in err
    assert "OK:" not in out


def test_a_stray_object_under_the_prefix_is_a_contract_stop(creds):
    land = small_run()
    land.sink.put_if_absent("cdc/cdc_r01/notes.txt", b"x")
    code, _, err = run(land, creds[0])
    assert code == EXIT_CONTRACT and "notes.txt" in err


def test_a_failed_read_is_a_bucket_failure(creds):
    land = small_run()
    land.sink.fail_read = lambda key: True
    code, _, err = run(land, creds[0])
    assert code == EXIT_BUCKET and "injected failure reading" in err


def test_ctrl_c_exits_130_and_says_nothing_was_written(creds):
    def interrupted(settings):
        raise KeyboardInterrupt
    code, _, err = run(environ=creds[0], factory=interrupted)
    assert code == EXIT_INTERRUPTED and err == "error: interrupted; nothing was written\n"


def test_a_bug_is_exit_1_with_a_traceback(creds):
    def broken(settings):
        raise RuntimeError("boom")
    code, _, err = run(environ=creds[0], factory=broken)
    assert code == EXIT_ERROR and "error: unexpected RuntimeError: boom" in err and "Traceback" in err


@pytest.mark.parametrize("error,code", [
    (KeyboardInterrupt(), 130), (SinkError("x"), 3), (ConfigError("x"), 1), (RuntimeError("x"), 1),
    (LandingError("x"), 2), (EventError("x"), 2), (DisagreementError("x"), 2), (IncompleteTransactions("x"), 2),
])
def test_every_error_maps_to_the_agreed_exit_code(error, code):
    assert exit_code(error) == code


def test_a_cdc_contract_error_that_is_not_a_sink_error_is_not_a_bucket_failure():
    assert exit_code(ObjectMismatch("x")) == EXIT_ERROR


def test_a_reason_is_one_plain_line():
    assert "\n" not in reason(EventError("a\n  b\n c"))
    assert reason(EventError("a\n  b")) == "a b"
    assert reason(SinkError("could not read")) == "could not read"
    assert reason(RuntimeError("boom")) == "unexpected RuntimeError: boom"


# ---------------------------------------------------------------------------
# pieces
# ---------------------------------------------------------------------------

def test_render_lines_up_a_summary():
    summary = IndexSummary(objects=908, events_read=1_017_902, unique_events=1_017_902, duplicates=0,
                           transactions=613, incomplete=0,
                           by_table={M: {"c": 20078, "u": 0}, T: {"c": 997650, "u": 0}, A: {"c": 37, "u": 18},
                                     R: {"c": 76, "u": 0}, K: {"c": 43, "u": 0}})
    assert render(summary).splitlines() == [
        "objects        908",
        "events         1,017,902  (1,017,902 unique, 0 duplicates)",
        "transactions   613  (0 incomplete)",
        "",
        "table                    inserts (c)   updates (u)",
        "merchants                     20,078             0",
        "merchant_transactions        997,650             0",
        "loan_applications                 37            18",
        "loan_repayments                   76             0",
        "risk_events                       43             0",
        "total                      1,017,884            18",
    ]


def test_render_keeps_what_was_read_apart_from_what_is_unique_and_a_copy_count():
    summary = IndexSummary(objects=3, events_read=10, unique_events=8, duplicates=2, transactions=4, incomplete=1,
                           by_table={M: {"c": 8, "u": 0}})
    lines = render(summary).splitlines()
    assert lines[1] == "events         10  (8 unique, 2 duplicates)"
    assert lines[2] == "transactions   4  (1 incomplete)"


def test_peak_memory_is_bytes_whatever_the_platforms_unit(monkeypatch):
    monkeypatch.setattr(check.resource, "getrusage", lambda who: SimpleNamespace(ru_maxrss=1000))
    monkeypatch.setattr(check.sys, "platform", "linux")
    assert peak_memory_bytes() == 1_024_000
    monkeypatch.setattr(check.sys, "platform", "darwin")
    assert peak_memory_bytes() == 1000


def test_peak_memory_of_this_process_is_plausible():
    assert 10_000_000 < peak_memory_bytes() < 100_000_000_000


def test_machine_memory_is_a_size_or_unknown(monkeypatch):
    assert machine_memory_bytes() > 100_000_000

    def broken(name):
        raise ValueError(name)
    monkeypatch.setattr(check.os, "sysconf", broken)
    assert machine_memory_bytes() is None


def test_progress_is_reported_for_every_object_in_order():
    land = small_run()
    seen = []
    build_index(land.reader(), 2, lambda done, total: seen.append((done, total)), source_db=SOURCE_DB)
    assert seen == [(n, 4) for n in range(1, 5)]


# ---------------------------------------------------------------------------
# the real command line
# ---------------------------------------------------------------------------

def test_the_command_refuses_to_start_without_credentials_as_a_subprocess():
    env = {k: v for k, v in os.environ.items() if k != CREDENTIALS_VARIABLE}
    result = subprocess.run([sys.executable, "-W", "ignore", "-m", "bronze.check"], cwd=ROOT, env={**env, "PYTHONPATH": "."},
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == EXIT_ERROR
    assert result.stderr.startswith("error: GOOGLE_APPLICATION_CREDENTIALS is not set") and result.stdout == ""


def test_a_usage_error_is_exit_1_because_2_means_a_contract_stop(capsys):
    assert main(["--nope"]) == EXIT_ERROR
    assert capsys.readouterr().err.startswith("error: ")


def test_help_exits_zero(capsys):
    assert main(["--help"]) == EXIT_OK
    assert "GOOGLE_APPLICATION_CREDENTIALS" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# --newest-marker: the DAG's "has the landing changed since the last verified run" lookup
# ---------------------------------------------------------------------------

def put_marker(store, stamp):
    when = datetime.strptime(stamp, "%Y%m%dT%H%M%S%fZ").replace(tzinfo=timezone.utc)
    tables = {name: TableMark(1 + i, 7, f"gs://fintech_bronze/bronze/{name}/metadata/00001-x.metadata.json")
              for i, name in enumerate(TABLES)}
    marker = build_marker(when, landed_objects=908, highest_commit_lsn=1, tables=tables)
    assert store.put_if_absent(marker.key, encode(marker)) is True
    return marker


def lookup(store, environ):
    out, err = io.StringIO(), io.StringIO()
    code = print_newest_marker(BronzeSettings(_env_file=None), store_factory=lambda s, e: store, environ=environ,
                               out=out, err=err)
    return code, out.getvalue(), err.getvalue()


def test_newest_marker_prints_the_newest_run_id_alone_with_one_listing_and_one_read(creds):
    environ, _ = creds
    store = MemorySink()
    put_marker(store, "20261107T170724234344Z")
    put_marker(store, "20261112T180000000000Z")
    code, out, err = lookup(store, environ)
    assert (code, out, err) == (EXIT_OK, "20261112T180000000000Z\n", "")
    assert store.listings == 1 and store.reads == 1


def test_newest_marker_prints_nothing_when_there_is_no_marker(creds):
    environ, _ = creds
    store = MemorySink()
    assert lookup(store, environ) == (EXIT_OK, "", "")
    assert store.reads == 0


def test_newest_marker_stops_on_an_object_under_runs_that_is_not_a_marker(creds):
    environ, _ = creds
    store = MemorySink()
    put_marker(store, "20261107T170724234344Z")
    store.objects["_runs/notes.txt"] = b"hello"
    code, out, err = lookup(store, environ)
    assert code != EXIT_OK and out == "" and err.startswith("error: ") and "_runs/notes.txt" in err


def test_newest_marker_stops_on_a_newest_marker_that_does_not_parse(creds):
    environ, _ = creds
    store = MemorySink()
    put_marker(store, "20261107T170724234344Z")
    store.objects["_runs/20270101T000000000000Z.json"] = b"{broken"
    code, out, err = lookup(store, environ)
    assert code != EXIT_OK and out == "" and "20270101T000000000000Z" in err


def test_newest_marker_reports_a_bucket_failure_as_exit_3(creds):
    environ, _ = creds

    class Down:
        def list_keys(self, prefix):
            raise SinkError("the bucket did not answer")

    code, out, err = lookup(Down(), environ)
    assert code == EXIT_BUCKET and out == "" and "the bucket did not answer" in err


def test_newest_marker_refuses_to_start_without_credentials():
    code, out, err = lookup(MemorySink(), {})
    assert code != EXIT_OK and out == "" and "GOOGLE_APPLICATION_CREDENTIALS" in err


def test_newest_marker_cannot_be_combined_with_roundtrip(capsys):
    assert main(["--newest-marker", "--roundtrip"]) == EXIT_ERROR
    assert "cannot be used together" in capsys.readouterr().err


def test_newest_marker_as_a_subprocess_refuses_without_credentials_and_prints_nothing():
    env = {k: v for k, v in os.environ.items() if k != CREDENTIALS_VARIABLE}
    result = subprocess.run([sys.executable, "-W", "ignore", "-m", "bronze.check", "--newest-marker"], cwd=ROOT,
                            env={**env, "PYTHONPATH": "."}, capture_output=True, text=True, timeout=60)
    assert result.returncode == EXIT_ERROR and result.stdout == ""
    assert result.stderr.startswith("error: GOOGLE_APPLICATION_CREDENTIALS is not set")
