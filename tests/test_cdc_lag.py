"""tests/test_cdc_lag.py: M6, the lag report (`python -m cdc.lag`).

A scripted reader gives it every state a slot can be in; a real slot checks the numbers against Postgres's own
arithmetic; the real lost and stalled slots are in test_cdc_lost_slot.py. It reads Postgres only: nothing here
(or in the command) touches a bucket.
"""
import io
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import psycopg
import pytest

from cdc.errors import SlotMissing
from cdc.lag import LagReport, execute, format_line, format_report, main, size, take_report
from cdc.lsn import format_lsn
from cdc.reader import Reader, SlotStatus
from tests.fakes import FakeReader
from tests.test_cdc_consumer_live import write_chain5
from tests.test_cdc_main import live_settings, settings
from tests.test_cdc_reader import slot_position

ROOT = Path(__file__).resolve().parent.parent
MB = 1024 * 1024
CAP = 3072 * MB
NOW = datetime(2026, 10, 27, 21, 30, 5)


@pytest.fixture(autouse=True)
def default_cdc_settings(monkeypatch):
    for name in [n for n in os.environ if n.startswith("CDC_")]:
        monkeypatch.delenv(name)


def reader_at(status, wal=None):
    reader = FakeReader([], statuses=[status])
    reader.wal = wal
    return reader


def report(status, wal=None, watch=None, sleep=None):
    reader = reader_at(status, wal)
    out, err = io.StringIO(), io.StringIO()
    code = execute(settings(), watch, reader_factory=lambda s: reader, out=out, err=err,
                   sleep=sleep or (lambda seconds: None), clock=lambda: NOW)
    return code, out.getvalue(), err.getvalue(), reader


def status(wal_status="reserved", safe=CAP, cap=CAP, confirmed=1000, restart=900):
    return SlotStatus(wal_status, safe, cap, confirmed, restart)


# ---------------------------------------------------------------------------
# sizes
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("n, text", [
    (0, "0 bytes"), (1023, "1023 bytes"), (1024, "1 kB"), (1536, "2 kB"), (MB - 1, "1,024 kB"), (MB, "1.0 MB"),
    (int(12.34 * MB), "12.3 MB"), (3072 * MB, "3,072.0 MB"), (-97 * MB, "-97.0 MB"), (None, "none"),
])
def test_sizes_are_printed_in_bytes_kilobytes_or_megabytes(n, text):
    assert size(n) == text


# ---------------------------------------------------------------------------
# one report, at every level
# ---------------------------------------------------------------------------

def test_a_healthy_slot_is_reported_in_full_and_exits_zero():
    code, out, err, reader = report(status(confirmed=100 * MB, restart=90 * MB), wal=130 * MB)
    assert code == 0 and err == "" and reader.closed
    lines = out.splitlines()
    assert lines[0] == "slot cdc_fake in fintech_cdc: reserved"
    assert lines[1].strip() == f"confirmed position  {format_lsn(100 * MB)}"
    assert lines[2].strip() == f"WAL position now    {format_lsn(130 * MB)}"
    assert "30.0 MB of WAL written since the confirmed position" in lines[3]
    assert lines[4].strip().startswith("held by the slot    40.0 MB") and format_lsn(90 * MB) in lines[4]
    assert "cap 3,072.0 MB, headroom 3,072.0 MB" in lines[5]
    assert lines[6].strip() == "level               healthy"


@pytest.mark.parametrize("safe, wal_status, said", [
    (CAP // 2 - MB, "reserved", "under half the cap is left"),
    (CAP // 10 - MB, "reserved", "under a tenth of the cap is left"),
    (-97 * MB, "unreserved", "past the cap but not lost yet"),
])
def test_each_warning_level_is_named_and_is_not_a_failure(safe, wal_status, said):
    code, out, err, _ = report(status(wal_status=wal_status, safe=safe))
    assert code == 0 and err == "" and said in out


def test_a_negative_headroom_is_printed_as_negative():
    _, out, _, _ = report(status(wal_status="unreserved", safe=-97 * MB))
    assert "headroom -97.0 MB" in out


def test_a_lost_slot_is_reported_and_exits_four_with_the_recovery():
    code, out, err, reader = report(status(wal_status="lost", safe=None, restart=None))
    assert code == 4 and reader.closed and err == ""
    assert "slot cdc_fake in fintech_cdc: lost" in out and "LOST" in out and "cannot be repaired" in out
    assert "same seed" in out and "new slot name" in out
    assert "held by the slot    none" in out and "headroom none (the slot is lost)" in out


def test_a_slot_with_no_cap_says_so():
    code, out, _, _ = report(status(safe=None, cap=None))
    assert code == 0 and "cap unlimited, headroom unlimited" in out and "no cap" in out


def test_the_report_never_goes_negative_when_the_slot_is_ahead_of_the_wal_reading():
    _, out, _, _ = report(status(confirmed=5000, restart=4000), wal=3000)
    assert "behind              0 bytes" in out and "held by the slot    0 bytes" in out


# ---------------------------------------------------------------------------
# watch
# ---------------------------------------------------------------------------

def test_watch_prints_a_header_then_one_line_per_sample_until_ctrl_c():
    calls = []

    def sleep(seconds):
        calls.append(seconds)
        if len(calls) == 3:
            raise KeyboardInterrupt

    code, out, err, reader = report(status(confirmed=100 * MB, restart=90 * MB), wal=130 * MB, watch=2.5, sleep=sleep)
    assert code == 0 and err == "" and reader.closed and calls == [2.5, 2.5, 2.5]
    lines = out.splitlines()
    assert lines[0] == "slot cdc_fake in fintech_cdc, every 2.5 s (Ctrl-C to stop)"
    assert len(lines) == 4
    assert lines[1] == "21:30:05  reserved    behind    30.0 MB  held    40.0 MB  headroom 3,072.0 MB  ok"


def test_watch_stops_at_once_with_four_when_the_slot_is_lost():
    sleeps = []
    code, out, err, _ = report(status(wal_status="lost", safe=None, restart=None), watch=1, sleep=sleeps.append)
    assert code == 4 and sleeps == [] and out.splitlines()[-1].endswith("none  lost")


def test_a_line_names_the_warning_level():
    line = format_line(take_report(reader_at(status(safe=CAP // 10 - MB)), "fintech_cdc"), NOW)
    assert line.endswith("tenth") and "reserved" in line


# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------

def test_a_missing_slot_exits_one_with_one_plain_line():
    reader = FakeReader([])
    reader.status = lambda: (_ for _ in ()).throw(SlotMissing("Slot cdc_fake does not exist in database fintech_cdc."))
    out, err = io.StringIO(), io.StringIO()
    code = execute(settings(), None, reader_factory=lambda s: reader, out=out, err=err)
    assert code == 1 and out.getvalue() == "" and reader.closed
    assert err.getvalue() == "error: Slot cdc_fake does not exist in database fintech_cdc.\n"


def test_an_unreachable_database_exits_one_and_never_prints_the_password():
    out, err = io.StringIO(), io.StringIO()
    code = execute(settings(host="localhost", port=1, password="s3cret-pa55word"), None, out=out, err=err)
    assert code == 1 and err.getvalue().startswith("error: lost or could not make the database connection")
    assert "s3cret-pa55word" not in out.getvalue() + err.getvalue() and len(err.getvalue().splitlines()) == 1


def test_a_bug_exits_one_names_the_error_and_shows_where():
    reader = FakeReader([])
    reader.status = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    out, err = io.StringIO(), io.StringIO()
    assert execute(settings(), None, reader_factory=lambda s: reader, out=out, err=err) == 1
    assert err.getvalue().startswith("error: unexpected RuntimeError: boom") and "Traceback" in err.getvalue()


# ---------------------------------------------------------------------------
# the numbers, against Postgres's own arithmetic, on a real slot
# ---------------------------------------------------------------------------

def sql_behind(dsn, slot):
    with psycopg.connect(dsn, autocommit=True) as conn:
        return int(conn.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) "
                                "FROM pg_replication_slots WHERE slot_name = %s", (slot,)).fetchone()[0])


def test_the_numbers_agree_with_postgres_and_move_as_the_slot_is_written_and_drained(cdc_dsn, cdc_slot):
    from cdc.consumer import Consumer
    from tests.fakes import MemorySink
    with Reader(cdc_dsn, cdc_slot, "fintech_cdc") as reader:
        fresh = take_report(reader, "fintech_cdc")
        write_chain5(cdc_dsn, [1, 2, 3])
        before = sql_behind(cdc_dsn, cdc_slot)
        written = take_report(reader, "fintech_cdc")
        after = sql_behind(cdc_dsn, cdc_slot)
        assert before <= written.behind_bytes <= after                  # Postgres's own difference, taken either side
        assert written.behind_bytes > fresh.behind_bytes + 1000          # the three transactions are in it
        assert written.status.confirmed_flush_lsn == fresh.status.confirmed_flush_lsn == slot_position(cdc_dsn, cdc_slot)
        assert written.held_bytes >= written.behind_bytes               # the slot holds from its restart position
        Consumer(reader, MemorySink(), database="fintech_cdc", prefix="cdc/x/").run(follow=False)
        drained = take_report(reader, "fintech_cdc")
    assert drained.status.confirmed_flush_lsn > written.status.confirmed_flush_lsn
    assert drained.behind_bytes < written.behind_bytes and drained.level == "ok"


def test_the_command_runs_end_to_end_against_a_real_slot(cdc_dsn, cdc_slot):
    out, err = io.StringIO(), io.StringIO()
    assert execute(live_settings(cdc_dsn, cdc_slot), None, out=out, err=err) == 0 and err.getvalue() == ""
    assert out.getvalue().startswith(f"slot {cdc_slot} in fintech_cdc_test_") and "level               healthy" in out.getvalue()


def test_a_slot_that_does_not_exist_exits_one_naming_it(cdc_dsn):
    out, err = io.StringIO(), io.StringIO()
    assert execute(live_settings(cdc_dsn, "cdc_no_such_slot"), None, out=out, err=err) == 1
    assert "cdc_no_such_slot does not exist in database" in err.getvalue()


# ---------------------------------------------------------------------------
# the entry point
# ---------------------------------------------------------------------------

def run_module(*args, **env):
    full = {k: v for k, v in os.environ.items() if not k.startswith("CDC_")}
    full.update({"PYTHONPATH": str(ROOT), **env})
    return subprocess.run([sys.executable, "-W", "ignore", "-m", "cdc.lag", *args], cwd=ROOT, capture_output=True,
                          text=True, env=full, timeout=60)


def test_the_module_runs_documents_itself_and_refuses_a_nonsense_interval():
    help_ = run_module("--help")
    assert help_.returncode == 0 and "--watch" in help_.stdout
    bad = run_module("--watch", "0")
    assert bad.returncode == 1 and bad.stderr.strip() == "error: --watch needs a number of seconds above zero"
    assert main(["--watch", "-3"]) == 1


def test_the_command_never_loads_googles_libraries():
    code = "import sys, cdc.lag; bad = sorted(m for m in sys.modules if m.split('.')[0] in ('google', 'requests')); print(bad); sys.exit(1 if bad else 0)"
    result = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True,
                            env={"PYTHONPATH": str(ROOT), "PATH": "/usr/bin:/usr/local/bin"})
    assert result.returncode == 0, result.stdout + result.stderr
