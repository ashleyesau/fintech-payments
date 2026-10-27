"""tests/test_cdc_main.py: M4 step 6, the command line, its exit codes and Ctrl-C.

`execute()` takes factories for the reader and the sink, so the exit codes are driven with a
scripted reader and the in-memory sink; Ctrl-C uses real signals sent to the test process; a few
tests run the real reader against the scratch slot; two run `python -m cdc.main` as a subprocess.
Nothing here touches GCS (the real bucket is the step 7 smoke test).
"""
import io
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import psycopg
import pytest
from google.auth import exceptions as auth_errors

from cdc.config import CdcSettings
from cdc.errors import (
    CdcError, DecodeError, ObjectMismatch, SchemaDriftError, SinkError, SlotLost, SlotMissing,
    UnknownTypeError, UnsupportedChange,
)
from cdc.gcs import GcsSink
from cdc.lsn import format_lsn
from cdc.main import execute, exit_code, main, reason
from cdc.reader import Reader
from tests.fakes import FakeReader, MemorySink
from tests.helpers import pgoutput_scenario
from tests.test_cdc_consumer import stream, transactions
from tests.test_cdc_consumer_live import PUBLICATION, write_chain5
from tests.test_cdc_reader import slot_position

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def default_cdc_settings(monkeypatch):
    """The tests assume the default CDC_ settings, whatever is exported in the shell running them."""
    for name in [n for n in os.environ if n.startswith("CDC_")]:
        monkeypatch.delenv(name)


def settings(**kwargs):
    kwargs.setdefault("poll_interval_seconds", 30.0)
    return CdcSettings(_env_file=None, **kwargs)


class ClosableReader(FakeReader):
    closed = False

    def close(self):
        self.closed = True


def run(cfg=None, *, follow=False, reader=None, sink=None, signals=False, sink_factory=None):
    reader = reader if reader is not None else ClosableReader(stream())
    sink = sink if sink is not None else MemorySink()
    out, err = io.StringIO(), io.StringIO()
    code = execute(cfg or settings(), follow, reader_factory=lambda s: reader,
                   sink_factory=sink_factory or (lambda s: sink), out=out, err=err, handle_signals=signals)
    return code, out.getvalue(), err.getvalue(), reader, sink


# ---------------------------------------------------------------------------
# the mapping
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("error, code", [
    (SlotLost("lost"), 4),
    (SinkError("upload gave up"), 3),
    (ObjectMismatch("different events"), 2),
    (DecodeError("bad message"), 2),
    (UnknownTypeError("unknown type"), 2),
    (UnsupportedChange("a delete"), 2),
    (SchemaDriftError("columns changed"), 2),
    (CdcError("any other stop"), 2),
    (SlotMissing("no such slot"), 1),
    (psycopg.OperationalError("connection lost"), 1),
    (RuntimeError("the slot is not advancing"), 1),
    (ValueError("a bug"), 1),
    (KeyboardInterrupt(), 130),
])
def test_every_error_maps_to_the_agreed_exit_code(error, code):
    assert exit_code(error) == code


def test_a_reason_is_one_plain_line():
    assert "\n" not in reason(SlotLost("line one\n   line two"))
    assert reason(SinkError("could not  write")) == "could not write"
    assert reason(RuntimeError("boom")) == "unexpected RuntimeError: boom"
    assert "The slot keeps its position" in reason(psycopg.OperationalError("refused"))


# ---------------------------------------------------------------------------
# --once, end to end with the scripted reader
# ---------------------------------------------------------------------------

def test_once_drains_the_slot_prints_a_start_line_and_a_summary_and_exits_zero():
    code, out, err, reader, sink = run()
    assert code == 0 and err == ""
    first, *middle, last = out.strip().splitlines()
    assert first == ("slot cdc_r01 in fintech_cdc to gs://cdc_landing/cdc/cdc_r01/ (draining once; batches of "
                     "10,000 messages, 8 uploads at a time)")
    assert any(line.startswith("landed 11 changes from 5 transactions as 5 objects (5 new, 0 already there)") for line in middle)
    end = transactions()[-1].end_lsn
    assert last == (f"done: 11 changes from 5 transactions in 1 batches (5 objects new, 0 already there); "
                    f"slot confirmed to {format_lsn(end)}")
    assert reader.confirmed == [end] and len(sink.objects) == 5


def test_an_empty_slot_is_a_clean_exit_with_nothing_to_report_but_that():
    code, out, err, reader, sink = run(reader=ClosableReader([]))
    assert code == 0 and err == "" and sink.attempts == 0
    assert out.strip().splitlines()[-1] == "done: nothing was waiting in the slot"


def test_the_settings_decide_the_database_the_prefix_the_batch_and_the_workers():
    cfg = settings(db="fintech_cdc_smoke", slot_name="cdc_r02", batch_size=1, upload_workers=2, gcs_bucket="elsewhere")
    code, out, err, reader, sink = run(cfg)
    assert code == 0
    assert out.startswith("slot cdc_r02 in fintech_cdc_smoke to gs://elsewhere/cdc/cdc_r02/ (draining once; batches of 1 messages, 2 uploads")
    assert all(key.startswith("cdc/cdc_r02/") for key in sink.objects)
    assert len(reader.confirmed) == 5                       # a batch of 1 is one transaction each
    from cdc.dedup import events_in
    assert {e["source"]["db"] for data in sink.objects.values() for e in events_in(data)} == {"fintech_cdc_smoke"}


def test_the_follow_start_line_names_the_poll_interval_and_a_stop_ends_it_with_a_summary(monkeypatch):
    import cdc.main as main_module
    original = main_module.Consumer.run

    def run_then_stop(self, follow):
        threading.Timer(0.2, self.request_stop).start()        # what the first Ctrl-C does
        return original(self, follow)

    monkeypatch.setattr(main_module.Consumer, "run", run_then_stop)
    out, err = io.StringIO(), io.StringIO()
    code = execute(settings(poll_interval_seconds=0.05), True, reader_factory=lambda s: ClosableReader([]),
                   sink_factory=lambda s: MemorySink(), out=out, err=err, handle_signals=False)
    assert code == 0 and "following" in out.getvalue() and "looking again every 0.05 s" in out.getvalue()
    assert out.getvalue().strip().splitlines()[-1] == "stopped on request: nothing was waiting in the slot"


def test_the_reader_is_closed_on_every_way_out():
    ok = run()[3]
    assert ok.closed
    failing = ClosableReader(stream(), statuses=[None])
    failing.status = lambda: (_ for _ in ()).throw(SlotMissing("gone"))
    assert run(reader=failing)[0] == 1 and failing.closed


# ---------------------------------------------------------------------------
# every non-zero exit: one line on stderr, nothing confirmed, no summary
# ---------------------------------------------------------------------------

def check_stopped(code, expected, out, err, reader, line_has):
    assert code == expected
    lines = err.strip().splitlines()
    assert lines[0].startswith("error: ") and line_has in lines[0]
    assert "done:" not in out and "stopped on request" not in out
    assert reader.confirmed == [] or code in (130,)


def test_a_lost_slot_exits_4_before_reading_anything():
    from cdc.reader import SlotStatus
    reader = ClosableReader(stream(), statuses=[SlotStatus("lost", None, 1, 0, None)])
    code, out, err, reader, sink = run(reader=reader)
    check_stopped(code, 4, out, err, reader, "is lost")
    assert len(err.strip().splitlines()) == 1 and reader.fetches == 0 and sink.attempts == 0


def test_a_failed_upload_exits_3_and_the_slot_is_not_confirmed():
    sink = MemorySink()
    sink.fail_put = lambda attempt, key: attempt == 2
    code, out, err, reader, _ = run(sink=sink)
    check_stopped(code, 3, out, err, reader, "injected failure")
    assert len(err.strip().splitlines()) == 1


def test_missing_google_credentials_exit_3_and_say_how_to_sign_in():
    def no_credentials(cfg):
        def raise_it():
            raise auth_errors.DefaultCredentialsError("Your default credentials were not found")
        return GcsSink.connect(cfg.gcs_bucket, client_factory=raise_it)

    code, out, err, reader, _ = run(sink_factory=no_credentials)
    check_stopped(code, 3, out, err, reader, "gcloud auth application-default login")
    assert reader.closed and reader.fetches == 0


def test_an_object_with_different_events_exits_2():
    from cdc.landing import build_batch
    from tests.test_cdc_envelope import NOW, assemble
    sink = MemorySink()
    sink.objects[build_batch(assemble("inserts_utc")[:1], "fintech_cdc", "cdc/cdc_r01/", NOW)[0].key] = b"someone else's"
    code, out, err, reader, _ = run(sink=sink)
    check_stopped(code, 2, out, err, reader, "already exists and holds different events")


@pytest.mark.parametrize("scenario, expected", [
    ("delete_and_truncate", "delete"),
    ("unknown_type", "type"),
    ("unchanged_toast", "TOAST"),
])
def test_a_change_the_contract_does_not_allow_exits_2_with_nothing_landed_or_confirmed(scenario, expected):
    reader = ClosableReader(pgoutput_scenario(scenario))
    code, out, err, reader, sink = run(reader=reader)
    assert code == 2 and err.startswith("error: ") and len(err.strip().splitlines()) == 1
    assert expected.lower() in err.lower()
    assert reader.confirmed == [] and sink.attempts == 0


def test_a_missing_slot_exits_1_with_one_plain_line_and_no_traceback():
    reader = ClosableReader(stream())
    reader.status = lambda: (_ for _ in ()).throw(SlotMissing("Slot cdc_r01 does not exist in database fintech_cdc."))
    code, out, err, reader, _ = run(reader=reader)
    check_stopped(code, 1, out, err, reader, "does not exist in database fintech_cdc")
    assert len(err.strip().splitlines()) == 1 and "Traceback" not in err


def test_a_bug_exits_1_names_the_error_and_shows_where_it_happened():
    class Stuck(ClosableReader):
        def confirm(self, end_lsn):
            self.confirmed.append(end_lsn)                  # accepted, but the slot does not move

    code, out, err, reader, _ = run(reader=Stuck(stream()), cfg=settings(batch_size=1))
    assert code == 1 and err.splitlines()[0].startswith("error: unexpected RuntimeError: the slot is not advancing")
    assert "Traceback" in err


# ---------------------------------------------------------------------------
# Ctrl-C, with real signals
# ---------------------------------------------------------------------------

def interrupt():
    os.kill(os.getpid(), signal.SIGINT)


class watchdog:
    """If nothing has stopped a follow run after `seconds`, send the second Ctrl-C that ends it with
    130. A broken signal handler then fails its test on the exit code instead of hanging the suite."""
    def __init__(self, seconds=4.0):
        self.timer = threading.Timer(seconds, interrupt)

    def __enter__(self):
        self.timer.start()

    def __exit__(self, *exc):
        self.timer.cancel()


def test_the_first_ctrl_c_while_waiting_stops_cleanly_and_exits_zero():
    before = signal.getsignal(signal.SIGINT)
    threading.Timer(0.3, interrupt).start()
    started = time.perf_counter()
    with watchdog():
        code, out, err, reader, _ = run(reader=ClosableReader([]), follow=True, signals=True)
    assert code == 0 and time.perf_counter() - started < 5
    assert out.strip().splitlines()[-1] == "stopped on request: nothing was waiting in the slot"
    assert "stopping after the batch in hand" in err and "Ctrl-C again" in err
    assert signal.getsignal(signal.SIGINT) is before          # the handler was put back


def test_the_first_ctrl_c_during_a_flush_lets_that_batch_land_and_confirm_then_exits_zero():
    class InterruptedDuringUpload(MemorySink):
        sent = False

        def put_if_absent(self, key, data):
            if not self.sent:
                self.sent = True
                interrupt()                                   # the first Ctrl-C, mid-flush
                time.sleep(0.2)                               # let the handler run before this upload returns
            return super().put_if_absent(key, data)

    sink = InterruptedDuringUpload()
    with watchdog():
        code, out, err, reader, _ = run(sink=sink, cfg=settings(batch_size=1, upload_workers=1), follow=True, signals=True)
    first = transactions()[0]
    assert code == 0 and reader.confirmed == [first.end_lsn]  # that batch was confirmed, and no other
    assert out.strip().splitlines()[-1].startswith("stopped on request: 3 changes from 1 transactions in 1 batches")
    assert "stopping after the batch in hand" in err


def test_a_second_ctrl_c_stops_at_once_exits_130_and_confirms_nothing():
    err_box = {}

    class InterruptedTwice(MemorySink):
        sent = False

        def put_if_absent(self, key, data):
            if not self.sent:                                 # exactly two signals in all, not two per upload
                self.sent = True
                interrupt()
                deadline = time.time() + 5
                while "stopping after" not in err_box["err"].getvalue() and time.time() < deadline:
                    time.sleep(0.01)                          # wait for the first handler to have run
                interrupt()
                time.sleep(0.3)
            return super().put_if_absent(key, data)

    reader, sink = ClosableReader(stream()), InterruptedTwice()
    out, err = io.StringIO(), io.StringIO()
    err_box["err"] = err
    before = signal.getsignal(signal.SIGINT)
    code = execute(settings(upload_workers=1), False, reader_factory=lambda s: reader, sink_factory=lambda s: sink,
                   out=out, err=err, handle_signals=True)
    assert code == 130 and reader.confirmed == [] and reader.closed
    assert "interrupted again" in err.getvalue() and "without confirming" in err.getvalue()
    assert signal.getsignal(signal.SIGINT) is before


# ---------------------------------------------------------------------------
# the real reader against the scratch slot
# ---------------------------------------------------------------------------

def live_settings(dsn, slot, **kwargs):
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    return settings(host=info["host"], port=int(info["port"]), db=info["dbname"], user=info["user"],
                    password=info["password"], slot_name=slot, **kwargs)


def test_a_real_slot_is_drained_once_and_a_second_run_finds_nothing(cdc_dsn, cdc_slot):
    write_chain5(cdc_dsn, [1, 1, 2])
    sink = MemorySink()
    cfg = live_settings(cdc_dsn, cdc_slot)
    code, out, err, _, _ = run(cfg, reader=Reader(cdc_dsn, cdc_slot, PUBLICATION), sink=sink)
    assert code == 0 and err == "" and "done: 9 changes from 3 transactions in 1 batches (6 objects new" in out
    with Reader(cdc_dsn, cdc_slot, PUBLICATION) as peek:
        assert peek.fetch(1000) == []
    again, out2, err2, _, _ = run(cfg, reader=Reader(cdc_dsn, cdc_slot, PUBLICATION), sink=sink)
    assert again == 0 and out2.strip().splitlines()[-1] == "done: nothing was waiting in the slot"


def test_a_slot_that_does_not_exist_exits_1_naming_it(cdc_dsn):
    cfg = live_settings(cdc_dsn, "cdc_no_such_slot")
    code, out, err, _, _ = run(cfg, reader=Reader(cdc_dsn, "cdc_no_such_slot", PUBLICATION))
    assert code == 1 and "cdc_no_such_slot does not exist in database" in err and len(err.strip().splitlines()) == 1


def test_an_unreachable_database_exits_1_and_never_prints_the_password():
    cfg = settings(host="localhost", port=1, password="s3cret-pa55word")
    out, err = io.StringIO(), io.StringIO()
    code = execute(cfg, False, sink_factory=lambda s: MemorySink(), out=out, err=err, handle_signals=False)
    assert code == 1 and "database connection" in err.getvalue()
    assert "s3cret-pa55word" not in out.getvalue() + err.getvalue()


# ---------------------------------------------------------------------------
# the entry point
# ---------------------------------------------------------------------------

def cli(*args, **env):
    full = {"PYTHONPATH": str(ROOT), "PATH": "/usr/bin:/usr/local/bin", **env}
    return subprocess.run([sys.executable, "-W", "ignore", "-m", "cdc.main", *args], cwd=ROOT, capture_output=True,
                          text=True, env=full, timeout=60)


def test_the_module_runs_and_documents_itself():
    result = cli("--help")
    assert result.returncode == 0 and "--once" in result.stdout and "--follow" in result.stdout


@pytest.mark.parametrize("args", [(), ("--once", "--follow")])
def test_a_usage_error_is_exit_1_not_2_because_2_means_a_contract_stop(args):
    result = cli(*args)
    assert result.returncode == 1
    assert result.stderr.strip() == "error: choose exactly one of --once or --follow"


def test_the_command_line_reports_an_unreachable_database_on_one_line_without_the_password():
    result = cli("--once", CDC_PORT="1", CDC_PASSWORD="s3cret-pa55word")
    assert result.returncode == 1
    assert len(result.stderr.strip().splitlines()) == 1 and result.stderr.startswith("error: lost or could not make")
    assert "s3cret-pa55word" not in result.stdout + result.stderr


def test_main_returns_the_code_instead_of_exiting_when_called_from_python():
    assert main(["--help"]) == 0
    assert main([]) == 1


def test_the_stale_streaming_stub_is_gone():
    assert not (ROOT / "simulator" / "consumer.py").exists()
