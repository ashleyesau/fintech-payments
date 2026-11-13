"""tests/test_bronze_main.py: G3 step 5, python -m bronze.main, its output, its modes and its exit codes.

execute() takes factories for the landing, the store and the catalog, so the command is driven over a synthetic landing
in memory, a local warehouse and an in-memory store. Nothing here touches GCS. Held here, each with a mutation that
fails it (see the commit message):
  - the default is a plan: it reads everything, prints what a write would do, and writes nothing (no table, no marker,
    no copy, no namespace); --write does the work; a second --write says there is nothing to do and changes nothing;
  - the report: the counts per table and per operation, landed against held, what a write would do, the marker lines,
    the marker written and the copy replaced, time and memory;
  - progress: one line per committed batch and pass 1 every 100 objects, on stderr;
  - the settings reach the run: batch rows, workers, source database, catalog file, the catalog's own warehouse;
  - every exit code (0, 1, 2, 3, 130) with exactly one "error:" line, and how many batches a failed write had committed;
  - --rebuild-catalog rebuilds a lost catalog from the marker and reads nothing from the landing;
  - the real command refuses to start without credentials, as a subprocess.
"""
import io
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

import bronze.main as main_module
from bronze.catalog_copy import catalog_bytes
from bronze.config import CREDENTIALS_VARIABLE, BronzeSettings
from bronze.errors import ConfigError
from bronze.main import _count, _megabytes, exit_code, execute, main, open_catalog, reason, render_tables
from bronze.marker import newest_marker
from bronze.run import NAMESPACE, TABLE_ORDER, run
from bronze.store import CATALOG_KEY
from cdc.errors import SinkError
from tests.bronze_env import Concurrency, Env, Spy, busy_landing, spread_landing
from tests.bronze_synth import DAY, SOURCE_DB, Landing, event

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


@pytest.fixture
def creds(tmp_path):
    path = tmp_path / "bronze-writer.json"
    path.write_text("{}")
    return {CREDENTIALS_VARIABLE: str(path)}


def settings_for(env, monkeypatch, **overrides):
    for variable in ("BRONZE_BUCKET", "BRONZE_PROJECT", "BRONZE_CATALOG_PATH", "BRONZE_READ_WORKERS",
                     "BRONZE_BATCH_ROWS", "CDC_GCS_BUCKET", "CDC_SLOT_NAME", "CDC_DB"):
        monkeypatch.delenv(variable, raising=False)
    return BronzeSettings(_env_file=None, catalog_path=str(env.catalog_file), **overrides)


class Command:
    """The command over a local warehouse: one object that remembers what the factories were asked for."""

    def __init__(self, env, land, creds, settings, source=None) -> None:
        self.env, self.land, self.creds, self.settings = env, land, creds, settings
        self.source = source
        self.opened = []

    def __call__(self, **kwargs):
        out, err = io.StringIO(), io.StringIO()

        def landing(settings, environ):
            self.opened.append("landing")
            return (self.source or self.land).reader()

        def store(settings, environ):
            self.opened.append("store")
            return self.env.store

        def catalog(settings):
            self.opened.append("catalog")
            return self.env.catalog

        kwargs.setdefault("environ", self.creds)
        code = execute(self.settings, landing_factory=landing, store_factory=store, catalog_factory=catalog,
                       out=out, err=err, **kwargs)
        return code, out.getvalue(), err.getvalue()


@pytest.fixture
def command(env, creds, monkeypatch):
    land = busy_landing()
    return Command(env, land, creds, settings_for(env, monkeypatch))


def table_line(out: str, name: str) -> list[str]:
    lines = [l for l in out.splitlines() if l.startswith(name + " ")]
    assert len(lines) == 2          # the counts of the landing, then the plan or result table
    return lines[-1].split()


def errors(err: str) -> list[str]:
    return [l for l in err.splitlines() if l.startswith("error:")]


# ---------------------------------------------------------------------------
# a plan is the default
# ---------------------------------------------------------------------------

def test_the_default_is_a_plan_that_writes_nothing_and_says_so(command, env):
    code, out, err = command()
    assert code == 0 and errors(err) == []
    assert "planning (nothing will be written)" in out and "OK: plan only, nothing was written. Run again with --write" in out
    assert not (env.dir / "warehouse").exists() and env.catalog.list_namespaces() == []
    assert env.store.objects == {} and env.store.overwrites == 0 and env.store.attempts == 0
    assert not any(env.has(n) for n in TABLE_ORDER)


def test_a_plan_prints_what_the_landing_holds_and_what_a_write_would_do(command):
    code, out, err = command()
    assert "a write would create 5 tables, append 27 rows in 21 objects, write a run marker, copy the catalog to the bucket" in out
    assert "run marker: none yet" in out
    assert table_line(out, "merchants")[:5] == ["merchants", "6", "0", "6", "6"] and "new table" in out
    assert table_line(out, "merchant_transactions")[:5] == ["merchant_transactions", "12", "0", "12", "6"]
    assert table_line(out, "loan_applications")[:5] == ["loan_applications", "2", "0", "2", "2"]
    assert table_line(out, "loan_repayments")[:5] == ["loan_repayments", "1", "0", "1", "1"]
    assert table_line(out, "risk_events")[:5] == ["risk_events", "6", "0", "6", "6"]
    assert re.search(r"objects\s+21", out) and "27 unique" in out
    assert re.search(r"loan_applications\s+1\s+1", out)                # one insert and one update


def test_a_plan_lists_the_tables_in_the_order_they_would_be_written(command):
    _, out, _ = command()
    names = [l.split()[0] for l in out.splitlines() if l.split() and l.split()[0] in TABLE_ORDER and l.count(" ") > 5]
    assert names[-5:] == list(TABLE_ORDER)


def test_a_plan_after_a_write_says_nothing_is_missing_and_the_marker_is_current(command, env):
    command(write=True)
    code, out, err = command()
    assert code == 0 and "a write would do nothing: Bronze is up to date" in out
    assert re.search(r"run marker: newest \d{8}T\d{12}Z, the tables were at it", out)
    assert table_line(out, "merchants") == ["merchants", "6", "6", "0", "0", "-", "up", "to", "date"]


def test_a_plan_with_a_late_object_says_how_much_a_write_would_add(command, env):
    command(write=True)
    command.land.transaction(("merchants", "c"), ("merchant_transactions", "c"), date="2025-05-05")
    command.land.flush()
    code, out, _ = command()
    assert "a write would append 2 rows in 2 objects, write a run marker, copy the catalog to the bucket" in out
    assert table_line(out, "merchants")[:5] == ["merchants", "7", "6", "1", "1"]


def test_a_plan_reads_each_landed_object_once_and_a_second_look_leaves_the_store_alone(command, env):
    spy = Spy(command.land.sink)
    command.source = type("S", (), {"reader": lambda self: spy.reader()})()
    command()
    assert spy.listings == 1 and len(spy.reads) == len(command.land.sink.objects)


# ---------------------------------------------------------------------------
# --write
# ---------------------------------------------------------------------------

def test_write_creates_the_tables_appends_every_change_and_writes_the_marker_and_the_copy(command, env):
    code, out, err = command(write=True)
    assert code == 0 and errors(err) == []
    assert "writing:" in out and "OK: appended 27 rows in 5 batches" in out
    assert [len(env.rows(n)) for n in TABLE_ORDER] == [6, 2, 1, 6, 12]
    marker = newest_marker(env.store)
    assert f"run marker written: {marker.run_id}" in out
    assert f"catalog copy: replaced in gs://fintech_bronze/{CATALOG_KEY}" in out
    assert env.store.objects[CATALOG_KEY] == catalog_bytes(env.catalog_file)
    assert table_line(out, "merchants")[:6] == ["merchants", "6", "0", "6", "6", "1"] and "created" in out


def test_a_second_write_says_there_is_nothing_to_append_and_changes_nothing(command, env):
    command(write=True)
    files, snapshots, objects = env.files(), env.snapshots(), dict(env.store.objects)
    code, out, err = command(write=True)
    assert code == 0 and "OK: nothing to append, Bronze already holds every landed change once" in out
    assert "run marker: unchanged, the tables are at it" in out and "catalog copy: not needed" in out
    assert out.count("up to date") == 5 and "created" not in out
    assert env.files() == files and env.snapshots() == snapshots and env.store.objects == objects


def test_a_second_write_says_it_skipped_the_index_and_a_plan_still_checks_everything(command, env):
    command(write=True)
    marker = newest_marker(env.store)
    code, out, err = command(write=True)
    assert code == 0 and errors(err) == []
    assert f"landing unchanged since run marker {marker.run_id}: no object was read, the index was skipped" in out
    assert "objects read" not in out and "read " not in err                  # no pass 1, so no progress lines
    code, out, err = command(write=False)                                    # a plan is the full check
    assert code == 0 and "index was skipped" not in out and "landing unchanged" not in out


def test_a_write_over_an_empty_landing_creates_the_tables_and_says_so(env, creds, monkeypatch):
    command = Command(env, Landing(), creds, settings_for(env, monkeypatch))
    code, out, _ = command(write=True)
    assert code == 0 and "OK: the tables were created, nothing to append" in out
    assert all(env.has(n) for n in TABLE_ORDER)


def test_the_report_ends_with_the_time_and_the_peak_memory(command):
    ticks = iter([100.0, 165.0])
    code, out, _ = command(clock=lambda: next(ticks))
    assert re.search(r"time 1 min 5 s, peak memory \d[\d,]* MB of ", out)


def test_the_report_names_the_source_database_and_the_credentials_file_but_never_its_contents(command, creds):
    _, out, _ = command()
    assert "source database fintech_cdc" in out and creds[CREDENTIALS_VARIABLE] in out
    assert "{}" not in out.split("credentials file", 1)[1].split("\n")[0].replace(creds[CREDENTIALS_VARIABLE], "")


# ---------------------------------------------------------------------------
# progress
# ---------------------------------------------------------------------------

def test_one_progress_line_per_committed_batch_goes_to_stderr(env, creds, monkeypatch):
    command = Command(env, spread_landing(5), creds, settings_for(env, monkeypatch, batch_rows=3))
    code, out, err = command(write=True)
    batches = [l for l in err.splitlines() if "batch" in l and "committed" in l]
    assert code == 0 and len(batches) == 3
    assert batches[0] == "  merchant_transactions: batch 1 committed, 4 rows (4 of 10)"
    assert batches[-1] == "  merchant_transactions: batch 3 committed, 2 rows (10 of 10)"
    assert "batch 1 committed" not in out


def test_a_batch_of_one_row_says_row_not_rows(env, creds, monkeypatch):
    land = Landing()
    land.transaction(("merchants", "c"))
    land.flush()
    _, _, err = Command(env, land, creds, settings_for(env, monkeypatch))(write=True)
    assert "  merchants: batch 1 committed, 1 row (1 of 1)" in err.splitlines()


def test_pass_1_reports_every_hundred_objects_and_the_last_one(command):
    _, _, err = command()
    reads = [l for l in err.splitlines() if l.startswith("  read ")]
    assert reads == ["  read 21 of 21 objects"]


def test_a_plan_has_no_batch_lines(command):
    _, _, err = command()
    assert "committed" not in err


# ---------------------------------------------------------------------------
# the settings reach the run
# ---------------------------------------------------------------------------

def test_the_batch_rows_setting_decides_how_many_commits(env, creds, monkeypatch):
    one = Command(env, spread_landing(6), creds, settings_for(env, monkeypatch, batch_rows=1))
    _, out, err = one(write=True)
    assert len([l for l in err.splitlines() if "committed" in l]) == 6
    other = Env(env.dir / "other")
    many = Command(other, spread_landing(6), creds, settings_for(other, monkeypatch, batch_rows=1000))
    _, _, err = many(write=True)
    assert len([l for l in err.splitlines() if "committed" in l]) == 1


def test_the_read_workers_setting_limits_the_reads(env, creds, monkeypatch):
    land = busy_landing()
    spy = Concurrency(land.sink)
    command = Command(env, land, creds, settings_for(env, monkeypatch, read_workers=1),
                      source=type("S", (), {"reader": lambda self: spy.reader()})())
    command(write=True)
    assert max(spy.levels) == 1 and len(spy.levels) == 2 * len(land.sink.objects)


def test_the_source_database_setting_is_held_to(env, creds, monkeypatch):
    monkeypatch.setenv("CDC_DB", "fintech_other")
    command = Command(env, busy_landing(), creds, BronzeSettings(_env_file=None, catalog_path=str(env.catalog_file)))
    code, out, err = command()
    assert code == 2 and "source database fintech_other" in out and errors(err)


def test_the_catalog_file_and_the_catalogs_own_warehouse_are_what_the_run_uses(command, env):
    command(write=True)
    again = command(write=True)
    assert again[0] == 0                                               # the second run's location check passed
    assert env.store.objects[CATALOG_KEY] == catalog_bytes(env.catalog_file)


# ---------------------------------------------------------------------------
# exit codes
# ---------------------------------------------------------------------------

def test_without_credentials_nothing_is_opened_and_the_exit_is_1(env, monkeypatch):
    command = Command(env, busy_landing(), {}, settings_for(env, monkeypatch))
    code, out, err = command()
    assert code == 1 and command.opened == [] and out == ""
    assert len(errors(err)) == 1 and errors(err)[0].startswith(f"error: {CREDENTIALS_VARIABLE} is not set")


def test_both_flags_together_are_a_usage_error_before_anything_is_opened(command):
    code, out, err = command(write=True, rebuild=True)
    assert code == 1 and command.opened == [] and "cannot be used together" in errors(err)[0]


def bad_event(land):
    e = event("merchants", "c", 0x7FFFFF00, 0, 1, date="2025-05-05")
    e["after"]["onboarded_at"] = "not a time"
    land.put("merchants", "2025-05-05", [e])


def test_a_contract_stop_is_exit_2_with_one_error_line_and_nothing_written(command, env):
    bad_event(command.land)
    code, out, err = command(write=True)
    assert code == 2 and len(errors(err)) == 1 and "onboarded_at" in errors(err)[0]
    assert env.catalog.list_namespaces() == [] and env.store.objects == {}
    assert "OK" not in out


def test_a_table_that_drifted_is_exit_2(command, env):
    command(write=True)
    from pyiceberg.types import StringType
    with env.table("risk_events").update_schema() as change:
        change.add_column("surprise", StringType())
    code, _, err = command()
    assert code == 2 and "risk_events" in errors(err)[0] and "differs from the contract" in errors(err)[0]


def test_a_lost_catalog_is_exit_2_and_names_the_rebuild_flag(command, env):
    command(write=True)
    lost = env.lose_catalog()
    command.env = lost
    code, _, err = command()
    assert code == 2 and "--rebuild-catalog" in errors(err)[0]


def test_a_failed_read_is_exit_3_in_a_plan_and_in_a_write(command, env):
    spy = Spy(command.land.sink, lambda key, n: (_ for _ in ()).throw(SinkError("injected: the bucket gave up")))
    command.source = type("S", (), {"reader": lambda self: spy.reader()})()
    for write in (False, True):
        code, out, err = command(write=write)
        assert code == 3 and errors(err) == ["error: injected: the bucket gave up"]


def test_a_write_that_fails_after_some_commits_says_how_many_and_that_a_rerun_continues(env, creds, monkeypatch):
    land = spread_landing(6)
    objects = len(land.sink.objects)
    state = {"n": 0}

    def fail(key, n):
        state["n"] += 1
        if state["n"] == objects + 4:
            raise SinkError("injected: the bucket gave up")

    command = Command(env, land, creds, settings_for(env, monkeypatch, batch_rows=1, read_workers=1),
                      source=type("S", (), {"reader": lambda self: Spy(land.sink, fail).reader()})())
    code, out, err = command(write=True)
    assert code == 3 and len(errors(err)) == 1
    assert "This run had committed 3 batches before it stopped; running again continues from there." in errors(err)[0]
    command.source = None
    code, out, _ = command(write=True)
    assert code == 0 and len(env.rows("merchant_transactions")) == 12


def test_a_failed_file_operation_from_pyarrow_is_exit_3_and_says_what_kind(command, env):
    class Broken:
        properties = {"warehouse": "gs://x"}

        def load_table(self, name):
            raise PermissionError("When reading information for key 'x': Forbidden")

    command.env.catalog = Broken()
    code, _, err = command()
    assert code == 3 and "a bucket or file operation failed (PermissionError: When reading" in errors(err)[0]
    assert len(errors(err)) == 1 and "Traceback" not in err


def test_a_bug_is_exit_1_and_prints_where(command, env):
    class Buggy:
        properties = {"warehouse": "gs://x"}

        def load_table(self, name):
            raise RuntimeError("something is wrong in the code")

    command.env.catalog = Buggy()
    code, _, err = command()
    assert code == 1 and "error: unexpected RuntimeError: something is wrong in the code" in err
    assert "Traceback (most recent call last)" in err and len(errors(err)) == 1


def test_an_interrupt_is_exit_130_and_a_write_says_how_many_batches_it_had_committed(env, creds, monkeypatch):
    land = spread_landing(5)
    command = Command(env, land, creds, settings_for(env, monkeypatch, batch_rows=1))
    real = main_module.run

    def interrupting(*args, **kwargs):
        original = kwargs["on_batch"]

        def stop(batch):
            original(batch)
            if batch.batch == 2:
                raise KeyboardInterrupt
        return real(*args, **{**kwargs, "on_batch": stop})

    monkeypatch.setattr(main_module, "run", interrupting)
    code, _, err = command(write=True)
    assert code == 130 and errors(err) == [
        "error: interrupted. This run had committed 2 batches before it stopped; running again continues from there."]
    assert len(env.rows("merchant_transactions")) == 4


def test_a_contract_stop_after_commits_does_not_say_it_twice(env, creds, monkeypatch):
    land = spread_landing(4)
    from bronze.errors import LandingChanged
    import bronze.run as run_module
    real = run_module._events_to_take

    def drop_the_last(obj, data, index, present, table):
        kept = real(obj, data, index, present, table)
        return kept[:1] if obj.key == sorted(land.sink.objects)[-1] else kept

    monkeypatch.setattr(run_module, "_events_to_take", drop_the_last)
    command = Command(env, land, creds, settings_for(env, monkeypatch, batch_rows=1))
    code, _, err = command(write=True)
    assert code == 2 and len(errors(err)) == 1 and errors(err)[0].count("committed") == 1


def test_every_non_zero_exit_prints_one_error_line_and_a_zero_exit_prints_none(command, env):
    code, out, err = command()
    assert code == 0 and errors(err) == []


def test_exit_codes_by_kind():
    from bronze.errors import ContractError, MarkerError
    assert exit_code(KeyboardInterrupt()) == 130
    assert exit_code(SinkError("x")) == 3 and exit_code(OSError("x")) == 3 and exit_code(TimeoutError("x")) == 3
    assert exit_code(ContractError("x")) == 2 and exit_code(MarkerError("x")) == 2
    assert exit_code(ConfigError("x")) == 1 and exit_code(RuntimeError("x")) == 1 and exit_code(ValueError("x")) == 1


def test_reasons_are_one_plain_line():
    assert reason(ConfigError("a\n  b")) == "a b"
    assert reason(KeyboardInterrupt()) == "interrupted"
    assert reason(SinkError("could not write x")) == "could not write x"
    assert reason(OSError("x")) == "a bucket or file operation failed (OSError: x)"
    assert reason(RuntimeError("x")) == "unexpected RuntimeError: x"


# ---------------------------------------------------------------------------
# --rebuild-catalog
# ---------------------------------------------------------------------------

def test_a_lost_catalog_is_rebuilt_from_the_marker_and_the_landing_is_never_read(command, env):
    command(write=True)
    lost = env.lose_catalog()
    command.env = lost
    command.opened.clear()
    code, out, err = command(rebuild=True)
    assert code == 0 and errors(err) == [] and command.opened == ["store", "catalog"]
    marker = newest_marker(env.store)
    assert f"from run marker {marker.run_id}" in out and "OK: run again without --rebuild-catalog" in out
    assert re.search(r"merchants: snapshot \d+, 6 rows", out) and re.search(r"merchant_transactions: snapshot \d+, 12 rows", out)
    assert all(lost.has(n) for n in TABLE_ORDER)
    code, out, _ = command()
    assert code == 0 and "a write would do nothing: Bronze is up to date" in out


def test_a_rebuild_writes_nothing_to_the_bucket(command, env):
    command(write=True)
    objects, overwrites = dict(env.store.objects), env.store.overwrites
    command.env = env.lose_catalog()
    command(rebuild=True)
    assert env.store.objects == objects and env.store.overwrites == overwrites


def test_a_rebuild_with_no_marker_is_exit_1(command, env):
    code, _, err = command(rebuild=True)
    assert code == 1 and "no run marker" in errors(err)[0]


def test_a_rebuild_over_a_catalog_that_still_has_the_tables_is_exit_1(command, env):
    command(write=True)
    code, _, err = command(rebuild=True)
    assert code == 1 and "already has" in errors(err)[0]


def test_a_rebuild_that_does_not_match_the_marker_is_exit_2(command, env):
    import json
    command(write=True)
    (key,) = [k for k in env.store.objects if k.startswith("_runs/")]
    body = json.loads(env.store.objects[key])
    body["tables"]["risk_events"]["rows"] += 1
    env.store.objects[key] = json.dumps(body).encode()
    command.env = env.lose_catalog()
    code, _, err = command(rebuild=True)
    assert code == 2 and "registered from" in errors(err)[0]


# ---------------------------------------------------------------------------
# the real catalog and the real command line
# ---------------------------------------------------------------------------

def test_the_real_catalog_is_a_sqlite_file_with_its_tables_in_the_bronze_bucket(tmp_path, monkeypatch):
    for variable in ("BRONZE_BUCKET", "BRONZE_CATALOG_PATH"):
        monkeypatch.delenv(variable, raising=False)
    path = tmp_path / "deep" / "er" / "catalog.db"
    catalog = open_catalog(BronzeSettings(_env_file=None, catalog_path=str(path)))
    assert path.is_file() and catalog.properties["warehouse"] == "gs://fintech_bronze"
    assert catalog.properties["uri"] == f"sqlite:///{path}"
    other = open_catalog(BronzeSettings(_env_file=None, catalog_path=str(tmp_path / "c2.db"), bucket="elsewhere"))
    assert other.properties["warehouse"] == "gs://elsewhere"


def test_the_command_refuses_to_start_without_credentials_as_a_subprocess():
    env = {k: v for k, v in os.environ.items() if k != CREDENTIALS_VARIABLE}
    for args in ([], ["--write"], ["--rebuild-catalog"]):
        result = subprocess.run([sys.executable, "-W", "ignore", "-m", "bronze.main", *args], cwd=ROOT,
                                env={**env, "PYTHONPATH": "."}, capture_output=True, text=True, timeout=60)
        assert result.returncode == 1, args
        assert result.stderr.startswith("error: GOOGLE_APPLICATION_CREDENTIALS is not set") and result.stdout == ""
        assert "Traceback" not in result.stderr


def test_a_usage_error_is_exit_1_because_2_means_a_contract_stop(capsys):
    assert main(["--nope"]) == 1
    assert capsys.readouterr().err.startswith("error: ")


def test_help_exits_zero_and_describes_both_flags(capsys):
    assert main(["--help"]) == 0
    text = capsys.readouterr().out
    assert "--write" in text and "--rebuild-catalog" in text and "GOOGLE_APPLICATION_CREDENTIALS" in text
    assert "only plans" in text


def test_render_tables_lines_up_and_is_a_plan_or_a_write(command, env):
    from bronze.run import run as run_function
    report = run_function(command.land.reader(), env.catalog, env.store, catalog_file=env.catalog_file, warehouse=env.uri,
                          source_db=SOURCE_DB, write=False)
    text = render_tables(report)
    lines = text.splitlines()
    assert lines[0].split() == ["table", "landed", "in", "Bronze", "to", "append", "objects", "batches"] and set(lines[1].replace(" ", "")) == {"-"}
    assert len(lines) == 2 + 5 and all(len(l) <= len(max(lines, key=len)) for l in lines)
    assert "new table" in text and " - " in lines[2] + " "


# ---------------------------------------------------------------------------
# wording and layout the first tests did not pin
# ---------------------------------------------------------------------------

def test_numbers_have_thousands_separators_and_the_plural_follows_the_count():
    assert _count(1234, "row") == "1,234 rows" and _count(1, "row") == "1 row" and _count(0, "row") == "0 rows"
    assert _count(2, "batch", "batches") == "2 batches" and _count(1, "batch", "batches") == "1 batch"


def test_memory_is_in_megabytes_or_says_unknown():
    assert _megabytes(None) == "unknown" and _megabytes(2_500_000_000) == "2,500 MB"


def test_a_machine_whose_memory_cannot_be_read_is_said_to_be_unknown(command, monkeypatch):
    monkeypatch.setattr(main_module, "machine_memory_bytes", lambda: None)
    _, out, _ = command()
    assert re.search(r"peak memory \d[\d,]* MB of unknown on this machine", out)


def test_the_numbers_in_the_table_line_up_on_their_right_edges(command, env):
    from bronze.run import run as run_function
    report = run_function(command.land.reader(), env.catalog, env.store, catalog_file=env.catalog_file,
                          warehouse=env.uri, source_db=SOURCE_DB, write=False)
    lines = render_tables(report).splitlines()
    header, rows = lines[0], lines[2:]
    ends = [header.index("landed") + len("landed"), header.index("in Bronze") + len("in Bronze"),
            header.index("to append") + len("to append"), header.index("objects") + len("objects")]
    for row in rows:
        assert [row[e - 1] != " " and (len(row) == e or row[e] == " ") for e in ends] == [True] * 4, row
    assert any(len(row.split()[1]) != len(rows[0].split()[1]) for row in rows)       # the widths differ, so this is a test


def test_a_plan_over_an_empty_landing_with_no_marker_still_says_a_marker_would_be_written(env, creds, monkeypatch):
    _, out, _ = Command(env, Landing(), creds, settings_for(env, monkeypatch))()
    assert "a write would create 5 tables, write a run marker, copy the catalog to the bucket" in out


def test_a_plan_where_the_tables_have_moved_past_the_marker_says_a_marker_would_be_written(command, env):
    from bronze.batches import arrow_schema_of, events_to_arrow
    command(write=True)
    row = event("merchants", "c", 0x7FFFFF00, 0, 1)
    command.land.put("merchants", DAY, [row])
    table = env.table("merchants")
    table.append(events_to_arrow([row], arrow_schema_of(table.schema()), source_db=SOURCE_DB))
    _, out, _ = command()
    assert "not at it" in out and "a write would write a run marker, copy the catalog to the bucket" in out
    assert "do nothing" not in out and "append" not in out.split("a write would", 1)[1]


def test_a_rebuilt_table_with_no_snapshot_is_listed_as_having_none(env, creds, monkeypatch):
    command = Command(env, Landing(), creds, settings_for(env, monkeypatch))
    command(write=True)
    command.env = env.lose_catalog()
    _, out, _ = command(rebuild=True)
    assert "  merchants: snapshot none, 0 rows" in out


def test_a_system_exit_is_not_swallowed_into_an_exit_code(env, creds, monkeypatch):
    def leave(settings, environ):
        raise SystemExit(9)

    with pytest.raises(SystemExit):
        execute(settings_for(env, monkeypatch), landing_factory=leave, store_factory=lambda s, e: env.store,
                catalog_factory=lambda s: env.catalog, environ=creds, out=io.StringIO(), err=io.StringIO())


def test_the_flags_reach_execute_and_nothing_is_written_by_default(monkeypatch):
    seen = []
    monkeypatch.setattr(main_module, "execute", lambda settings, **kwargs: seen.append(kwargs) or 0)
    assert main([]) == 0 and main(["--write"]) == 0 and main(["--rebuild-catalog"]) == 0
    assert seen == [{"write": False, "rebuild": False, "verified_marker_file": None},
                    {"write": True, "rebuild": False, "verified_marker_file": None},
                    {"write": False, "rebuild": True, "verified_marker_file": None}]


def test_the_exit_code_of_execute_is_the_exit_code_of_the_command(monkeypatch):
    monkeypatch.setattr(main_module, "execute", lambda settings, **kwargs: 2)
    assert main(["--write"]) == 2


def test_a_write_records_the_new_marker_as_verified_in_the_file_it_is_given(command, env, tmp_path):
    path = tmp_path / "verified"
    code, out, err = command(write=True, verified_marker_file=path)
    assert code == 0 and errors(err) == []
    marker = newest_marker(env.store)
    assert path.read_text() == marker.run_id + "\n"
    assert f"verified marker: {marker.run_id}" in out


def test_a_write_that_skipped_the_index_leaves_no_file_and_removes_a_stale_one(command, env, tmp_path):
    command(write=True)
    path = tmp_path / "verified"
    path.write_text("stale\n")
    code, out, err = command(write=True, verified_marker_file=path)
    assert code == 0 and errors(err) == [] and "the index was skipped" in out
    assert not path.exists()


def test_a_failed_write_leaves_no_verified_marker_file(env, creds, monkeypatch, tmp_path):
    land = spread_landing(6)
    objects = len(land.sink.objects)
    state = {"n": 0}

    def fail(key, n):
        state["n"] += 1
        if state["n"] == objects + 4:
            raise SinkError("injected: the bucket gave up")

    command = Command(env, land, creds, settings_for(env, monkeypatch, batch_rows=1, read_workers=1),
                      source=type("S", (), {"reader": lambda self: Spy(land.sink, fail).reader()})())
    path = tmp_path / "verified"
    path.write_text("stale\n")
    code, out, err = command(write=True, verified_marker_file=path)
    assert code == 3
    assert not path.exists()


def test_the_verified_marker_file_without_write_is_a_usage_error_before_anything_is_opened(command, tmp_path):
    path = tmp_path / "verified"
    code, out, err = command(verified_marker_file=path)
    assert code == 1 and len(errors(err)) == 1 and "needs --write" in errors(err)[0]
    assert command.opened == [] and not path.exists()


def test_a_write_over_an_existing_marker_records_the_new_one_as_verified(command, env, tmp_path):
    command(write=True)
    old = newest_marker(env.store).run_id
    command.land.transaction(("merchants", "c"), ("merchant_transactions", "c"), date="2025-05-05")
    command.land.flush()
    path = tmp_path / "verified"
    code, out, err = command(write=True, verified_marker_file=path)
    new = newest_marker(env.store).run_id
    assert code == 0 and errors(err) == [] and new != old
    assert path.read_text() == new + "\n"
