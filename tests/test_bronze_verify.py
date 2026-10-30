"""tests/test_bronze_verify.py: G4 step 2, python -m bronze.verify, its output, its flag, its checks before any Google
operation and its exit codes.

execute() takes factories for the landing and the catalog and a stand-in for the check, so the command is driven over a
synthetic landing in memory and a local warehouse. Nothing here touches GCS. The checks of the source database itself
(check_source) run against real Postgres databases built for the test (they skip locally when Postgres is down and fail
under CI); the whole command against a real source and a real landing is in tests/test_cdc_roundtrip.py (slow). Held here:
  - the plain command is the full proof and the flag is the exception: --landing-only never touches the source, says
    twice that it is NOT FULL PROOF, and its pass is worded differently from VERIFIED;
  - every cheap check comes before the first Google operation, in order: credentials, the local catalog holding the five
    tables, the source database reachable and allowed to create the scratch database and holding the five tables; a
    failure of any reads nothing from the landing, and the source is never skipped quietly;
  - the three ways to not pass are told apart: NOT VERIFIED (a mismatch, with a PROBLEM line each), NOT PROVEN (the source
    changed during the check: not a mismatch, not a pass) and both together;
  - the figures: landed against Bronze with the snapshot each table was read at, the source against the replay, the updates
    against the disbursed loans, time and memory;
  - every exit code (0, 1, 2, 3, 130) with exactly one "error:" line, and no password in any output;
  - the real command refuses to start without credentials, as a subprocess.
"""
import io
import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

import bronze.verify as verify
from bronze.config import CREDENTIALS_VARIABLE, BronzeSettings
from bronze.errors import ConfigError, MappingError, SourceError
from bronze.replay import Digest, Report, check
from bronze.run import TABLE_ORDER
from bronze.verify import NOT_FULL_PROOF, check_catalog, check_source, execute, exit_code, main, reason, render
from cdc.errors import SinkError
from tests.bronze_env import Env, Spy, busy_landing
from tests.bronze_synth import SOURCE_DB
from tests.fakes import MemorySink
from tests.test_bronze_replay import reread

ROOT = Path(__file__).resolve().parent.parent
PASSWORD = "s3cret-pa55word"
DSN = f"host=localhost port=5432 dbname=fintech_cdc user=sim password={PASSWORD}"
DEAD_DSN = f"host=127.0.0.1 port=1 dbname=fintech_cdc user=sim password={PASSWORD}"
TABLES = ("merchants", "merchant_transactions", "risk_events", "loan_applications", "loan_repayments")


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
                     "BRONZE_BATCH_ROWS", "CDC_GCS_BUCKET", "CDC_SLOT_NAME", "CDC_DB", "CDC_HOST", "CDC_PORT", "CDC_USER",
                     "CDC_PASSWORD"):
        monkeypatch.delenv(variable, raising=False)
    return BronzeSettings(_env_file=None, catalog_path=str(env.catalog_file), **overrides)


class Verify:
    """The command over a local warehouse: it remembers what was opened and checked, and in what order."""

    def __init__(self, env, land, creds, settings, source=None) -> None:
        self.env, self.land, self.creds, self.settings, self.source = env, land, creds, settings, source
        self.opened = []
        self.spy = Spy((source or land).sink)

    def __call__(self, **kwargs):
        out, err = io.StringIO(), io.StringIO()

        def landing(settings, environ):
            self.opened.append("landing")
            return self.spy.reader()

        def catalog(settings):
            self.opened.append("catalog")
            return self.env.catalog

        kwargs.setdefault("environ", self.creds)
        kwargs.setdefault("source_dsn", DSN)
        code = execute(self.settings, landing_factory=landing, catalog_factory=catalog, out=out, err=err, **kwargs)
        return code, out.getvalue(), err.getvalue()


@pytest.fixture
def verifier(env, creds, monkeypatch, tmp_path):
    """Bronze written from a busy landing; the source check is a recorder that passes (it is tested on its own below)."""
    land = busy_landing()
    env.run(land.reader())
    command = Verify(env, land, creds, settings_for(env, monkeypatch))
    command.source_checked = []

    def recorder(source_dsn, settings):
        command.opened.append("source check")
        command.source_checked.append(source_dsn)

    monkeypatch.setattr(verify, "check_source", recorder)
    return command


def errors(err: str) -> list[str]:
    return [l for l in err.splitlines() if l.startswith("error:")]


def problems(err: str) -> list[str]:
    return [l for l in err.splitlines() if l.startswith("PROBLEM:")]


def full_report(**changes) -> Report:
    """A finished full check over five tables with the real run's shape of figures."""
    report = Report(
        landed={t: Digest(10 + n, 0xA0 + n) for n, t in enumerate(TABLES)},
        bronze={t: Digest(10 + n, 0xA0 + n) for n, t in enumerate(TABLES)},
        source={t: (10 + n, 0xB0 + n) for n, t in enumerate(TABLES)},
        replayed={t: (10 + n, 0xB0 + n) for n, t in enumerate(TABLES)},
        source_before={t: (10 + n, 0xB0 + n) for n, t in enumerate(TABLES)},
        snapshots={t: 1000 + n for n, t in enumerate(TABLES)}, updates=18, disbursed=18, objects=908)
    for name, value in changes.items():
        setattr(report, name, value)
    return report


def stub(report):
    """A check_fn that returns `report` and remembers how it was called."""
    calls = []

    def check_fn(catalog, reader, source_dsn, *, source_db, say, workers):
        calls.append({"source_dsn": source_dsn, "source_db": source_db, "say": say, "workers": workers,
                      "catalog": catalog, "reader": reader})
        return report

    check_fn.calls = calls
    return check_fn


# ---------------------------------------------------------------------------
# --landing-only: the real check, no source, and never full proof
# ---------------------------------------------------------------------------

def test_landing_only_passes_and_says_so_is_not_full_proof(verifier):
    code, out, err = verifier(landing_only=True)
    assert code == 0 and errors(err) == [] and problems(err) == []
    assert out.count(NOT_FULL_PROOF) == 3                          # the top, the figures and the verdict
    assert out.rstrip().splitlines()[-1] == f"OK (landing only): every landed event is in Bronze exactly once. {NOT_FULL_PROOF}"
    assert "VERIFIED" not in out and "source database" not in out.replace("source database fintech_cdc", "")


def test_landing_only_never_touches_the_source_not_even_to_look(verifier, monkeypatch):
    code, out, err = verifier(landing_only=True, source_dsn=DEAD_DSN)
    assert code == 0 and errors(err) == []
    assert verifier.source_checked == [] and verifier.opened == ["catalog", "landing"]
    assert "source digest" not in out and "replay" not in out


def test_landing_only_prints_the_counts_the_digests_and_the_snapshots(verifier, env):
    code, out, err = verifier(landing_only=True)
    report = check(env.catalog, verifier.land.reader(), None, source_db=SOURCE_DB)
    for table in TABLES:
        line = next(l for l in out.splitlines() if l.startswith(table + " "))
        cells = line.split()
        assert cells[1:3] == [str(report.landed[table].rows)] * 2
        assert cells[3:5] == [f"{report.landed[table].total:x}", f"{report.bronze[table].total:x}"]
        assert cells[5] == str(env.snapshots()[table][-1])
    assert f"landed objects read    {report.objects}" in out
    assert f"{'total':<21}  {sum(d.rows for d in report.landed.values()):>12,}" in out


def test_a_changed_value_in_a_landed_object_is_a_mismatch_with_a_problem_line_and_exit_2(verifier, env):
    sink = MemorySink()
    sink.objects = dict(verifier.land.sink.objects)
    name = next(n for n in sorted(sink.objects) if "/merchants/" in n)
    sink.objects[name] = reread(sink.objects[name], lambda events: events[0]["after"].update(merchant_id="somebody else"))
    verifier.spy = Spy(sink)
    code, out, err = verifier(landing_only=True)
    assert code == 2
    assert problems(err) == ["PROBLEM: merchants: Bronze holds the same number of changes as landed (6) but not the same events"]
    assert errors(err) == ["error: NOT VERIFIED: 1 problems"]
    assert "landed against Bronze" in out and "OK" not in out and "VERIFIED" not in out


def test_landing_only_with_a_table_missing_from_bronze_is_exit_1_before_any_read(env, creds, monkeypatch):
    land = busy_landing()                                          # nothing written: the catalog has no tables
    command = Verify(env, land, creds, settings_for(env, monkeypatch))
    code, out, err = command(landing_only=True)
    assert code == 1 and out == ""
    assert errors(err) == [errors(err)[0]] and "--rebuild-catalog" in err and "merchants" in err
    assert command.opened == ["catalog"] and command.spy.listings == 0 and command.spy.reads == []


# ---------------------------------------------------------------------------
# every cheap check comes first and a failure of one reads nothing from the landing
# ---------------------------------------------------------------------------

def test_without_credentials_nothing_is_opened(verifier):
    code, out, err = verifier(environ={})
    assert code == 1 and verifier.opened == [] and errors(err) == [errors(err)[0]]
    assert "GOOGLE_APPLICATION_CREDENTIALS" in err


def test_the_checks_run_in_order_credentials_catalog_source_then_the_landing(verifier):
    code, out, err = verifier(check_fn=stub(full_report()))
    assert code == 0 and verifier.opened == ["catalog", "source check", "landing"]
    assert verifier.source_checked == [DSN]


def test_a_catalog_without_the_tables_stops_before_the_source_and_the_landing(env, creds, monkeypatch):
    command = Verify(env, busy_landing(), creds, settings_for(env, monkeypatch))
    monkeypatch.setattr(verify, "check_source", lambda *a: command.opened.append("source check"))
    code, out, err = command(check_fn=stub(full_report()))
    assert code == 1 and command.opened == ["catalog"] and command.spy.reads == []


def test_a_lost_catalog_says_how_to_rebuild_it_and_names_the_file(env, creds, monkeypatch):
    settings = settings_for(env, monkeypatch)
    with pytest.raises(ConfigError) as caught:
        check_catalog(env.catalog, settings)
    message = str(caught.value)
    assert str(env.catalog_file) in message and "python -m bronze.main --rebuild-catalog" in message
    assert "BRONZE_CATALOG_PATH" in message and all(t in message for t in TABLES)


def test_a_catalog_missing_only_one_table_names_only_that_table(verifier, env, monkeypatch):
    env.catalog.drop_table("bronze.risk_events")
    with pytest.raises(ConfigError) as caught:
        check_catalog(env.catalog, verifier.settings)
    assert "for risk_events." in str(caught.value) and "merchants" not in str(caught.value)


def test_a_complete_catalog_passes_the_check(verifier, env):
    assert check_catalog(env.catalog, verifier.settings) is None


def test_an_unreachable_source_fails_with_exit_3_before_one_landing_read(env, creds, monkeypatch):
    command = Verify(env, busy_landing(), creds, settings_for(env, monkeypatch))
    env.run(command.land.reader())
    code, out, err = command(source_dsn=DEAD_DSN)
    assert code == 3 and out == ""
    assert command.opened == ["catalog"] and command.spy.listings == 0 and command.spy.reads == []
    assert len(errors(err)) == 1 and "127.0.0.1:1/fintech_cdc" in err
    assert "never skipped" in err and "--landing-only" in err and "NOT full proof" in err
    assert PASSWORD not in out + err


def test_an_unreachable_source_is_never_a_skip_and_never_a_pass(env, creds, monkeypatch):
    command = Verify(env, busy_landing(), creds, settings_for(env, monkeypatch))
    env.run(command.land.reader())
    assert command(source_dsn=DEAD_DSN)[0] == 3 != 0


def test_the_source_defaults_to_the_cdc_database_and_its_password_is_never_printed(verifier, monkeypatch):
    monkeypatch.setenv("CDC_PASSWORD", PASSWORD)
    monkeypatch.setenv("CDC_HOST", "db.example")
    code, out, err = verifier(check_fn=stub(full_report()), source_dsn=None)
    assert code == 0
    assert "host=db.example port=5432 dbname=fintech_cdc" in verifier.source_checked[0] and PASSWORD in verifier.source_checked[0]
    assert "db.example:5432/fintech_cdc" in out and PASSWORD not in out + err


def test_where_names_host_port_and_database_with_defaults_and_never_the_password():
    assert verify._where(DSN) == "localhost:5432/fintech_cdc"
    assert verify._where("host=db.example port=6543 dbname=x user=u password=p") == "db.example:6543/x"
    assert verify._where("dbname=only") == "localhost:5432/only"
    assert PASSWORD not in verify._where(DSN)


# ---------------------------------------------------------------------------
# the source database's own checks (real Postgres)
# ---------------------------------------------------------------------------

def test_a_usable_source_passes_the_source_check(blank_dsn, env, monkeypatch):
    assert check_source(blank_dsn, settings_for(env, monkeypatch)) is None


def test_an_unreachable_source_raises_with_the_way_out_in_the_message(env, monkeypatch):
    with pytest.raises(SourceError) as caught:
        check_source(DEAD_DSN, settings_for(env, monkeypatch))
    message = str(caught.value)
    assert message.startswith("cannot use the source database 127.0.0.1:1/fintech_cdc: ")
    assert "never skipped" in message and "--landing-only" in message and PASSWORD not in message


def test_a_database_without_the_five_tables_is_refused_naming_the_first_missing_one(blank_dsn, env, monkeypatch):
    from psycopg import conninfo
    name = f"verify_empty_{os.getpid()}"
    with psycopg.connect(blank_dsn, autocommit=True) as admin:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.execute(f'CREATE DATABASE "{name}"')
        try:
            with pytest.raises(SourceError) as caught:
                check_source(conninfo.make_conninfo(blank_dsn, dbname=name), settings_for(env, monkeypatch))
        finally:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    assert "it has no table merchants; is fintech_cdc the right database?" in str(caught.value)


def test_a_database_missing_only_the_last_table_is_refused_too(blank_dsn, env, monkeypatch):
    from psycopg import conninfo
    name = f"verify_short_{os.getpid()}"
    with psycopg.connect(blank_dsn, autocommit=True) as admin:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.execute(f'CREATE DATABASE "{name}"')
        try:
            dsn = conninfo.make_conninfo(blank_dsn, dbname=name)
            with psycopg.connect(dsn, autocommit=True) as setup:
                for table in TABLE_ORDER[:-1]:
                    setup.execute(f"CREATE TABLE {table} (x int)")
            with pytest.raises(SourceError) as caught:
                check_source(dsn, settings_for(env, monkeypatch))
        finally:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    assert TABLE_ORDER[-1] == "merchant_transactions"
    assert "it has no table merchant_transactions" in str(caught.value)


def test_a_user_who_may_not_create_a_database_is_refused_before_anything_is_read(blank_dsn, env, monkeypatch):
    from psycopg import conninfo
    role = f"verify_nocreate_{os.getpid()}"
    with psycopg.connect(blank_dsn, autocommit=True) as admin:
        admin.execute(f"DROP ROLE IF EXISTS {role}")
        admin.execute(f"CREATE ROLE {role} LOGIN PASSWORD 'pw' NOSUPERUSER NOCREATEDB")
        try:
            dsn = conninfo.make_conninfo(blank_dsn, user=role, password="pw")
            with pytest.raises(SourceError) as caught:
                check_source(dsn, settings_for(env, monkeypatch))
        finally:
            admin.execute(f"DROP ROLE IF EXISTS {role}")
    assert "its user may not create a database" in str(caught.value) and "--landing-only" in str(caught.value)


# ---------------------------------------------------------------------------
# the three ways not to pass, and the one way to pass
# ---------------------------------------------------------------------------

def test_a_full_pass_is_verified_with_the_figures_and_exit_0(verifier):
    check_fn = stub(full_report())
    code, out, err = verifier(check_fn=check_fn)
    assert code == 0 and errors(err) == [] and problems(err) == []
    assert out.rstrip().splitlines()[-1] == ("VERIFIED: Bronze equals the source database fintech_cdc in all five tables by "
                                             "digest, every landed event is in Bronze exactly once, and the 18 updates in "
                                             "Bronze are the 18 disbursed loans")
    assert NOT_FULL_PROOF not in out and "landing only" not in out
    assert "updates held in Bronze 18, loans the source shows as disbursed 18" in out
    assert "landed objects read    908" in out


def test_the_verdict_counts_the_updates_and_the_disbursed_loans_separately(verifier):
    # a replayed disbursement is a second update for the same loan, so the two counts can differ on a pass
    out = verifier(check_fn=stub(full_report(updates=19, disbursed=18)))[1]
    assert out.rstrip().splitlines()[-1].endswith("and the 19 updates in Bronze are the 18 disbursed loans")


def test_the_check_is_called_with_the_source_the_database_name_the_workers_and_the_progress_line(verifier, env):
    check_fn = stub(full_report())
    verifier(check_fn=check_fn)
    (call,) = check_fn.calls
    assert call["source_dsn"] == DSN and call["source_db"] == "fintech_cdc" and call["workers"] == 8
    assert call["catalog"] is env.catalog
    call["say"]("progress line")


def test_progress_goes_to_stderr_and_not_to_the_figures(verifier):
    def check_fn(catalog, reader, source_dsn, *, source_db, say, workers):
        say("landed side: 100 of 908 objects")
        return full_report()

    code, out, err = verifier(check_fn=check_fn)
    assert "landed side: 100 of 908 objects" in err
    assert "landed side: 100 of 908 objects" not in out


def test_landing_only_gives_the_check_no_source(verifier):
    check_fn = stub(full_report(source={}, replayed={}, source_before={}, updates=1, disbursed=None))
    verifier(landing_only=True, check_fn=check_fn)
    assert check_fn.calls[0]["source_dsn"] is None


def test_the_workers_setting_reaches_the_check(env, creds, monkeypatch):
    command = Verify(env, busy_landing(), creds, settings_for(env, monkeypatch, read_workers=3))
    env.run(command.land.reader())
    monkeypatch.setattr(verify, "check_source", lambda *a: None)
    check_fn = stub(full_report())
    command(check_fn=check_fn)
    assert check_fn.calls[0]["workers"] == 3


def test_problems_are_not_verified_one_problem_line_each_and_exit_2(verifier):
    code, out, err = verifier(check_fn=stub(full_report(problems=["merchants: 5 changes landed, Bronze holds 4",
                                                                 "risk_events: the table replayed from Bronze differs"])))
    assert code == 2
    assert problems(err) == ["PROBLEM: merchants: 5 changes landed, Bronze holds 4",
                             "PROBLEM: risk_events: the table replayed from Bronze differs"]
    assert errors(err) == ["error: NOT VERIFIED: 2 problems"]
    assert "VERIFIED" not in out and "landed objects read" in out         # the figures are still printed


def test_a_moved_source_is_not_proven_not_a_mismatch_and_exit_2(verifier):
    code, out, err = verifier(check_fn=stub(full_report(moved=["merchants", "loan_applications"])))
    assert code == 2 and problems(err) == []
    (line,) = errors(err)
    assert line.startswith("error: NOT PROVEN: the source changed during the check (tables: merchants, loan_applications).")
    assert "not a Bronze mismatch and not a pass" in line and "when the source is quiet" in line
    assert "NOT VERIFIED" not in err and "VERIFIED" not in out


def test_a_moved_source_with_other_problems_says_both_in_one_error_line(verifier):
    code, out, err = verifier(check_fn=stub(full_report(moved=["merchants"], problems=["risk_events: 6 changes landed, Bronze holds 5"])))
    assert code == 2 and problems(err) == ["PROBLEM: risk_events: 6 changes landed, Bronze holds 5"]
    (line,) = errors(err)
    assert "NOT PROVEN" in line and "1 other problems were also found" in line


def test_exit_code_2_is_also_what_a_contract_stop_gives(verifier):
    def raising(*args, **kwargs):
        raise MappingError("merchants", "merchant_id", "a column the contract does not allow")

    code, out, err = verifier(check_fn=raising)
    assert code == 2 and len(errors(err)) == 1 and "merchant_id" in err


# ---------------------------------------------------------------------------
# every exit code, one line each
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("error, code", [
    (KeyboardInterrupt(), 130),
    (SinkError("the bucket gave up after 5 attempts"), 3),
    (SourceError("cannot use the source"), 3),
    (OSError("gcs failed"), 3),
    (psycopg.OperationalError("server closed the connection"), 3),
    (MappingError("merchants", "x", "bad"), 2),
    (ConfigError("a setting is wrong"), 1),
    (ValueError("a bug"), 1),
])
def test_every_way_out_is_an_exit_code_and_one_error_line(verifier, error, code):
    def raising(*args, **kwargs):
        raise error

    got, out, err = verifier(check_fn=raising)
    assert got == code == exit_code(error)
    assert len(errors(err)) == 1


def test_a_system_exit_from_inside_the_check_is_not_swallowed(verifier):
    def leaving(*args, **kwargs):
        raise SystemExit(7)

    with pytest.raises(SystemExit) as caught:
        verifier(check_fn=leaving)
    assert caught.value.code == 7


def test_an_interrupt_says_nothing_was_written(verifier):
    def raising(*args, **kwargs):
        raise KeyboardInterrupt

    assert "error: interrupted; nothing was written to either bucket" in verifier(check_fn=raising)[2]


def test_a_bug_prints_its_traceback_and_a_bronze_error_does_not(verifier):
    def bug(*args, **kwargs):
        raise ValueError("boom")

    def tidy(*args, **kwargs):
        raise ConfigError("tidy")

    assert "Traceback" in verifier(check_fn=bug)[2] and "unexpected ValueError: boom" in verifier(check_fn=bug)[2]
    assert "Traceback" not in verifier(check_fn=tidy)[2]


def test_a_source_that_fails_in_the_middle_of_the_check_says_it_was_the_source(verifier):
    def raising(*args, **kwargs):
        raise psycopg.OperationalError("server closed the connection unexpectedly")

    code, out, err = verifier(check_fn=raising)
    assert code == 3
    assert errors(err) == ["error: the source database failed during the check (OperationalError: server closed the "
                           "connection unexpectedly)"]


def test_a_failed_bucket_is_a_plain_exit_3(verifier):
    def raising(*args, **kwargs):
        raise SinkError("reading cdc/cdc_r01/x gave up after 5 attempts")

    code, out, err = verifier(check_fn=raising)
    assert code == 3 and errors(err) == ["error: reading cdc/cdc_r01/x gave up after 5 attempts"]


def test_reason_for_each_kind_of_error():
    assert reason(SinkError("a  b\n c")) == "a b c"
    assert reason(OSError("x")).startswith("a bucket or file operation failed (OSError: x")
    assert reason(ValueError("y")) == "unexpected ValueError: y"
    assert reason(KeyboardInterrupt()).startswith("interrupted")


def test_no_password_in_any_output_on_any_path(verifier):
    outputs = []
    for check_fn in (stub(full_report()), stub(full_report(problems=["x"])), stub(full_report(moved=["merchants"]))):
        code, out, err = verifier(check_fn=check_fn)
        outputs.append(out + err)
    outputs.append("".join(verifier(source_dsn=DEAD_DSN)[1:]))
    assert all(PASSWORD not in text for text in outputs)


# ---------------------------------------------------------------------------
# the figures
# ---------------------------------------------------------------------------

def test_render_shows_both_comparisons_with_digests_in_hex_and_the_snapshots():
    text = render(full_report(), landing_only=False)
    lines = text.splitlines()
    merchants = next(l for l in lines if l.startswith("merchants ") and "a0" in l)
    assert merchants.split() == ["merchants", "10", "10", "a0", "a0", "1000"]
    source = next(l for l in lines if l.startswith("merchants ") and "b0" in l)
    assert source.split() == ["merchants", "10", "10", "b0", "b0"]
    assert any(l.split()[:3] == ["total", "60", "60"] for l in lines)         # 10 + 11 + 12 + 13 + 14
    assert "the source against Bronze replayed into a blank database" in text


def test_render_shows_none_for_a_table_with_no_snapshot_and_unknown_for_a_missing_disbursed_count():
    text = render(full_report(snapshots={t: None for t in TABLES}, disbursed=None), landing_only=False)
    assert text.count(" none") == 5 and "disbursed unknown" in text


def test_render_landing_only_has_no_source_section_and_the_banner():
    text = render(full_report(source={}, replayed={}, disbursed=None), landing_only=True)
    assert text.endswith(NOT_FULL_PROOF) and "the source against Bronze" not in text and "updates held" not in text


def test_render_a_different_digest_between_landed_and_bronze_is_visible():
    report = full_report()
    report.bronze["risk_events"] = Digest(12, 0xFF)
    text = render(report, landing_only=False)
    assert next(l for l in text.splitlines() if l.startswith("risk_events ") and "ff" in l).split()[3:5] == ["a2", "ff"]


def test_render_keeps_each_side_in_its_own_column_and_totals_each_side_separately():
    report = full_report()
    report.landed["risk_events"] = Digest(12, 0xA2)
    report.bronze["risk_events"] = Digest(11, 0xFF)                        # Bronze holds one fewer than landed
    report.source["risk_events"], report.replayed["risk_events"] = (12, 0xB2), (11, 0xCC)
    lines = render(report, landing_only=False).splitlines()
    events = next(l for l in lines if l.startswith("risk_events ") and "ff" in l).split()
    assert events[:5] == ["risk_events", "12", "11", "a2", "ff"]          # landed first, Bronze second
    rows = next(l for l in lines if l.startswith("risk_events ") and "cc" in l).split()
    assert rows == ["risk_events", "12", "11", "b2", "cc"]                # source first, replay second
    assert next(l for l in lines if l.startswith("total")).split() == ["total", "60", "59"]    # landed 60, Bronze 59


def test_the_report_ends_with_the_time_and_the_memory(verifier):
    out = verifier(check_fn=stub(full_report()))[1]
    assert "min" in out and "peak memory" in out and "on this machine" in out


# ---------------------------------------------------------------------------
# the command line
# ---------------------------------------------------------------------------

def test_the_flag_reaches_execute_and_defaults_to_the_full_check(monkeypatch):
    seen = []
    monkeypatch.setattr(verify, "execute", lambda settings, **kwargs: seen.append(kwargs) or 0)
    assert main([]) == 0 and main(["--landing-only"]) == 0
    assert seen == [{"landing_only": False}, {"landing_only": True}]


def test_a_usage_error_is_exit_1_not_clicks_2(capsys):
    assert main(["--nonsense"]) == 1
    assert capsys.readouterr().err.startswith("error: ")


def test_the_help_names_the_flag_and_says_it_is_not_full_proof(capsys):
    assert main(["--help"]) == 0
    text = " ".join(capsys.readouterr().out.split())
    assert "--landing-only" in text and "NOT full proof" in text


@pytest.mark.parametrize("arguments", [[], ["--landing-only"]])
def test_the_real_command_refuses_to_start_without_credentials(arguments):
    environ = {k: v for k, v in os.environ.items() if k != CREDENTIALS_VARIABLE}
    done = subprocess.run([sys.executable, "-m", "bronze.verify", *arguments], cwd=ROOT, env=environ,
                          capture_output=True, text=True, timeout=120)
    assert done.returncode == 1 and "GOOGLE_APPLICATION_CREDENTIALS" in done.stderr and done.stdout == ""


# ---------------------------------------------------------------------------
# where the time went
# ---------------------------------------------------------------------------

from bronze.replay import STAGES
from bronze.verify import render_timings


def timed_report(**changes) -> Report:
    timings = {"source_before": 2.0, "landed_wait": 30.0, "landed_work": 60.0, "bronze_read": 40.0, "bronze_convert": 50.0,
               "bronze_digest": 20.0, "replay_insert": 100.0, "replay_updates": 1.0, "source_compare": 7.0}
    return full_report(timings=timings, total_seconds=320.0, **changes)


def test_the_time_table_shows_each_stage_that_ran_in_order_with_its_share_of_the_whole():
    lines = render_timings(timed_report())
    rows = {l.split("  ")[0].strip(): l for l in lines}
    labels = [label for key, label in STAGES]
    shown = [l for l in lines[2:] if any(l.startswith(label) for label in labels)]
    assert [next(label for label in labels if l.startswith(label)) for l in shown] == labels            # the order of STAGES
    assert "landed side: waiting for objects to arrive" in rows and rows["landed side: waiting for objects to arrive"].split()[-2:] == ["30.0", "9.4%"]
    assert rows["replay: inserting into the scratch database"].split()[-2:] == ["100.0", "31.2%"]


def test_what_no_stage_covers_is_shown_and_the_whole_check_is_the_last_figure():
    lines = render_timings(timed_report())
    other = next(l for l in lines if l.startswith("everything else")).split()
    assert other[-2:] == ["10.0", "3.1%"]                                  # 320 - 310
    whole = next(l for l in lines if l.startswith("the whole check")).split()
    assert whole[-2:] == ["320.0", "100.0%"]
    assert lines[-1].startswith("waiting is time the digest sat idle")


def test_a_stage_that_did_not_run_is_not_shown_and_a_report_without_timings_has_no_table():
    report = full_report(timings={"landed_wait": 1.0, "landed_work": 3.0}, total_seconds=4.0)
    lines = render_timings(report)
    assert any(l.startswith("landed side: waiting") for l in lines) and not any("replay" in l for l in lines)
    assert render_timings(full_report()) == []


def test_the_rest_is_never_negative_and_a_zero_total_does_not_divide():
    over = render_timings(full_report(timings={"landed_wait": 5.0}, total_seconds=4.0))
    assert next(l for l in over if l.startswith("everything else")).split()[-2] == "0.0"
    zero = render_timings(full_report(timings={"landed_wait": 0.0}, total_seconds=0.0))
    assert next(l for l in zero if l.startswith("the whole check")).split()[-1] == "-"


def test_the_full_report_ends_with_the_time_table_and_the_landing_only_report_puts_it_before_the_banner():
    full = render(timed_report(), landing_only=False)
    assert full.index("updates held in Bronze") < full.index("where the time went")
    landing = render(timed_report(source={}, replayed={}, disbursed=None), landing_only=True)
    assert landing.index("where the time went") < landing.index(NOT_FULL_PROOF) and landing.endswith(NOT_FULL_PROOF)


def test_the_time_table_is_in_what_the_command_prints(verifier):
    out = verifier(check_fn=stub(timed_report()))[1]
    assert "where the time went" in out and "the whole check" in out and "320.0" in out


def test_the_time_table_columns_line_up_whatever_the_longest_label_is():
    lines = render_timings(timed_report())
    table = [l for l in lines[1:-1]]                                       # the header, the stages, the rest, the whole
    assert len({len(l) for l in table}) == 1                               # every row is as wide as the header
    longest = max(len(label) for key, label in STAGES)
    assert table[0].index("seconds") >= longest                            # the numbers start after the longest label
    short = render_timings(full_report(timings={"landed_wait": 1.0}, total_seconds=2.0))
    assert len({len(l) for l in short[1:-1]}) == 1 and short[1].index("seconds") == len("landed side: waiting for objects to arrive") + 2 + 2
