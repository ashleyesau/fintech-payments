"""
Tests for the consumer's Google project and credentials guard (Terraform plan stage 6, questions 3 and 4).

The consumer runs as the cdc-consumer service account through an impersonated credentials file. That file
carries no Google project, so CDC_PROJECT supplies one, and the consumer refuses to start unless
GOOGLE_APPLICATION_CREDENTIALS names a file, so it cannot fall back to an unrestricted personal login.
"""
from __future__ import annotations

import io
from pathlib import Path

import pytest

import cdc.gcs
from cdc.config import CREDENTIALS_VARIABLE, CdcSettings, require_credentials
from cdc.errors import SinkError
from cdc.gcs import GcsSink
from cdc.main import _connect_sink, execute, exit_code
from tests.fakes import FakeReader, MemorySink
from tests.test_cdc_consumer import stream
from tests.test_cdc_consumer_live import PUBLICATION, write_chain5
from tests.test_cdc_gcs import FakeStorageClient
from tests.test_cdc_reader import slot_position

ROOT = Path(__file__).resolve().parent.parent
IMPERSONATED_FILE = (
    '{"type": "impersonated_service_account", "service_account_impersonation_url": '
    '"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/'
    'cdc-consumer@payments-512910.iam.gserviceaccount.com:generateAccessToken", '
    '"source_credentials": {"type": "authorized_user", "client_id": "x", "client_secret": "y", '
    '"refresh_token": "z"}, "delegates": []}')


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    """No CDC_ setting, no Google project and no credentials variable leaks in from the shell."""
    import os
    for name in [n for n in os.environ if n.startswith("CDC_")]:
        monkeypatch.delenv(name)
    for name in (CREDENTIALS_VARIABLE, "GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "CLOUDSDK_CORE_PROJECT"):
        monkeypatch.delenv(name, raising=False)


def settings(**kwargs) -> CdcSettings:
    return CdcSettings(_env_file=None, **kwargs)


def recording_client(monkeypatch):
    calls = []

    def make(*args, **kwargs):
        calls.append((args, kwargs))
        return FakeStorageClient()

    monkeypatch.setattr(cdc.gcs.storage, "Client", make)
    return calls


def credentials_file(tmp_path, text="{}"):
    path = tmp_path / "application_default_credentials.json"
    path.write_text(text)
    return path


# ---------------------------------------------------------------------------
# the project setting
# ---------------------------------------------------------------------------

def test_the_project_defaults_to_the_real_one():
    assert settings().project == "payments-512910"


def test_the_project_is_the_cdc_project_variable(monkeypatch):
    monkeypatch.setenv("CDC_PROJECT", "another-project")
    assert settings().project == "another-project"


def test_env_example_lists_cdc_project_with_its_default():
    pairs = dict(line.split("=", 1) for line in (ROOT / ".env.example").read_text().splitlines()
                 if "=" in line and not line.lstrip().startswith("#"))
    assert pairs["CDC_PROJECT"].strip() == settings().project


def test_the_sink_is_made_for_the_configured_project(monkeypatch, tmp_path):
    calls = recording_client(monkeypatch)
    monkeypatch.setenv(CREDENTIALS_VARIABLE, str(credentials_file(tmp_path)))
    _connect_sink(settings())
    assert calls == [((), {"project": "payments-512910"})]


def test_the_project_setting_changes_the_project_the_sink_is_made_for(monkeypatch, tmp_path):
    calls = recording_client(monkeypatch)
    monkeypatch.setenv(CREDENTIALS_VARIABLE, str(credentials_file(tmp_path)))
    _connect_sink(settings(project="another-project"))
    assert calls == [((), {"project": "another-project"})]


def test_the_sink_is_made_for_the_configured_bucket(monkeypatch, tmp_path):
    recording_client(monkeypatch)
    monkeypatch.setenv(CREDENTIALS_VARIABLE, str(credentials_file(tmp_path)))
    assert _connect_sink(settings(gcs_bucket="elsewhere")).bucket_name == "elsewhere"


def test_the_reconciler_makes_its_sink_for_the_configured_project_too(monkeypatch):
    from cdc import reconcile
    seen = []

    def connect(bucket_name, *args, **kwargs):
        seen.append((bucket_name, args, kwargs))
        raise SinkError("stop here")

    monkeypatch.setattr(GcsSink, "connect", staticmethod(connect))
    err = io.StringIO()
    code = reconcile.execute(settings(project="p2", gcs_bucket="b2"), out=io.StringIO(), err=err)
    assert code == 3 and seen == [("b2", (), {"project": "p2"})]


def test_the_permissions_command_takes_its_default_project_from_the_setting():
    import cdc.permissions as perm
    option = {p.name: p for p in perm.main.params}["project"]
    assert option.default == settings().project == "payments-512910"


def test_the_real_client_can_be_made_from_an_impersonated_file_with_no_project_anywhere(monkeypatch, tmp_path):
    # The bug as it happened to Bronze on the Air: no project in the file and none in the environment. This
    # builds the real storage.Client (no network: nothing is sent until the first list, read or write).
    monkeypatch.setenv(CREDENTIALS_VARIABLE, str(credentials_file(tmp_path, IMPERSONATED_FILE)))
    sink = _connect_sink(settings())
    assert isinstance(sink, GcsSink) and sink.bucket_name == "cdc_landing"
    assert sink._bucket.client.project == "payments-512910"


# ---------------------------------------------------------------------------
# the guard itself
# ---------------------------------------------------------------------------

def test_a_named_file_passes_and_is_returned(tmp_path):
    path = credentials_file(tmp_path)
    assert require_credentials({CREDENTIALS_VARIABLE: str(path)}) == path


def test_the_guard_reads_the_process_environment_by_default(monkeypatch, tmp_path):
    path = credentials_file(tmp_path)
    monkeypatch.setenv(CREDENTIALS_VARIABLE, str(path))
    assert require_credentials() == path
    monkeypatch.delenv(CREDENTIALS_VARIABLE)
    with pytest.raises(SinkError):
        require_credentials()


def test_a_value_is_stripped_before_it_is_used(tmp_path):
    path = credentials_file(tmp_path)
    assert require_credentials({CREDENTIALS_VARIABLE: f"  {path}\n"}) == path


def test_a_home_directory_in_the_value_is_expanded(monkeypatch, tmp_path):
    credentials_file(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    assert require_credentials({CREDENTIALS_VARIABLE: "~/application_default_credentials.json"}) == \
        tmp_path / "application_default_credentials.json"


@pytest.mark.parametrize("value", [None, "", " ", "\t\n"])
def test_an_unset_or_blank_variable_is_refused_and_the_message_says_why(value):
    environ = {} if value is None else {CREDENTIALS_VARIABLE: value}
    with pytest.raises(SinkError) as stop:
        require_credentials(environ)
    message = str(stop.value)
    assert CREDENTIALS_VARIABLE in message and "is not set" in message
    assert "cdc-consumer" in message and "overwrite or delete landed objects" in message


def test_a_file_that_does_not_exist_is_refused_and_named(tmp_path):
    missing = tmp_path / "nope.json"
    with pytest.raises(SinkError) as stop:
        require_credentials({CREDENTIALS_VARIABLE: str(missing)})
    assert "does not exist" in str(stop.value) and str(missing) in str(stop.value)


def test_a_directory_is_not_a_credentials_file(tmp_path):
    with pytest.raises(SinkError, match="does not exist"):
        require_credentials({CREDENTIALS_VARIABLE: str(tmp_path)})


def test_the_guard_never_reads_the_file(tmp_path):
    path = credentials_file(tmp_path, "{ this is not json, and holds a secret token")
    assert require_credentials({CREDENTIALS_VARIABLE: str(path)}) == path


def test_a_refusal_is_a_sink_error_so_it_exits_3():
    with pytest.raises(SinkError) as stop:
        require_credentials({})
    assert exit_code(stop.value) == 3


# ---------------------------------------------------------------------------
# the consumer's real path
# ---------------------------------------------------------------------------

def never_make_a_client(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("a storage client must not be made without credentials")

    monkeypatch.setattr(cdc.gcs.storage, "Client", refuse)


class ClosableReader(FakeReader):
    closed = False

    def close(self):
        self.closed = True


def test_the_real_sink_path_refuses_to_start_without_the_variable_before_anything_is_read(monkeypatch):
    never_make_a_client(monkeypatch)
    reader = ClosableReader(stream())
    out, err = io.StringIO(), io.StringIO()
    code = execute(settings(), False, reader_factory=lambda s: reader, out=out, err=err, handle_signals=False)
    assert code == 3
    lines = err.getvalue().strip().splitlines()
    assert len(lines) == 1 and lines[0].startswith("error: ") and CREDENTIALS_VARIABLE in lines[0]
    assert reader.fetches == 0 and reader.closed and out.getvalue() == ""


def test_the_real_sink_path_refuses_a_missing_file_with_exit_3(monkeypatch, tmp_path):
    never_make_a_client(monkeypatch)
    monkeypatch.setenv(CREDENTIALS_VARIABLE, str(tmp_path / "gone.json"))
    reader = ClosableReader(stream())
    err = io.StringIO()
    code = execute(settings(), False, reader_factory=lambda s: reader, out=io.StringIO(), err=err, handle_signals=False)
    assert code == 3 and "does not exist" in err.getvalue() and reader.fetches == 0


def test_an_injected_sink_never_meets_the_guard():
    # the in-memory and directory sinks the tests and test tools use do not go through _connect_sink
    reader, sink = ClosableReader(stream()), MemorySink()
    code = execute(settings(), False, reader_factory=lambda s: reader, sink_factory=lambda s: sink,
                   out=io.StringIO(), err=io.StringIO(), handle_signals=False)
    assert code == 0 and reader.fetches > 0


def test_with_the_variable_set_the_consumer_runs_through_the_real_path_to_its_sink(monkeypatch, tmp_path):
    monkeypatch.setenv(CREDENTIALS_VARIABLE, str(credentials_file(tmp_path)))
    recording_client(monkeypatch)
    reader = ClosableReader(stream())
    err = io.StringIO()
    code = execute(settings(), False, reader_factory=lambda s: reader, out=io.StringIO(), err=err, handle_signals=False)
    assert code == 0, err.getvalue()
    assert reader.fetches > 0


def test_a_real_slot_keeps_its_changes_when_the_guard_refuses(cdc_dsn, cdc_slot, monkeypatch):
    # The refusal must happen before the slot is read or confirmed: the changes stay waiting.
    import psycopg
    from cdc.reader import Reader
    from tests.test_cdc_main import live_settings
    never_make_a_client(monkeypatch)
    write_chain5(cdc_dsn, [1, 1, 2])
    before = slot_position(cdc_dsn, cdc_slot)
    out, err = io.StringIO(), io.StringIO()
    code = execute(live_settings(cdc_dsn, cdc_slot), False,
                   reader_factory=lambda s: Reader(cdc_dsn, cdc_slot, PUBLICATION), out=out, err=err,
                   handle_signals=False)
    assert code == 3 and CREDENTIALS_VARIABLE in err.getvalue()
    assert slot_position(cdc_dsn, cdc_slot) == before
    with Reader(cdc_dsn, cdc_slot, PUBLICATION) as peek:
        assert len(peek.fetch(1000)) > 0
