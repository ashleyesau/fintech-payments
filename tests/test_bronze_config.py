"""tests/test_bronze_config.py: G1 step 1, the Bronze settings and the credentials guard.

No cloud and no database. The guard is the cheap control that a Bronze command runs as the
bronze-writer account; the account's permissions are the real one (GCS Plan (V2) section 13).
"""
from pathlib import Path

import pytest

from bronze.config import CREDENTIALS_VARIABLE, BronzeSettings, require_credentials
from bronze.errors import BronzeError, ConfigError

ROOT = Path(__file__).parent.parent


def env_example() -> dict[str, str]:
    pairs = (line.split("=", 1) for line in (ROOT / ".env.example").read_text().splitlines()
             if line.startswith("BRONZE_"))
    return {key: value for key, value in pairs}


def clear_settings_env(monkeypatch):
    for key in list(env_example()) + ["CDC_GCS_BUCKET", "CDC_SLOT_NAME"]:
        monkeypatch.delenv(key, raising=False)


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

def test_env_example_lists_every_bronze_setting_with_its_default():
    fields = {f"BRONZE_{name.upper()}": field for name, field in BronzeSettings.model_fields.items()}
    listed = env_example()
    assert set(listed) == set(fields)
    for key, field in fields.items():
        assert field.annotation(listed[key]) == field.default, key


def test_the_defaults_are_the_decided_ones(monkeypatch):
    # G-D7: the bucket is fintech_bronze. G-D3: the SQLite catalog is outside the repo.
    clear_settings_env(monkeypatch)
    s = BronzeSettings(_env_file=None)
    assert s.bucket == "fintech_bronze"
    assert s.catalog_file.is_absolute()
    assert ROOT not in s.catalog_file.parents


def test_the_catalog_path_is_a_setting(monkeypatch, tmp_path):
    monkeypatch.setenv("BRONZE_CATALOG_PATH", str(tmp_path / "catalog.db"))
    assert BronzeSettings(_env_file=None).catalog_file == tmp_path / "catalog.db"


def test_the_landing_location_comes_from_the_cdc_settings(monkeypatch):
    clear_settings_env(monkeypatch)
    s = BronzeSettings(_env_file=None)
    assert (s.landing_bucket, s.slot_name, s.landing_prefix) == ("cdc_landing", "cdc_r01", "cdc/cdc_r01/")
    monkeypatch.setenv("CDC_GCS_BUCKET", "other_landing")
    monkeypatch.setenv("CDC_SLOT_NAME", "cdc_r02")
    assert (s.landing_bucket, s.slot_name, s.landing_prefix) == ("other_landing", "cdc_r02", "cdc/cdc_r02/")


def test_the_landing_bucket_is_not_a_bronze_setting():
    # One place says where the landed objects are. A BRONZE_ copy could disagree with the consumer's.
    assert not any("landing" in name or "slot" in name for name in BronzeSettings.model_fields)


def test_the_catalog_file_is_ignored_by_git():
    assert "catalog.db" in (ROOT / ".gitignore").read_text().splitlines()


# ---------------------------------------------------------------------------
# The credentials guard
# ---------------------------------------------------------------------------

def test_a_named_file_passes_and_its_path_is_returned(tmp_path):
    file = tmp_path / "application_default_credentials.json"
    file.write_text("{}")
    assert require_credentials({CREDENTIALS_VARIABLE: str(file)}) == file


def test_a_home_directory_path_is_expanded(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    (tmp_path / "creds.json").write_text("{}")
    assert require_credentials({CREDENTIALS_VARIABLE: "~/creds.json"}) == tmp_path / "creds.json"


def test_an_unset_variable_stops_the_command_and_says_why():
    with pytest.raises(ConfigError) as caught:
        require_credentials({})
    message = str(caught.value)
    assert CREDENTIALS_VARIABLE in message
    assert "own login" in message


@pytest.mark.parametrize("value", ["", "   "])
def test_an_empty_variable_is_the_same_as_an_unset_one(value):
    with pytest.raises(ConfigError, match="is not set"):
        require_credentials({CREDENTIALS_VARIABLE: value})


def test_a_missing_file_stops_the_command_and_names_the_path(tmp_path):
    missing = tmp_path / "nope.json"
    with pytest.raises(ConfigError) as caught:
        require_credentials({CREDENTIALS_VARIABLE: str(missing)})
    assert str(missing) in str(caught.value)


def test_a_directory_is_not_a_credentials_file(tmp_path):
    with pytest.raises(ConfigError):
        require_credentials({CREDENTIALS_VARIABLE: str(tmp_path)})


def test_the_guard_never_prints_what_is_in_the_file(tmp_path):
    # The file holds a refresh token. It is not read at all, so it cannot reach a message.
    file = tmp_path / "creds.json"
    file.write_text('{"refresh_token": "SECRET-VALUE"}')
    assert "SECRET-VALUE" not in str(require_credentials({CREDENTIALS_VARIABLE: str(file)}))


def test_the_guard_reads_the_process_environment_by_default(monkeypatch, tmp_path):
    file = tmp_path / "creds.json"
    file.write_text("{}")
    monkeypatch.delenv(CREDENTIALS_VARIABLE, raising=False)
    with pytest.raises(ConfigError):
        require_credentials()
    monkeypatch.setenv(CREDENTIALS_VARIABLE, str(file))
    assert require_credentials() == file


def test_a_config_error_is_a_bronze_error():
    assert issubclass(ConfigError, BronzeError)
