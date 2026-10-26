"""tests/test_main.py: how the CLI turns options into a run. No database needed."""
from datetime import date

import pytest
from click.testing import CliRunner

from simulator.config import settings
from simulator.main import _backfill_settings, main
from simulator.state import save_checkpoint

FLAGS = {"enable_risk_events": True, "enable_loan_applications": True}


def write_checkpoint(path, **extra) -> None:
    values = {"start_date": date(2025, 2, 1), "merchant_count": 321, "seed": 9}
    values.update(extra)
    save_checkpoint([], date(2025, 3, 1), FLAGS, str(path), extra=values)


def test_defaults_come_from_the_settings(tmp_path):
    run = _backfill_settings(None, None, None, None, str(tmp_path / "c.json"), resume=False)
    assert run.start_date == date.fromisoformat(settings.simulation_start_date)
    assert run.end_date == date.fromisoformat(settings.simulation_end_date)
    assert run.merchant_count == settings.default_merchant_count
    assert run.seed == settings.random_seed
    assert run.flags == {
        "enable_risk_events": settings.enable_risk_events,
        "enable_loan_applications": settings.enable_loan_applications,
    }


def test_options_override_the_settings(tmp_path):
    from datetime import datetime

    run = _backfill_settings(datetime(2025, 5, 1), datetime(2025, 8, 1), 50, 3, str(tmp_path / "c.json"), resume=False)
    assert (run.start_date, run.end_date, run.merchant_count, run.seed) == (
        date(2025, 5, 1), date(2025, 8, 1), 50, 3,
    )


def test_a_resume_takes_the_start_date_count_and_seed_from_the_checkpoint(tmp_path):
    path = tmp_path / "c.json"
    write_checkpoint(path)
    run = _backfill_settings(None, None, None, None, str(path), resume=True)
    assert run.start_date == date(2025, 2, 1)
    assert run.merchant_count == 321 and run.seed == 9


def test_an_explicit_option_still_wins_on_resume_and_is_then_checked_by_the_runner(tmp_path):
    path = tmp_path / "c.json"
    write_checkpoint(path)
    assert _backfill_settings(None, None, 500, None, str(path), resume=True).merchant_count == 500


def test_the_checkpoint_path_option_overrides_the_setting(tmp_path):
    run = _backfill_settings(None, None, None, None, str(tmp_path / "mine.json"), resume=False)
    assert run.checkpoint_path == str(tmp_path / "mine.json")


def test_resuming_without_a_checkpoint_is_a_usage_error(tmp_path):
    result = CliRunner().invoke(main, ["--mode", "backfill", "--resume", "--checkpoint", str(tmp_path / "none.json")])
    assert result.exit_code == 2
    assert "nothing to resume" in result.output


def test_an_end_date_that_is_not_after_the_start_date_is_a_usage_error(tmp_path):
    result = CliRunner().invoke(main, [
        "--mode", "backfill", "--start-date", "2025-02-01", "--end-date", "2025-02-01",
        "--checkpoint", str(tmp_path / "c.json"),
    ])
    assert result.exit_code == 2
    assert "--end-date must be after --start-date" in result.output


def test_help_describes_the_merchant_count_and_the_exclusive_end_date():
    result = CliRunner().invoke(main, ["--help"])
    text = " ".join(result.output.split())
    assert "not merchants active" in text
    assert "exclusive" in text
