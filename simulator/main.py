"""
main.py: Simulation entry point.

Backfill mode (advance as fast as possible over a date range; the end date is exclusive):

    python -m simulator.main --mode backfill --start-date 2025-01-01 --end-date 2026-01-01 --merchant-count 100

Pick a stopped backfill up from its checkpoint (the start date, merchant count and seed
come from the checkpoint):

    python -m simulator.main --mode backfill --end-date 2026-01-01 --resume

Streaming mode (replay each simulated day at a controlled pace, writing events in
near-real-time). It continues from the checkpoint if one exists, such as a backfill's:

    python -m simulator.main --mode stream --duration-days 2

Both modes write events directly to Postgres via psycopg3.
"""
from __future__ import annotations

import logging
import os
from dataclasses import replace
from datetime import date

import click
import psycopg

from simulator import db
from simulator.config import settings
from simulator.runner import RunError, RunSettings, format_summary, run_backfill
from simulator.state import load_checkpoint
from simulator.streaming import run_stream


@click.command()
@click.option(
    "--mode",
    type=click.Choice(["backfill", "stream"]),
    required=True,
    help="backfill: process a date range as fast as possible. stream: advance at real-time pace.",
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="First simulated day (YYYY-MM-DD). Defaults to SIMULATION_START_DATE.",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Backfill stops before this day (YYYY-MM-DD, exclusive). Defaults to SIMULATION_END_DATE.",
)
@click.option(
    "--duration-days",
    type=int,
    default=1,
    show_default=True,
    help="Duration in simulated days for stream mode.",
)
@click.option(
    "--merchant-count",
    type=int,
    default=None,
    help="Merchants onboarded over the first six months, not merchants active: about two "
    "thirds ever transact. Defaults to DEFAULT_MERCHANT_COUNT.",
)
@click.option("--seed", type=int, default=None, help="Seeds a fresh run. Defaults to RANDOM_SEED.")
@click.option(
    "--checkpoint",
    "checkpoint_path",
    default=None,
    help="Checkpoint file, written after every simulated day. Defaults to CHECKPOINT_PATH.",
)
@click.option("--resume", is_flag=True, help="Backfill only: continue a stopped run from its checkpoint.")
@click.option(
    "--speed-factor",
    type=float,
    default=None,
    help="Stream mode: simulated seconds per real second. Defaults to STREAMING_SPEED_FACTOR.",
)
def main(
    mode: str,
    start_date,
    end_date,
    duration_days: int,
    merchant_count,
    seed,
    checkpoint_path,
    resume: bool,
    speed_factor,
) -> None:
    """SA Fintech synthetic event generator: writes to Postgres (V1)."""
    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(message)s")
    if mode == "backfill":
        _run_backfill(start_date, end_date, merchant_count, seed, checkpoint_path, resume)
    else:
        _run_stream(start_date, duration_days, merchant_count, seed, checkpoint_path, speed_factor)


def _backfill_settings(start_date, end_date, merchant_count, seed, checkpoint_path, resume: bool) -> RunSettings:
    path = checkpoint_path or settings.checkpoint_path
    start = start_date.date() if start_date else None
    count = merchant_count
    if resume:
        try:
            extra = load_checkpoint(path).extra
        except FileNotFoundError:
            raise click.UsageError(f"No checkpoint at {path}, so there is nothing to resume.") from None
        start = start or extra["start_date"]
        count = count or extra["merchant_count"]
        seed = seed if seed is not None else extra["seed"]
    return RunSettings(
        start_date=start or date.fromisoformat(settings.simulation_start_date),
        end_date=end_date.date() if end_date else date.fromisoformat(settings.simulation_end_date),
        merchant_count=count or settings.default_merchant_count,
        seed=seed if seed is not None else settings.random_seed,
        enable_risk_events=settings.enable_risk_events,
        enable_loan_applications=settings.enable_loan_applications,
        checkpoint_path=path,
    )


def _run_backfill(start_date, end_date, merchant_count, seed, checkpoint_path, resume: bool) -> None:
    run = _backfill_settings(start_date, end_date, merchant_count, seed, checkpoint_path, resume)
    if run.end_date <= run.start_date:
        raise click.UsageError("--end-date must be after --start-date")
    try:
        conn = db.connect()
    except psycopg.OperationalError as error:
        raise click.ClickException(f"Cannot reach Postgres ({error}). Is the container up?") from error
    try:
        summary = run_backfill(run, conn, resume=resume)
    except RunError as error:
        raise click.ClickException(str(error)) from error
    finally:
        conn.close()
    click.echo(format_summary(summary))


def _run_stream(start_date, duration_days: int, merchant_count, seed, checkpoint_path, speed_factor) -> None:
    path = checkpoint_path or settings.checkpoint_path
    # An existing checkpoint is continued; without one the run starts fresh.
    run = replace(
        _backfill_settings(start_date, None, merchant_count, seed, path, resume=os.path.exists(path)),
        end_date=None,
    )
    try:
        conn = db.connect()
    except psycopg.OperationalError as error:
        raise click.ClickException(f"Cannot reach Postgres ({error}). Is the container up?") from error
    try:
        summary = run_stream(run, conn, duration_days, speed_factor or settings.streaming_speed_factor)
    except RunError as error:
        raise click.ClickException(str(error)) from error
    finally:
        conn.close()
    click.echo(format_summary(summary))


if __name__ == "__main__":
    main()
