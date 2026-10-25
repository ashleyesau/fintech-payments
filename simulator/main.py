"""
main.py: Simulation entry point.

Backfill mode (advance as fast as possible over a date range):

    python -m simulator.main --mode backfill --start-date 2025-01-01 --end-date 2026-01-01

Streaming mode (advance at a controlled pace, writing events in near-real-time):

    python -m simulator.main --mode stream --duration-days 1

Both modes write events directly to Postgres via psycopg3.

Phase 7 work: implement the full main loop, checkpoint load/save, and
the streaming time-advance logic.
"""
from __future__ import annotations

import click


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
    help="Start date for backfill mode (YYYY-MM-DD).",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="End date for backfill mode (YYYY-MM-DD).",
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
    default=1_000,
    show_default=True,
    help="Number of merchants to simulate.",
)
def main(
    mode: str,
    start_date,
    end_date,
    duration_days: int,
    merchant_count: int,
) -> None:
    """SA Fintech synthetic event generator: writes to Postgres (V1)."""
    if mode == "backfill":
        _run_backfill(start_date, end_date, merchant_count)
    else:
        _run_stream(duration_days, merchant_count)


def _run_backfill(start_date, end_date, merchant_count: int) -> None:
    if not start_date or not end_date:
        raise click.UsageError("--start-date and --end-date are required for backfill mode")
    # Phase 7 implementation
    raise NotImplementedError("Phase 7")


def _run_stream(duration_days: int, merchant_count: int) -> None:
    # Phase 7 implementation
    raise NotImplementedError("Phase 7")


if __name__ == "__main__":
    main()
