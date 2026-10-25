"""
main.py: Simulation entry point.

Usage
-----
Backfill mode (advance as fast as possible over a date range):

    python -m simulator.main --mode backfill --start-date 2025-01-01 --end-date 2026-01-01

Streaming mode (advance at a controlled pace, writing events in near-real-time):

    python -m simulator.main --mode streaming --duration 3600

Both modes write events directly to Postgres via psycopg3.

Phase 7 work: implement the full main loop, checkpoint load/save, and
the streaming time-advance logic.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SA Fintech event generator: writes to Postgres (V1)"
    )
    parser.add_argument(
        "--mode",
        choices=["backfill", "streaming"],
        required=True,
        help="backfill: process a date range as fast as possible. "
             "streaming: advance at real-time pace.",
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        help="Start date for backfill mode (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        help="End date for backfill mode (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=3600,
        help="Duration in seconds for streaming mode (default: 3600)",
    )
    parser.add_argument(
        "--merchant-count",
        type=int,
        default=1_000,
        help="Number of merchants to simulate (default: 1000)",
    )
    return parser.parse_args()


def run_backfill(args: argparse.Namespace) -> None:
    if not args.start_date or not args.end_date:
        print("--start-date and --end-date are required for backfill mode", file=sys.stderr)
        sys.exit(1)
    # Phase 7 implementation
    raise NotImplementedError("Phase 7")


def run_streaming(args: argparse.Namespace) -> None:
    # Phase 7 implementation
    raise NotImplementedError("Phase 7")


def main() -> None:
    args = parse_args()
    if args.mode == "backfill":
        run_backfill(args)
    else:
        run_streaming(args)


if __name__ == "__main__":
    main()
