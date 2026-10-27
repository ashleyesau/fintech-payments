"""
scripts/boosted_backfill.py: a backfill with the loan application rate multiplied, written to
Postgres, so the lending checks have enough loans to read.

At the default application rate a 15-month run originates about 120 to 205 loans and no
write-offs, so PAR and the vintage curves cannot be read from it. This runs the same backfill as
`python -m simulator.main --mode backfill`, with the monthly application propensity of Growing
and Stable merchants multiplied. Repayment behaviour is not touched. The 24-month, 300-merchant,
x5 runs that the Phase 8 lending numbers come from used the defaults below.

    PYTHONPATH=. python scripts/boosted_backfill.py --db val_x44 --seed 44

The database must already exist with migrations 001 and 002 applied and its tables empty; the run
refuses to start otherwise, as the CLI does. Then read it with

    POSTGRES_DB=val_x44 python -m simulator.validation

A boosted run is a measurement aid, not a scenario to demo: more loans change merchant risk scores
and states a little, so keep the default-rate runs for everything that is not lending.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date

from simulator import db, lending
from simulator.config import settings
from simulator.runner import RunSettings, format_summary, run_backfill


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--db", default=settings.postgres_db, help="database name (default POSTGRES_DB)")
    parser.add_argument("--seed", type=int, default=settings.random_seed)
    parser.add_argument("--merchants", type=int, default=300, help="merchants onboarded")
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2025, 1, 1))
    parser.add_argument("--end-date", type=date.fromisoformat, default=date(2027, 1, 1), help="exclusive")
    parser.add_argument("--boost", type=float, default=5.0, help="multiplier on the monthly application propensity")
    parser.add_argument("--checkpoint", default=None, help="default: checkpoint_<db>.json in the working directory")
    args = parser.parse_args()
    if args.end_date <= args.start_date:
        parser.error("--end-date must be after --start-date")

    for state in ("growing", "stable"):
        lending._PROPENSITY[state] *= args.boost

    dsn = (
        f"host={settings.postgres_host} port={settings.postgres_port} dbname={args.db} "
        f"user={settings.postgres_user} password={settings.postgres_password}"
    )
    run = RunSettings(
        start_date=args.start_date,
        end_date=args.end_date,
        merchant_count=args.merchants,
        seed=args.seed,
        checkpoint_path=args.checkpoint or f"checkpoint_{args.db}.json",
    )
    conn = db.connect(dsn)
    try:
        print(format_summary(run_backfill(run, conn)))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
