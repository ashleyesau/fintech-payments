"""Fail if a BigQuery external Bronze table's columns differ from the Bronze contract.

Runs after register_bq and before any dbt layer. The contract (bronze.schema.build_schema)
is the source of truth. Compares column names only.
"""
from __future__ import annotations

import argparse
import sys

from google.cloud import bigquery

from bronze.schema import build_schema

TABLES = ("merchants", "merchant_transactions", "loan_applications", "loan_repayments", "risk_events")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", default="bronze")
    parser.add_argument("--location", default="us-east1")
    args = parser.parse_args()

    client = bigquery.Client(project=args.project, location=args.location)
    problems = []
    for name in TABLES:
        expected = [f.name for f in build_schema(name).fields]
        actual = [f.name for f in client.get_table(f"{args.project}.{args.dataset}.{name}").schema]
        missing = [c for c in expected if c not in actual]
        extra = [c for c in actual if c not in expected]
        if missing or extra:
            problems.append(f"{name}: missing in BigQuery {missing}, not in contract {extra}")
    if problems:
        print("BigQuery external tables differ from the Bronze contract:", file=sys.stderr)
        for line in problems:
            print("  " + line, file=sys.stderr)
        return 2
    print(f"OK: {len(TABLES)} external tables match the Bronze contract")
    return 0


if __name__ == "__main__":
    sys.exit(main())
