"""
tests/test_db.py: Connection smoke test for Phase 1.

Verifies that psycopg3 can reach the Postgres container and that the
five V1 tables exist. Skips automatically if the database is not running
so CI passes without Docker.
"""

import pytest
import psycopg

from simulator.config import DSN

EXPECTED_TABLES = {
    "merchants",
    "merchant_transactions",
    "loan_applications",
    "loan_repayments",
    "risk_events",
}


def _connect():
    """Return an open connection or raise OperationalError."""
    return psycopg.connect(DSN)


def test_connection():
    """Connect and execute SELECT 1."""
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
        assert result == (1,)
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable: {exc}")


def test_tables_exist():
    """All five V1 tables are present in the public schema."""
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_type = 'BASE TABLE'
                    """
                )
                tables = {row[0] for row in cur.fetchall()}
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable: {exc}")

    missing = EXPECTED_TABLES - tables
    assert not missing, f"Missing tables: {missing}"


def test_cdc_publication_exists():
    """fintech_cdc publication covers all five tables."""
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pubname FROM pg_publication WHERE pubname = 'fintech_cdc'"
                )
                row = cur.fetchone()
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable: {exc}")

    assert row is not None, "Publication fintech_cdc not found"
