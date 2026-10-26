"""Shared fixtures: a throwaway Postgres database built from the migrations.

Each test session gets its own database (fintech_test_<pid>), so nothing touches the
fintech database. Tests that need it skip when Postgres is not reachable.
"""
import os
from pathlib import Path

import psycopg
import pytest

from simulator import db
from simulator.config import settings

MIGRATIONS = sorted((Path(__file__).parent.parent / "migrations").glob("*.sql"))
TABLES = ("loan_repayments", "risk_events", "loan_applications", "merchant_transactions", "merchants")


def _dsn(dbname: str) -> str:
    return (
        f"host={settings.postgres_host} port={settings.postgres_port} dbname={dbname} "
        f"user={settings.postgres_user} password={settings.postgres_password}"
    )


@pytest.fixture(scope="session")
def dsn():
    name = f"fintech_test_{os.getpid()}"
    try:
        admin = psycopg.connect(_dsn(settings.postgres_db), autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable: {exc}")
    admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    admin.execute(f'CREATE DATABASE "{name}"')
    try:
        with psycopg.connect(_dsn(name), autocommit=True) as conn:
            for path in MIGRATIONS:
                conn.execute(path.read_text())
        yield _dsn(name)
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()


@pytest.fixture
def conn(dsn):
    """A connection to the throwaway database with every table emptied first."""
    connection = db.connect(dsn)
    for table in TABLES:
        connection.execute(f"TRUNCATE {table} CASCADE")
    connection.commit()
    yield connection
    connection.rollback()
    connection.close()
