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


@pytest.fixture(scope="session")
def uninterrupted(dsn, tmp_path_factory):
    """Every table after one uninterrupted LONG_RUN, written once for all the resume and
    streaming tests. Application propensity is boosted so the run reaches lending."""
    from datetime import timedelta

    from simulator import lending
    from simulator.runner import RunSettings, run_backfill
    from tests.helpers import LONG_RUN, START, reset, table_contents

    with pytest.MonkeyPatch.context() as patch:
        for state in ("growing", "stable"):
            patch.setitem(lending._PROPENSITY, state, lending._PROPENSITY[state] * 10)
        connection = db.connect(dsn)
        reset(connection)
        path = tmp_path_factory.mktemp("uninterrupted") / "checkpoint.json"
        run = RunSettings(START, START + timedelta(days=LONG_RUN["days"]), LONG_RUN["count"],
                          checkpoint_path=str(path))
        run_backfill(run, connection)
        contents = table_contents(connection)
        reset(connection)
        connection.close()
    assert len(contents["loan_applications"]) > 0 and len(contents["loan_repayments"]) > 0
    return contents


@pytest.fixture(scope="session")
def validated(dsn, tmp_path_factory):
    """A 13-month backfill in its own throwaway database, with validate.sql's temp tables
    built on a connection that stays open for the session. Used by test_reconcile.py.

    Application propensity is raised tenfold, as in `uninterrupted`, so a 100-merchant run
    originates enough loans for the lending checks to have data; it does not touch
    repayment behaviour. The run ends 2026-02-01 so December and January are both in it.
    Returns (connection, final merchants).
    """
    from datetime import date

    from simulator import lending, validation
    from simulator.runner import RunSettings, run_backfill
    from simulator.state import load_checkpoint

    name = f"fintech_val_{os.getpid()}"
    admin = psycopg.connect(_dsn(settings.postgres_db), autocommit=True)
    admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    admin.execute(f'CREATE DATABASE "{name}"')
    try:
        with psycopg.connect(_dsn(name), autocommit=True) as setup:
            for path in MIGRATIONS:
                setup.execute(path.read_text())
        checkpoint = str(tmp_path_factory.mktemp("validated") / "checkpoint.json")
        with pytest.MonkeyPatch.context() as patch:
            for state in ("growing", "stable"):
                patch.setitem(lending._PROPENSITY, state, lending._PROPENSITY[state] * 10)
            writer = db.connect(_dsn(name))
            run_backfill(RunSettings(date(2025, 1, 1), date(2026, 2, 1), 100, seed=42,
                                     checkpoint_path=checkpoint), writer)
            writer.close()
        reader = db.connect(_dsn(name))
        validation.run_setup(reader)
        yield reader, load_checkpoint(checkpoint).merchants
        reader.close()
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()
