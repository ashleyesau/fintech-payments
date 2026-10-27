"""tests/test_cdc_foundation.py: M1, the CDC foundation.

Migration 003, the settings, the D9 skip and fail rules, and a slot smoke test: a slot
created before any data returns well-formed pgoutput messages for a real backfill, and an
update on loan_applications carries the full old row.

Nothing here decodes messages. It looks at the message type bytes and, for updates, the
old-row marker; the decoder is M2. The slot tests use the throwaway database from
`cdc_dsn` and need wal_level=logical (see tests/conftest.py and decision D9).
"""
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import psycopg
import pytest

from cdc.config import CdcSettings
from simulator import db
from simulator.common import SAST
from simulator.runner import run_backfill
from tests.conftest import TABLES
from tests.helpers import START, peek_changes, require_cdc_postgres, settings_for, split_update

ROOT = Path(__file__).parent.parent


# ---------------------------------------------------------------------------
# Migration 003
# ---------------------------------------------------------------------------

def identities(conn) -> dict[str, str]:
    rows = conn.execute(
        "SELECT relname, relreplident FROM pg_class "
        "WHERE relkind = 'r' AND relnamespace = 'public'::regnamespace"
    )
    return dict(rows.fetchall())


def test_only_loan_applications_has_full_replica_identity(conn):
    found = identities(conn)
    assert found["loan_applications"] == "f"
    assert {t: found[t] for t in TABLES if t != "loan_applications"} == {
        t: "d" for t in TABLES if t != "loan_applications"
    }


def test_migration_003_can_be_applied_twice(conn):
    sql = next((ROOT / "migrations").glob("003_*.sql")).read_text()
    conn.execute(sql)
    conn.execute(sql)
    assert identities(conn)["loan_applications"] == "f"


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

def env_example() -> dict[str, str]:
    pairs = (line.split("=", 1) for line in (ROOT / ".env.example").read_text().splitlines()
             if line.startswith("CDC_"))
    return {key: value for key, value in pairs}


def test_env_example_lists_every_cdc_setting_with_its_default():
    fields = {f"CDC_{name.upper()}": field for name, field in CdcSettings.model_fields.items()}
    listed = env_example()
    assert set(listed) == set(fields)
    for key, field in fields.items():
        assert field.annotation(listed[key]) == field.default, key


def test_the_defaults_name_the_first_run(monkeypatch):
    for key in env_example():
        monkeypatch.delenv(key, raising=False)
    s = CdcSettings(_env_file=None)
    assert (s.db, s.slot_name, s.publication, s.gcs_bucket) == ("fintech_cdc", "cdc_r01", "fintech_cdc", "cdc_landing")
    assert s.gcs_prefix == "cdc/cdc_r01/"
    assert s.dsn == "host=localhost port=5432 dbname=fintech_cdc user=sim password=sim"


def test_a_new_slot_name_gives_a_new_prefix_and_the_consumer_has_its_own_connection(monkeypatch):
    monkeypatch.setenv("CDC_SLOT_NAME", "cdc_r02")
    monkeypatch.setenv("CDC_USER", "cdc_reader")
    s = CdcSettings(_env_file=None)
    assert s.gcs_prefix == "cdc/cdc_r02/"
    assert "user=cdc_reader" in s.dsn


# ---------------------------------------------------------------------------
# D9: when the CDC tests skip and when they fail
# ---------------------------------------------------------------------------

class FakeAdmin:
    def __init__(self, wal_level):
        self.wal_level, self.closed = wal_level, False

    def execute(self, sql):
        assert sql == "SHOW wal_level"
        return self

    def fetchone(self):
        return (self.wal_level,)

    def close(self):
        self.closed = True


def unreachable():
    raise psycopg.OperationalError("connection refused")


def test_unreachable_postgres_skips_locally():
    with pytest.raises(pytest.skip.Exception):
        require_cdc_postgres(unreachable, ci=False)


def test_unreachable_postgres_fails_under_ci():
    with pytest.raises(pytest.fail.Exception, match="CI is set"):
        require_cdc_postgres(unreachable, ci=True)


@pytest.mark.parametrize("ci", [False, True])
def test_a_reachable_postgres_that_is_not_logical_fails_even_locally(ci):
    admin = FakeAdmin("replica")
    with pytest.raises(pytest.fail.Exception, match="wal_level is 'replica'"):
        require_cdc_postgres(lambda: admin, ci=ci)
    assert admin.closed


def test_a_logical_postgres_is_returned():
    admin = FakeAdmin("logical")
    assert require_cdc_postgres(lambda: admin, ci=True) is admin


# ---------------------------------------------------------------------------
# Slot smoke test
# ---------------------------------------------------------------------------

def table_names(connection) -> dict[int, str]:
    rows = connection.execute("SELECT oid::int, relname FROM pg_class WHERE relnamespace = 'public'::regnamespace")
    return dict(rows.fetchall())


def test_a_slot_created_before_any_data_has_nothing_to_return(cdc_dsn, cdc_slot):
    with psycopg.connect(cdc_dsn, autocommit=True) as reader:
        assert peek_changes(reader, cdc_slot) == []


@pytest.mark.slow
def test_a_short_backfill_makes_the_slot_return_every_row_as_an_insert(cdc_dsn, cdc_slot, tmp_path):
    writer = db.connect(cdc_dsn)
    summary = run_backfill(settings_for(tmp_path, days=45, count=100), writer)
    written = summary.rows_written
    writer.close()
    assert written["merchants"] > 0 and written["merchant_transactions"] > 0

    with psycopg.connect(cdc_dsn, autocommit=True) as reader:
        names = table_names(reader)
        messages = peek_changes(reader, cdc_slot)

    kinds = Counter(m[:1] for _, _, m in messages)
    assert kinds[b"D"] == 0 and kinds[b"T"] == 0, "V1 writes no deletes or truncates"
    assert kinds[b"U"] == written["loan_application_updates"]
    assert kinds[b"B"] == kinds[b"C"] > 0

    inserts = Counter(names[int.from_bytes(m[1:5], "big")] for _, _, m in messages if m[:1] == b"I")
    assert inserts == Counter({t: n for t, n in written.items() if t in TABLES and n})

    first_seen = set()
    for _, _, message in messages:
        if message[:1] == b"I":
            assert int.from_bytes(message[1:5], "big") in first_seen, "a relation message precedes the first insert"
        elif message[:1] == b"R":
            first_seen.add(int.from_bytes(message[1:5], "big"))


def application(application_id: str) -> dict:
    return {
        "application_id": application_id, "merchant_id": "m-1", "applicant_user_id": "user-1",
        "business_id": "biz-1", "application_channel": "web",
        "submission_ts": datetime(2025, 3, 1, 8, 15, tzinfo=SAST),
        "decision_ts": datetime(2025, 3, 1, 8, 16, tzinfo=SAST), "decision": "approved",
        "requested_amount": 600_000, "requested_term_months": 6,
        "approved_amount": 600_000, "approved_term_months": 6, "total_cost_percent": 0.07,
    }


def disbursement(application_id: str) -> dict:
    return {"application_id": application_id, "disbursed_at": datetime(2025, 3, 3, 9, 30, tzinfo=SAST),
            "disbursement_amount_minor": 600_000}


def updates_in(reader, slot):
    return [split_update(m) for _, _, m in peek_changes(reader, slot) if m[:1] == b"U"]


def test_a_disbursement_carries_the_full_old_row_and_a_replay_carries_an_identical_pair(cdc_dsn, cdc_slot):
    writer = db.connect(cdc_dsn)
    batch = db.BackfillWriter(writer)
    batch.add("loan_applications", [application("app-1")])
    batch.end_of_day()
    db.disburse_loan(writer, disbursement("app-1"))
    db.disburse_loan(writer, disbursement("app-1"))
    writer.close()

    with psycopg.connect(cdc_dsn, autocommit=True) as reader:
        first, replay = updates_in(reader, cdc_slot)

    assert first[0] == b"O" and first[1] != first[2], "a real disbursement changes the row"
    assert replay[0] == b"O" and replay[1] == replay[2], "a replayed disbursement is old == new"
    assert replay[2] == first[2]


def test_without_full_identity_an_update_has_no_old_row(cdc_dsn, cdc_slot):
    """Negative control: the old-row check above can fail."""
    with psycopg.connect(cdc_dsn, autocommit=True) as control:
        control.execute("ALTER TABLE loan_applications REPLICA IDENTITY DEFAULT")
        try:
            writer = db.connect(cdc_dsn)
            batch = db.BackfillWriter(writer)
            batch.add("loan_applications", [application("app-2")])
            batch.end_of_day()
            db.disburse_loan(writer, disbursement("app-2"))
            writer.close()
            (kind, old, new), = updates_in(control, cdc_slot)
        finally:
            control.execute("ALTER TABLE loan_applications REPLICA IDENTITY FULL")
    assert kind is None and old is None and new
