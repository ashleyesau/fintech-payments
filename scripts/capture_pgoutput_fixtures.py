"""
capture_pgoutput_fixtures.py: capture raw pgoutput messages from a real slot into
tests/fixtures/pgoutput.json.

The decoder tests (tests/test_cdc_decoder.py) run against these bytes, so most of the CDC
suite needs no database. Run this again only if the fixtures need to change; the output is
committed. It needs a Postgres with wal_level=logical and creates, then drops, a scratch
database and slots of its own. Nothing here touches `fintech` or `fintech_cdc`.

    PYTHONPATH=. python scripts/capture_pgoutput_fixtures.py

Each scenario plants rows with values written out here by hand, as the JSON the contract
specifies. Those `expected` values do not come from the decoder, so a test comparing the
two checks the decoder. The same transactions are read under several reader TimeZones to
exercise the offsets in timestamptz text.
"""
from __future__ import annotations

import json
import os
import random
from pathlib import Path

import psycopg

from simulator.config import settings

ROOT = Path(__file__).parent.parent
OUT = ROOT / "tests" / "fixtures" / "pgoutput.json"
MIGRATIONS = sorted((ROOT / "migrations").glob("*.sql"))


def dsn(dbname: str) -> str:
    return (f"host={settings.postgres_host} port={settings.postgres_port} dbname={dbname} "
            f"user={settings.postgres_user} password={settings.postgres_password}")


# ---------------------------------------------------------------------------
# Rows: what is written (SQL values) and what the contract says comes out (JSON values)
# ---------------------------------------------------------------------------

MERCHANT = {
    "merchant_id": "m-001", "archetype": "spaza_shop", "province": "gauteng", "is_metro": True,
    "onboarded_at": "2025-01-13 22:00:00+00", "first_transaction_at": None,
    "merchant_state": "new", "risk_score": 14, "loan_count": 0, "has_active_loan": False,
    "snapshot_at": "2025-03-01 21:59:00+00",
}
MERCHANT_OUT = {**MERCHANT, "onboarded_at": "2025-01-13T22:00:00.000000Z",
                "snapshot_at": "2025-03-01T21:59:00.000000Z"}

TRANSACTION = {
    "transaction_id": "txn-001", "merchant_id": "m-001", "event_type": "payment",
    "channel": "card_present", "status": "completed", "entry_mode": "contactless",
    "payment_method": "visa", "bank_selected": None, "amount_minor": 12550, "currency": "ZAR",
    "interchange_fee_minor": 63, "scheme_fee_minor": 9, "acquirer_markup_minor": 120,
    "mcc": "5411", "auth_code": "", "decline_code": None, "risk_score": 12, "risk_action": "allow",
    "created_at": "2025-03-01 22:30:12.345678+00", "bank_selected_at": None,
    "login_succeeded_at": None, "otp_requested_at": None, "otp_entered_at": None,
    "authorized_at": "2025-03-01 22:30:14+00", "completed_at": "2025-03-01 22:30:15.5+00",
    "refunded_at": None, "settlement_date": "2025-03-03",
    "batch_id": 'batch-\u00e9 "q" \\ , caf\u00e9 \u4e2d', "device_id": "POS-1", "pos_version": "v3.1.0",
    "is_test": False, "correlation_id": "corr-1",
}
TRANSACTION_OUT = {**TRANSACTION, "created_at": "2025-03-01T22:30:12.345678Z",
                   "authorized_at": "2025-03-01T22:30:14.000000Z",
                   "completed_at": "2025-03-01T22:30:15.500000Z"}

RISK = {
    "alert_id": "alert-1", "entity_type": "transaction", "entity_id": "txn-001",
    "merchant_id": "m-001", "rule_id": "rule-large-txn-flag", "rule_type": "large_txn_flag",
    "risk_score": 19, "risk_score_delta": -5, "action": "flag_review", "action_by": "system_auto",
    "action_ts": "2025-03-01 22:30:12.345678+00", "resolved_ts": None, "outcome": None,
}
RISK_OUT = {**RISK, "action_ts": "2025-03-01T22:30:12.345678Z"}


def application(application_id: str, **overrides) -> dict:
    row = {
        "application_id": application_id, "merchant_id": "m-001", "applicant_user_id": "user-1",
        "business_id": "biz-1", "application_channel": "web",
        "submission_ts": "2025-03-01 08:15:00+00", "decision_ts": "2025-03-01 08:15:42+00",
        "business_registration": "2019/123456/07", "months_trading": 38,
        "declared_monthly_revenue": 4500000, "requested_amount": 6000000,
        "requested_term_months": 6, "owner_credit_score": 612, "owner_bureau": "transunion",
        "actioned_by": "automated_engine", "decision": "approved", "decline_reasons": [],
        "approved_amount": 6000000, "approved_term_months": 6, "total_cost_percent": "0.0700",
        "disbursed_at": None, "disbursement_amount_minor": None, "model_version_id": "risk-v1",
        "fica_status": "pass",
    }
    row.update(overrides)
    return row


def application_out(row: dict) -> dict:
    out = dict(row)
    for key in ("submission_ts", "decision_ts", "disbursed_at"):
        if out[key] is not None:
            out[key] = out[key].replace(" ", "T").replace("+00", ".000000Z")
    return out


TRICKY_ARRAY = ["with, comma", 'with "quote"', None, "NULL", "back\\slash", " lead space", ""]
APPLICATIONS = [
    application("app-1"),
    application("app-2", decision="declined", requested_amount=9_007_199_254_740_991,
                decline_reasons=["high_risk_score", "low_volume"], approved_amount=None,
                approved_term_months=None, total_cost_percent="1.0000"),
    application("app-3", decline_reasons=TRICKY_ARRAY, total_cost_percent="0.0000"),
    application("app-4", decline_reasons=None, total_cost_percent=None, decision_ts=None,
                business_registration=None, months_trading=None, declared_monthly_revenue=None,
                owner_credit_score=None, owner_bureau=None, actioned_by=None, decision=None,
                approved_amount=None, approved_term_months=None, model_version_id=None,
                fica_status=None),
]

REPAYMENT = {
    "repayment_id": "rep-1", "loan_id": "app-1", "merchant_id": "m-001", "installment_number": 1,
    "due_date": "2025-04-02", "payment_ts": None, "principal_due": 1000000, "interest_due": 70000,
    "total_due": 1070000, "amount_paid": 0, "outstanding_before": 6420000,
    "outstanding_after": 6420000, "debit_method": "debicheck", "mandate_id": "mnd-1",
    "mandate_status": "ACCP", "collection_status": "unpaid_nsf", "response_code": "AC04",
    "retry_attempt": 0, "days_past_due": 0, "par_bucket": "current", "is_cured": False,
    "write_off_flag": True,
}


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def insert(conn, table: str, row: dict) -> None:
    columns = ", ".join(row)
    values = ", ".join(f"%({c})s::text[]" if c == "decline_reasons" else f"%({c})s" for c in row)
    conn.execute(f"INSERT INTO {table} ({columns}) VALUES ({values})", row)


def peek(conn, slot: str, publication: str, timezone: str = "UTC") -> list[dict]:
    conn.execute(f"SET TimeZone = '{timezone}'")
    rows = conn.execute(
        "SELECT lsn::text, xid::text::bigint, data FROM pg_logical_slot_peek_binary_changes"
        "(%s, NULL, NULL, 'proto_version', '1', 'publication_names', %s)", (slot, publication)).fetchall()
    return [{"lsn": lsn, "xid": xid, "hex": bytes(data).hex()} for lsn, xid, data in rows]


def skip_all(conn, slot: str, publication: str) -> None:
    """Confirm everything waiting, so the next peek starts clean (and repeats its relation messages)."""
    last = conn.execute(
        "SELECT lsn::text FROM pg_logical_slot_peek_binary_changes"
        "(%s, NULL, NULL, 'proto_version', '1', 'publication_names', %s) ORDER BY lsn DESC LIMIT 1",
        (slot, publication)).fetchone()
    if last:
        conn.execute("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (slot, last[0]))


class Scenario:
    def __init__(self, conn, name: str, publication: str = "fintech_cdc"):
        self.conn, self.slot, self.publication = conn, f"fx_{name}"[:60], publication

    def __enter__(self):
        # Empty the tables before the slot exists: a slot publishes TRUNCATE too.
        if self.publication == "fintech_cdc":
            self.conn.execute("TRUNCATE loan_repayments, risk_events, loan_applications, merchant_transactions, merchants")
        self.conn.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (self.slot,))
        return self

    def __exit__(self, *exc):
        self.conn.execute("SELECT pg_drop_replication_slot(%s)", (self.slot,))


def op(kind, table, new=None, old=None, old_kind=None):
    entry = {"op": kind, "table": table}
    if kind == "T":
        entry["tables"] = table
        del entry["table"]
        return entry
    if new is not None:
        entry["new"] = new
    if kind in ("U", "D"):
        entry["old_kind"], entry["old"] = old_kind, old
    return entry


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def capture_inserts(conn, timezones: dict[str, str]) -> dict:
    """Every table, every column type, NULLs, tricky arrays, two transactions."""
    expected = [
        op("I", "merchants", MERCHANT_OUT),
        op("I", "merchant_transactions", TRANSACTION_OUT),
        op("I", "risk_events", RISK_OUT),
        *[op("I", "loan_applications", application_out(a)) for a in APPLICATIONS],
        op("I", "loan_repayments", {**REPAYMENT}),
    ]
    out = {}
    with Scenario(conn, "inserts") as s:
        with conn.transaction():
            insert(conn, "merchants", MERCHANT)
            insert(conn, "merchant_transactions", TRANSACTION)
            insert(conn, "risk_events", RISK)
        with conn.transaction():
            for a in APPLICATIONS:
                insert(conn, "loan_applications", a)
            insert(conn, "loan_repayments", REPAYMENT)
        for label, tz in timezones.items():
            out[f"inserts_{label}"] = {
                "description": f"Two transactions inserting every table, read with TimeZone {tz}.",
                "reader_timezone": tz,
                **({"expected": expected} if label == "utc" else {"expected_same_as": "inserts_utc"}),
                "messages": peek(conn, s.slot, s.publication, tz),
            }
    return out


def capture_disbursement(conn) -> dict:
    inserted = application("app-1")
    after = {**inserted, "disbursed_at": "2025-03-03 09:30:00+00", "disbursement_amount_minor": 6000000}
    update = ("UPDATE loan_applications SET disbursed_at = '2025-03-03 09:30:00+00', "
              "disbursement_amount_minor = 6000000 WHERE application_id = 'app-1'")
    with Scenario(conn, "disbursement") as s:
        insert(conn, "loan_applications", inserted)
        conn.execute(update)
        conn.execute(update)  # a replayed disbursement: a second, identical update
        return {"disbursement": {
            "description": "An insert, a real disbursement update and a replayed one (REPLICA IDENTITY FULL).",
            "reader_timezone": "UTC",
            "expected": [
                op("I", "loan_applications", application_out(inserted)),
                op("U", "loan_applications", application_out(after), application_out(inserted), "O"),
                op("U", "loan_applications", application_out(after), application_out(after), "O"),
            ],
            "messages": peek(conn, s.slot, s.publication)}}


def capture_default_identity_updates(conn) -> dict:
    blank = {k: None for k in MERCHANT_OUT}
    renamed = {**MERCHANT, "merchant_id": "m-002"}
    with Scenario(conn, "default_updates") as s:
        insert(conn, "merchants", MERCHANT)
        conn.execute("UPDATE merchants SET risk_score = 15 WHERE merchant_id = 'm-001'")
        conn.execute("UPDATE merchants SET merchant_id = 'm-002' WHERE merchant_id = 'm-001'")
        after_score = {**MERCHANT_OUT, "risk_score": 15}
        return {"default_identity_updates": {
            "description": "Updates on a default-identity table: no old row, then a key change (old row of kind K).",
            "reader_timezone": "UTC",
            "expected": [
                op("I", "merchants", MERCHANT_OUT),
                op("U", "merchants", after_score, None, None),
                op("U", "merchants", {**after_score, "merchant_id": "m-002"},
                   {**blank, "merchant_id": "m-001", "snapshot_at": MERCHANT_OUT["snapshot_at"]}, "K"),
            ],
            "messages": peek(conn, s.slot, s.publication)}}


def capture_delete_and_truncate(conn) -> dict:
    blank = {k: None for k in RISK_OUT}
    with Scenario(conn, "delete_truncate") as s:
        insert(conn, "risk_events", RISK)
        conn.execute("DELETE FROM risk_events WHERE alert_id = 'alert-1'")
        conn.execute("TRUNCATE risk_events")
        return {"delete_and_truncate": {
            "description": "An insert, a delete (key-only old row) and a truncate. V1 produces neither of the last two.",
            "reader_timezone": "UTC",
            "expected": [
                op("I", "risk_events", RISK_OUT),
                op("D", "risk_events", None, {**blank, "alert_id": "alert-1"}, "K"),
                op("T", ["risk_events"]),
            ],
            "messages": peek(conn, s.slot, s.publication)}}


def capture_unknown_type(conn) -> dict:
    conn.execute("CREATE TABLE scratch_uuid (id int PRIMARY KEY, u uuid)")
    conn.execute("CREATE PUBLICATION scratch_uuid FOR TABLE scratch_uuid")
    with Scenario(conn, "unknown_type", "scratch_uuid") as s:
        conn.execute("INSERT INTO scratch_uuid VALUES (1, '123e4567-e89b-12d3-a456-426614174000')")
        return {"unknown_type": {
            "description": "A relation with a uuid column (type OID 2950), which the contract does not map.",
            "reader_timezone": "UTC", "error": "UnknownTypeError",
            "messages": peek(conn, s.slot, s.publication)}}


def capture_unchanged_toast(conn) -> dict:
    conn.execute("CREATE TABLE scratch_toast (id int PRIMARY KEY, big text, n int)")
    conn.execute("ALTER TABLE scratch_toast ALTER COLUMN big SET STORAGE EXTERNAL")
    conn.execute("CREATE PUBLICATION scratch_toast FOR TABLE scratch_toast")
    big = "".join(random.Random(1).choice("0123456789abcdef") for _ in range(6000))
    with Scenario(conn, "unchanged_toast", "scratch_toast") as s:
        conn.execute("INSERT INTO scratch_toast VALUES (1, %s, 1)", (big,))
        skip_all(conn, s.slot, s.publication)  # keep the 6 KB insert out of the fixture
        conn.execute("UPDATE scratch_toast SET n = 2 WHERE id = 1")
        messages = peek(conn, s.slot, s.publication)
        return {"unchanged_toast": {
            "description": "An update that leaves an out-of-line (TOAST) column unchanged: the new tuple carries the 'u' marker.",
            "reader_timezone": "UTC", "error": "DecodeError", "messages": messages}}


def main() -> None:
    name = f"fx_capture_{os.getpid()}"
    admin = psycopg.connect(dsn(settings.postgres_db), autocommit=True)
    level = admin.execute("SHOW wal_level").fetchone()[0]
    if level != "logical":
        raise SystemExit(f"wal_level is {level!r}; start Postgres with wal_level=logical")
    version = admin.execute("SHOW server_version").fetchone()[0]
    admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    admin.execute(f'CREATE DATABASE "{name}"')
    try:
        with psycopg.connect(dsn(name), autocommit=True) as conn:
            for path in MIGRATIONS:
                conn.execute(path.read_text())
            scenarios = {}
            scenarios.update(capture_inserts(conn, {
                "utc": "UTC", "johannesburg": "Africa/Johannesburg",
                "kolkata": "Asia/Kolkata", "st_johns": "America/St_Johns"}))
            scenarios.update(capture_disbursement(conn))
            scenarios.update(capture_default_identity_updates(conn))
            scenarios.update(capture_delete_and_truncate(conn))
            scenarios.update(capture_unknown_type(conn))
            scenarios.update(capture_unchanged_toast(conn))
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    lines = ["{", f'"postgres_version": {json.dumps(version)},', '"scenarios": {']
    for i, (key, scenario) in enumerate(scenarios.items()):
        messages = scenario.pop("messages")
        head = json.dumps(scenario, ensure_ascii=False)[:-1]
        body = ",\n".join("  " + json.dumps(m) for m in messages)
        lines.append(f'{json.dumps(key)}: {head}, "messages": [\n{body}\n]' + "}" + ("," if i < len(scenarios) - 1 else ""))
    lines += ["}", "}"]
    OUT.write_text("\n".join(lines) + "\n")
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes, Postgres {version})")


if __name__ == "__main__":
    main()
