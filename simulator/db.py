"""
db.py: Postgres writers for the five V1 streams.

Rows come from the generators as plain dicts. Every row is validated against its Pydantic
model before it is written, and the validated dump is what reaches the database, so a
generator bug fails loudly here instead of tripping a CHECK constraint (or worse, not
tripping one).

Two writers share the same API:

  - BackfillWriter buffers rows and writes them with executemany, one transaction per
    flush, in dependency order. Fast; used for long date ranges.
  - StreamingWriter writes each event as it happens. A transaction and the risk events it
    triggered commit together (Chain 5 atomicity), which is what lets the CDC layer see
    them as one causally linked change.

Both are used the same way for each merchant-day:

    writer.write_merchant_day(events, risk_rows, lending)
    writer.write_snapshots(snapshot_rows(merchants, day))

merchants gets one end-of-day snapshot row per merchant per day (INSERT only), as Event
Schemas specifies. merchant_transactions, loan_repayments and risk_events are INSERT-only;
loan_applications is INSERTed at decision and UPDATEd at disbursement.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from typing import Any, Iterable, Optional

import psycopg
from pydantic import BaseModel, ValidationError

from simulator import models
from simulator.common import end_of_day
from simulator.config import settings


@dataclass(frozen=True)
class Stream:
    name: str
    table: str
    model: type[BaseModel]
    key: tuple[str, ...]


STREAMS: dict[str, Stream] = {
    s.name: s
    for s in (
        Stream("merchants", "merchants", models.MerchantSnapshot, ("merchant_id", "snapshot_at")),
        Stream("merchant_transactions", "merchant_transactions", models.MerchantTransaction, ("transaction_id",)),
        Stream("risk_events", "risk_events", models.RiskEvent, ("alert_id",)),
        Stream("loan_applications", "loan_applications", models.LoanApplication, ("application_id",)),
        Stream("loan_repayments", "loan_repayments", models.LoanRepayment, ("repayment_id",)),
    )
}

# Write order inside a flush. loan_repayments.loan_id references loan_applications
# (migration 002), and a disbursement UPDATE needs its application row, so applications
# come first, then their updates, then repayments.
_FLUSH_ORDER = ("merchants", "merchant_transactions", "risk_events", "loan_applications", "updates", "loan_repayments")

# Columns whose Python value is a list and must be sent as a Postgres array.
_ARRAY_COLUMNS = {"decline_reasons": "text[]"}

_UPDATE_SQL = (
    "UPDATE loan_applications SET disbursed_at = %(disbursed_at)s, "
    "disbursement_amount_minor = %(disbursement_amount_minor)s "
    "WHERE application_id = %(application_id)s"
)


class RowValidationError(ValueError):
    """A generated row failed its Pydantic model; nothing was written."""

    def __init__(self, stream: str, row: dict, error: ValidationError):
        key = ", ".join(f"{k}={row.get(k)!r}" for k in STREAMS[stream].key) if stream in STREAMS else ""
        problems = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in error.errors())
        super().__init__(f"{stream} row ({key}) failed validation: {problems}")
        self.stream = stream


def connect(dsn: Optional[str] = None) -> psycopg.Connection:
    """One long-lived connection per process. Not autocommit: writers manage transactions."""
    return psycopg.connect(dsn or settings.dsn, autocommit=False)


@lru_cache(maxsize=None)
def insert_statement(stream_name: str, skip_duplicates: bool = False) -> str:
    stream = STREAMS[stream_name]
    columns = list(stream.model.model_fields)
    values = ", ".join(
        f"%({c})s::{_ARRAY_COLUMNS[c]}" if c in _ARRAY_COLUMNS else f"%({c})s" for c in columns
    )
    conflict = f" ON CONFLICT ({', '.join(stream.key)}) DO NOTHING" if skip_duplicates else ""
    return f"INSERT INTO {stream.table} ({', '.join(columns)}) VALUES ({values}){conflict}"


def validated(stream_name: str, row: dict) -> dict:
    """The row as its Pydantic model dumps it, or RowValidationError."""
    try:
        return STREAMS[stream_name].model(**row).model_dump()
    except ValidationError as error:
        raise RowValidationError(stream_name, row, error) from error


def validated_update(update: dict) -> dict:
    """A disbursement update: application_id, a timezone-aware disbursed_at, an amount."""
    problems = []
    if not update.get("application_id"):
        problems.append("application_id is missing")
    stamp = update.get("disbursed_at")
    if not isinstance(stamp, datetime) or stamp.tzinfo is None:
        problems.append("disbursed_at must be a timezone-aware datetime")
    amount = update.get("disbursement_amount_minor")
    if not isinstance(amount, int) or amount < 0:
        problems.append("disbursement_amount_minor must be a non-negative integer")
    if problems:
        raise ValueError(f"loan_applications update failed validation: {'; '.join(problems)}")
    return {k: update[k] for k in ("application_id", "disbursed_at", "disbursement_amount_minor")}


def snapshot_rows(merchants: Iterable[Any], day: date) -> list[dict]:
    """The end-of-day merchants rows for `day`. Merchants not yet onboarded have none.
    A churned merchant keeps getting rows for churned_snapshot_days after it churns (and
    while it has an active loan), so the dimension stays continuous without growing for
    ever. See Merchant.snapshot_due."""
    stamp = end_of_day(day)
    return [
        m.snapshot(stamp)
        for m in merchants
        if (m.onboarded_at is None or m.onboarded_at <= day) and m.snapshot_due(day)
    ]


def disburse_loan(conn: psycopg.Connection, update: dict) -> None:
    """Apply one disbursement UPDATE and commit. Raises if the application row is missing."""
    row = validated_update(update)
    with conn.transaction():
        cursor = conn.execute(_UPDATE_SQL, row)
        if cursor.rowcount != 1:
            raise LookupError(f"no loan_applications row for application_id={row['application_id']!r}")
    conn.commit()


def _split_lending(lending: dict) -> tuple[list[dict], list[dict], list[dict]]:
    return (
        lending.get("loan_applications", []),
        lending.get("loan_application_updates", []),
        lending.get("loan_repayments", []),
    )


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------

class BackfillWriter:
    """Buffers validated rows and writes them in batches.

    A flush is one transaction: either everything buffered reaches the database or none of
    it does. Flush at the end of every simulated day (end_of_day()); add() also flushes by
    itself when the buffer reaches batch_size rows, so a very busy day is written in several
    transactions.
    """

    def __init__(self, conn: psycopg.Connection, batch_size: int = 2000, skip_duplicates: bool = False):
        self.conn = conn
        self.batch_size = batch_size
        self.skip_duplicates = skip_duplicates
        self._buffers: dict[str, list[dict]] = {name: [] for name in STREAMS}
        self._updates: list[dict] = []
        self.written: Counter = Counter()

    def buffered(self) -> int:
        return sum(len(rows) for rows in self._buffers.values()) + len(self._updates)

    def add(self, stream: str, rows: Iterable[dict]) -> None:
        """Validate every row first, so one bad row leaves the buffer as it was."""
        checked = [validated(stream, row) for row in rows]
        self._buffers[stream].extend(checked)
        if self.buffered() >= self.batch_size:
            self.flush()

    def add_updates(self, updates: Iterable[dict]) -> None:
        checked = [validated_update(u) for u in updates]
        self._updates.extend(checked)
        if self.buffered() >= self.batch_size:
            self.flush()

    def write_merchant_day(self, events: list[dict], risk_rows: list[dict], lending: dict) -> None:
        applications, updates, repayments = _split_lending(lending)
        self.add("merchant_transactions", events)
        self.add("risk_events", risk_rows)
        self.add("loan_applications", applications)
        self.add_updates(updates)
        self.add("loan_repayments", repayments)

    def write_snapshots(self, rows: list[dict]) -> None:
        self.add("merchants", rows)

    def flush(self) -> None:
        if not self.buffered():
            return
        counts: Counter = Counter()
        with self.conn.transaction():
            with self.conn.cursor() as cursor:
                for name in _FLUSH_ORDER:
                    if name == "updates":
                        if self._updates:
                            cursor.executemany(_UPDATE_SQL, self._updates)
                            if cursor.rowcount != len(self._updates):
                                raise LookupError(
                                    f"{len(self._updates) - cursor.rowcount} disbursement updates matched no application row"
                                )
                            counts["loan_application_updates"] += len(self._updates)
                    elif self._buffers[name]:
                        cursor.executemany(insert_statement(name, self.skip_duplicates), self._buffers[name])
                        counts[name] += len(self._buffers[name])
        self.conn.commit()
        self.written.update(counts)
        self._buffers = {name: [] for name in STREAMS}
        self._updates = []

    def end_of_day(self) -> None:
        self.flush()

    def close(self) -> None:
        self.flush()


# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------

class StreamingWriter:
    """Writes each event as it happens.

    A transaction and the risk events it triggered are inserted in one transaction, so a
    failure rolls both back and a reader of the WAL sees them together. Risk events that
    point at the merchant, or at a transaction not in this call, commit on their own.
    """

    def __init__(self, conn: psycopg.Connection, skip_duplicates: bool = False):
        self.conn = conn
        self.skip_duplicates = skip_duplicates
        self.written: Counter = Counter()

    def _insert(self, stream: str, row: dict) -> None:
        self.conn.execute(insert_statement(stream, self.skip_duplicates), row)
        self.written[stream] += 1

    def write(self, stream: str, rows: Iterable[dict]) -> None:
        """One transaction per row."""
        for row in rows:
            row = validated(stream, row)
            with self.conn.transaction():
                self._insert(stream, row)
            self.conn.commit()

    def write_events(self, events: list[dict], risk_rows: list[dict]) -> None:
        by_entity: dict[str, list[dict]] = {}
        for risk_row in risk_rows:
            by_entity.setdefault(risk_row["entity_id"], []).append(validated("risk_events", risk_row))

        for event in events:
            row = validated("merchant_transactions", event)
            with self.conn.transaction():
                self._insert("merchant_transactions", row)
                for risk_row in by_entity.pop(row["transaction_id"], []):
                    self._insert("risk_events", risk_row)
            self.conn.commit()

        leftovers = [r for entity, rows in by_entity.items() for r in rows]
        for risk_row in leftovers:
            with self.conn.transaction():
                self._insert("risk_events", risk_row)
            self.conn.commit()

    def disburse(self, update: dict) -> None:
        """One disbursement UPDATE, committed on its own."""
        disburse_loan(self.conn, update)
        self.written["loan_application_updates"] += 1

    def write_merchant_day(self, events: list[dict], risk_rows: list[dict], lending: dict) -> None:
        applications, updates, repayments = _split_lending(lending)
        self.write_events(events, risk_rows)
        self.write("loan_applications", applications)
        for update in updates:
            self.disburse(update)
        self.write("loan_repayments", repayments)

    def write_snapshots(self, rows: list[dict]) -> None:
        self.write("merchants", rows)

    def end_of_day(self) -> None:
        pass

    def close(self) -> None:
        pass
