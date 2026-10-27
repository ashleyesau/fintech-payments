"""
envelope.py: the change event, as specified by the CDC Change Event Contract (v1).

`Envelope` is the contract as code: every field, in the contract's order, validated on
construction, so an event that breaks the contract cannot be built. `build_events` turns one
decoded Transaction into its events; `to_line` serialises one as an NDJSON line.

The decoder has already mapped column values to their JSON form (contract section 4), so this
module wraps rows and adds the three things the rows do not carry: where the change came from
(source and transaction), when the row happened (event_ts and event_date, section 6) and
when the consumer saw it (ts_ms and ingested_at).
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from cdc.errors import DecodeError
from cdc.pgoutput import Insert
from cdc.transactions import Transaction
from cdc.types import SAFE_INT, format_utc

Table = Literal["merchants", "merchant_transactions", "loan_applications", "loan_repayments", "risk_events"]
Flag = Literal["noop_update", "event_time_fallback"]

UTC_TIMESTAMP = r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$"
ISO_DATE = r"^\d{4}-\d\d-\d\d$"

# Each table's own event time (contract section 6). A disbursement update takes the insert's
# column from its after-image, so a row never changes partition.
EVENT_TIME_COLUMN = {
    "merchants": "snapshot_at",
    "merchant_transactions": "created_at",
    "loan_applications": "submission_ts",
    "loan_repayments": "payment_ts",
    "risk_events": "action_ts",
}

# South African time is UTC+2 with no daylight saving. event_date is the SAST date.
_SAST = timedelta(hours=2)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MILLISECOND = timedelta(milliseconds=1)


class Source(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    db: str
    schema_: Literal["public"] = Field(alias="schema")
    table: Table
    commit_lsn: int = Field(ge=0, le=SAFE_INT)
    change_lsn: int = Field(ge=0, le=SAFE_INT)
    commit_ts: str = Field(pattern=UTC_TIMESTAMP)


class TransactionRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    id: int = Field(ge=0)
    position: int = Field(ge=0)
    size: int = Field(ge=1)

    @model_validator(mode="after")
    def position_is_inside_the_transaction(self):
        if self.position >= self.size:
            raise ValueError(f"position {self.position} is not inside a transaction of {self.size} changes")
        return self


class Envelope(BaseModel):
    """One change event. Field order is the contract's order and is the JSON key order."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    envelope_version: Literal[1] = 1
    op: Literal["c", "u"]
    source: Source
    transaction: TransactionRef
    event_ts: str = Field(pattern=UTC_TIMESTAMP)
    event_date: str = Field(pattern=ISO_DATE)
    ts_ms: int = Field(ge=0)
    ingested_at: str = Field(pattern=UTC_TIMESTAMP)
    flags: list[Flag]
    before: Optional[dict[str, Any]]
    after: dict[str, Any]

    @model_validator(mode="after")
    def before_belongs_to_updates(self):
        if (self.op == "u") != (self.before is not None):
            raise ValueError("an update has a before row and an insert has none")
        return self


def sast_date(utc_timestamp: str) -> str:
    """The South African date of a contract timestamp: '2025-03-01T22:30:12.345678Z' is 2025-03-02."""
    moment = datetime(int(utc_timestamp[0:4]), int(utc_timestamp[5:7]), int(utc_timestamp[8:10]),
                      int(utc_timestamp[11:13])) + _SAST
    return f"{moment.year:04d}-{moment.month:02d}-{moment.day:02d}"


def event_time(table: str, row: dict) -> tuple[str, str, list[str]]:
    """(event_ts, event_date, flags) for a row of `table`, from the row's own event time.

    loan_repayments.payment_ts is NULL on an unpaid attempt. The date then comes from due_date,
    event_ts is due_date at 00:00 SAST, and the event is flagged event_time_fallback.
    """
    column = EVENT_TIME_COLUMN[table]
    value = row[column]
    if value is not None:
        return value, sast_date(value), []
    if table == "loan_repayments" and row.get("due_date") is not None:
        due = row["due_date"]
        eve = (date.fromisoformat(due) - timedelta(days=1)).isoformat()
        return f"{eve}T22:00:00.000000Z", due, ["event_time_fallback"]
    raise DecodeError(f"{table}.{column} is NULL, so the row has no event time")


def build_events(transaction: Transaction, database: str, now: Optional[datetime] = None) -> list[Envelope]:
    """The events of one transaction, in commit order, with their position and the size.

    `now` is the consumer's clock for ts_ms and ingested_at (both from the same millisecond);
    it is read once per transaction. An empty transaction gives no events.
    """
    size = len(transaction.changes)
    if size == 0:
        return []
    now = now or datetime.now(timezone.utc)
    ts_ms = (now - _EPOCH) // _MILLISECOND
    ingested_at = format_utc((_EPOCH + timedelta(milliseconds=ts_ms)).replace(tzinfo=None))
    commit_ts = format_utc(transaction.commit_ts.replace(tzinfo=None))

    events = []
    for position, (change_lsn, message) in enumerate(transaction.changes):
        table = message.relation.name
        if isinstance(message, Insert):
            op, before = "c", None
        else:
            op, before = "u", message.old
        event_ts, event_date, flags = event_time(table, message.new)
        if before is not None and before == message.new:
            flags = ["noop_update", *flags]
        events.append(Envelope(
            op=op,
            source=Source(db=database, schema=message.relation.schema, table=table,
                          commit_lsn=transaction.commit_lsn, change_lsn=change_lsn, commit_ts=commit_ts),
            transaction=TransactionRef(id=transaction.xid, position=position, size=size),
            event_ts=event_ts, event_date=event_date, ts_ms=ts_ms, ingested_at=ingested_at,
            flags=flags, before=before, after=message.new,
        ))
    return events


def to_line(event: Envelope) -> bytes:
    """One NDJSON line: compact separators, LF line end, no BOM. ASCII only: non-ASCII text is
    escaped, which keeps the output valid UTF-8 and stops U+2028, U+2029 and U+0085 (which some
    readers treat as line breaks) from appearing raw.

    Pydantic's serialiser writes the line directly (compact, in field order, control characters
    escaped), about three times faster than model_dump plus json.dumps, and produces the same
    text for ASCII data. The rare line with non-ASCII text is re-escaped the slow way.
    """
    text = event.model_dump_json(by_alias=True)
    if not text.isascii():
        text = json.dumps(json.loads(text), separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    return text.encode("ascii") + b"\n"
