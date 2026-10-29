"""
events.py: one landed object, checked and reduced to what the index keeps.

Pass 1 (GCS Plan (V2) section 4, G1 design decision 1) reads each object once and keeps, per event,
only its key, its transaction size, its operation and a hash of its content. This module is that
reduction: summarise_object parses an object, runs the structural checks of plan section 10 on every
event and returns the compact tuples. Nothing here holds more than one object.

The structural checks (check_event) are the cheap ones. They stop the run, with the object and the
event named, on:
  - a line that is not a JSON object, or an object that is not gzipped NDJSON ending in a line feed;
  - fields that differ from the envelope (contract section 3), at the top level, in source and in
    transaction (names, in any order), taken from the Envelope model so there is one copy of the contract;
  - envelope_version other than 1, an op other than c or u, an unknown table;
  - after (and before, where there is one) whose keys differ from cdc.columns.EXPECTED_COLUMNS, in
    name, number or order;
  - a before image on an insert, no before image on an update, or an update on any table but
    loan_applications (the consumer stops on those, so the contract says they never land);
  - a transaction position outside its size, or a commit LSN that is not an integer below 2^63;
  - an event that does not belong to the object that holds it: its table or event date differs from
    the object's name, or its commit LSN lies outside the LSN range in the name.
Value shapes (timestamps, types, NUMERIC text) are checked by the mapping (bronze.mapping), which
summarise_object runs on every event after these checks and discards the row: a value of the wrong shape stops
pass 1, before any commit (G2 Q7).

Two copies of one change must agree on everything except ts_ms and ingested_at. event_hash is what
the index compares: an 8 byte blake2b digest of the event without those two fields, keys sorted, so
the order of keys in a line does not matter. Eight bytes make a false "agree" a 1 in 2^64 chance per
compared pair, which is far below anything that matters at 11M events.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import zlib
from dataclasses import dataclass

from bronze.errors import EventError, MappingError
from bronze.landing import LandedObject
from bronze.mapping import event_to_row
from cdc.columns import EXPECTED_COLUMNS
from cdc.dedup import events_in
from cdc.envelope import Envelope, Source, TransactionRef
from cdc.landing import CLOCK_FIELDS

# Bronze stores commit_lsn as a long (Bronze contract section 3).
LSN_LIMIT = 2 ** 63
# A guard against a corrupt size allocating memory in the index, not a contract rule (the contract
# says only size >= 1). The real run's largest transaction is a few thousand changes.
MAX_TRANSACTION_SIZE = 10_000_000

_ENVELOPE_KEYS = tuple(Envelope.model_fields)
_SOURCE_KEYS = tuple(f.alias or name for name, f in Source.model_fields.items())
_TRANSACTION_KEYS = tuple(TransactionRef.model_fields)

# (commit_lsn, position, transaction size, op, content hash)
Row = tuple[int, int, int, str, int]


class _Violation(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ObjectSummary:
    obj: LandedObject
    rows: list[Row]


def _is_int(value) -> bool:
    return type(value) is int


def _same_keys(what: str, found, expected: tuple[str, ...]) -> None:
    """The fields of an envelope object are the contract's, in any order."""
    if not isinstance(found, dict):
        raise _Violation(f"{what} is not an object")
    missing = [k for k in expected if k not in found]
    extra = [k for k in found if k not in expected]
    if missing or extra:
        detail = []
        if missing:
            detail.append(f"missing {', '.join(missing)}")
        if extra:
            detail.append(f"unexpected {', '.join(map(str, extra))}")
        raise _Violation(f"{what} fields differ from the contract ({'; '.join(detail)})")


def _same_columns(what: str, image, table: str) -> None:
    expected = EXPECTED_COLUMNS[table]
    if not isinstance(image, dict):
        raise _Violation(f"{what} is not an object")
    if tuple(image) == expected:
        return
    missing = [c for c in expected if c not in image]
    extra = [c for c in image if c not in expected]
    detail = []
    if missing:
        detail.append(f"missing {', '.join(missing)}")
    if extra:
        detail.append(f"unexpected {', '.join(map(str, extra))}")
    if not detail:
        detail.append("the same columns in a different order")
    raise _Violation(f"{what} columns differ from the {table} model ({'; '.join(detail)}). "
                     "A schema change needs a new migration, a CDC update and a new Bronze contract version")


def check_event(event, obj: LandedObject) -> tuple[int, int, int, str]:
    """Raise _Violation unless `event` is a structurally valid event that belongs to `obj`.
    Returns (commit_lsn, position, size, op)."""
    _same_keys("the event", event, _ENVELOPE_KEYS)
    version = event["envelope_version"]
    if not _is_int(version) or version != 1:
        raise _Violation(f"envelope_version is {version!r}, not 1")
    op = event["op"]
    if op not in ("c", "u"):
        raise _Violation(f"op is {op!r}, not c or u")

    _same_keys("source", event["source"], _SOURCE_KEYS)
    _same_keys("transaction", event["transaction"], _TRANSACTION_KEYS)
    table = event["source"]["table"]
    if not isinstance(table, str) or table not in EXPECTED_COLUMNS:
        raise _Violation(f"{table!r} is not one of the five tables: {', '.join(EXPECTED_COLUMNS)}")
    if table != obj.table:
        raise _Violation(f"the event is for {table} but the object name says {obj.table}")
    if event["event_date"] != obj.event_date:
        raise _Violation(f"event_date is {event['event_date']!r} but the object name says {obj.event_date}")

    commit_lsn = event["source"]["commit_lsn"]
    if not _is_int(commit_lsn) or not 0 <= commit_lsn < LSN_LIMIT:
        raise _Violation(f"commit_lsn {commit_lsn!r} is not an integer from 0 to 2^63 - 1")
    if not obj.first_lsn <= commit_lsn <= obj.last_lsn:
        raise _Violation(f"commit_lsn {commit_lsn:016x} lies outside the object name's range "
                         f"{obj.first_lsn:016x} to {obj.last_lsn:016x}")
    position, size = event["transaction"]["position"], event["transaction"]["size"]
    if not _is_int(position) or not _is_int(size) or not 0 <= position < size:
        raise _Violation(f"transaction position {position!r} is not inside a transaction of size {size!r}")
    if size > MAX_TRANSACTION_SIZE:
        raise _Violation(f"transaction size {size} is above {MAX_TRANSACTION_SIZE}, which is not plausible "
                         "for this source; raise MAX_TRANSACTION_SIZE if it is real")

    _same_columns("after", event["after"], table)
    before = event["before"]
    if op == "c":
        if before is not None:
            raise _Violation("an insert has a before image")
    else:
        if table != "loan_applications":
            raise _Violation(f"an update on {table}: only loan_applications is ever updated")
        if before is None:
            raise _Violation("an update has no before image")
        _same_columns("before", before, table)
    return commit_lsn, position, size, op


def event_hash(event: dict) -> int:
    """The content hash of an event without ts_ms and ingested_at."""
    body = {k: v for k, v in event.items() if k not in CLOCK_FIELDS}
    text = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return int.from_bytes(hashlib.blake2b(text.encode("ascii"), digest_size=8).digest(), "big")


def summarise_object(obj: LandedObject, data: bytes, *, source_db: str) -> ObjectSummary:
    """Parse and check every event of one landed object and reduce it to index rows. Each event is also
    mapped to its Bronze row (bronze.mapping) and the row thrown away, so a value of the wrong shape, or an
    event from another source database, raises a MappingError that names this object and event."""
    try:
        events = events_in(data)
    except (OSError, EOFError, zlib.error, ValueError) as error:
        raise EventError(f"{obj.key}: not valid gzipped NDJSON of events ({type(error).__name__}: {error})") from error
    rows: list[Row] = []
    for number, event in enumerate(events, start=1):
        try:
            commit_lsn, position, size, op = check_event(event, obj)
        except _Violation as violation:
            raise EventError(f"{obj.key}, event {number}: {violation}") from None
        try:
            event_to_row(event, source_db=source_db)
        except MappingError as error:
            error.where = f"{obj.key}, event {number}: "
            raise
        rows.append((commit_lsn, position, size, op, event_hash(event)))
    return ObjectSummary(obj, rows)
