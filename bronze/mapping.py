"""
mapping.py: one landed event to one Bronze row, and back.

Two pure functions, both strict (GCS Plan (V2) section 8 and 10, DECISION_LOG "G2 Q4" and "G2 Q7"):

  event_to_row(event, source_db=...)         -> dict of Bronze column name -> Python value
  row_to_event(table, row, source_db=...)    -> the event dict, in the contract's field and key order

The conversion and the shape check are the same code. A value converts or raises a MappingError that
names the table, the column, what was expected and what was found; there is no separate validator that
could disagree with the converter, and nothing is coerced: an integer is an int (not a bool, a float or
a string) that fits its column's width, a timestamp is exactly 'YYYY-MM-DDTHH:MM:SS.ffffffZ', a date is
exactly 'YYYY-MM-DD'. Pass 1 calls event_to_row on every event and throws the row away, so a value of
the wrong shape stops the run before pass 2 and before any commit (G2 Q7).

Rows hold: str, int, bool, datetime.date, a timezone aware UTC datetime.datetime (microsecond
precision, so lossless), None, and list of str or None. NUMERIC (total_cost_percent) stays its exact text.

The column types come from bronze.schema (so the generated schema and the mapping cannot disagree) and
the field lists from cdc.columns. The 13 envelope columns are all required. Three event fields are not
Bronze columns by decision G2 Q1: source.db (must equal the warehouse's database, `source_db`),
source.schema (must be 'public') and source.table (it is the Iceberg table).

Two things the mapping checks beyond a value's own shape, because Bronze's contract promises them:
  - event_date is the South African date (UTC+2) of event_ts, so a row can never land in the wrong month;
  - an update carries a before image, an insert has none, and only loan_applications has updates.
Integer lower bounds follow the CDC contract (LSNs, ids, positions, ts_ms at least 0; size at least 1;
position below size); upper bounds are the column's width, not the consumer's own 2^53 stop.
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache

from pyiceberg.types import BooleanType, DateType, IntegerType, ListType, LongType, StringType, TimestamptzType

from bronze.errors import MappingError
from bronze.schema import BEFORE_PREFIX, BEFORE_TABLES, build_schema
from cdc import types
from cdc.columns import EXPECTED_COLUMNS, EXPECTED_TYPES, SCHEMA
from cdc.envelope import Envelope, Source, TransactionRef

_UTC = timezone.utc
_SAST = timedelta(hours=2)
FLAGS = ("noop_update", "event_time_fallback")

_EVENT_KEYS = frozenset(Envelope.model_fields)
_SOURCE_KEYS = frozenset(f.alias or name for name, f in Source.model_fields.items())
_TRANSACTION_KEYS = frozenset(TransactionRef.model_fields)

_TIMESTAMP = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})\.([0-9]{6})Z")
_DATE = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})")
_NUMERIC = re.compile(r"-?[0-9]+(\.[0-9]+)?")


class _Bad(ValueError):
    """A value is not what its column holds. Caught where the column is known and turned into a MappingError."""


class _Kind:
    """How one kind of column converts: `forward` takes the event's JSON value to the row's Python value,
    `backward` the other way. Both raise _Bad for anything that is not exactly the right shape."""

    __slots__ = ("expected", "forward", "backward")

    def __init__(self, expected: str, forward, backward) -> None:
        self.expected, self.forward, self.backward = expected, forward, backward


def _text(value):
    if type(value) is str and (value.isascii() or _encodable(value)):
        return value
    raise _Bad


def _encodable(value: str) -> bool:
    """False for text that cannot be written as UTF-8 (a lone surrogate): Parquet would fail on it at write."""
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _boolean(value):
    if type(value) is bool:
        return value
    raise _Bad


def _integer_kind(bits: int, minimum: int | None = None) -> _Kind:
    low = -(1 << (bits - 1)) if minimum is None else minimum
    high = (1 << (bits - 1)) - 1

    def check(value):
        if type(value) is int and low <= value <= high:
            return value
        raise _Bad

    return _Kind(f"an integer from {low} to {high}", check, check)


def _date_forward(value):
    if type(value) is str:
        match = _DATE.fullmatch(value)
        if match is not None:
            try:
                return date(int(match[1]), int(match[2]), int(match[3]))
            except ValueError:
                pass
    raise _Bad


def _date_backward(value):
    if type(value) is date:
        return f"{value.year:04d}-{value.month:02d}-{value.day:02d}"
    raise _Bad


def _timestamp_forward(value):
    if type(value) is str:
        match = _TIMESTAMP.fullmatch(value)
        if match is not None:
            try:
                return datetime(int(match[1]), int(match[2]), int(match[3]), int(match[4]), int(match[5]),
                                int(match[6]), int(match[7]), tzinfo=_UTC)
            except ValueError:
                pass
    raise _Bad


def _timestamp_backward(value):
    if type(value) is datetime and value.utcoffset() is not None:
        u = value.astimezone(_UTC)
        return (f"{u.year:04d}-{u.month:02d}-{u.day:02d}T{u.hour:02d}:{u.minute:02d}:{u.second:02d}"
                f".{u.microsecond:06d}Z")
    raise _Bad


def _numeric(value):
    if type(value) is str and _NUMERIC.fullmatch(value):
        return value
    raise _Bad


def _text_list(value):
    """A source list column: a list whose elements are text or null."""
    if type(value) is list:
        for element in value:
            if element is not None and not (type(element) is str and (element.isascii() or _encodable(element))):
                raise _Bad
        return list(value)
    raise _Bad


def _flags(value):
    if type(value) is list and all(type(flag) is str and flag in FLAGS for flag in value):
        return list(value)
    raise _Bad


def _op(value):
    if type(value) is str and value in ("c", "u"):
        return value
    raise _Bad


def _version(value):
    if type(value) is int and value == 1:
        return value
    raise _Bad


TEXT = _Kind("a string", _text, _text)
BOOLEAN = _Kind("true or false", _boolean, _boolean)
DATE = _Kind("a date 'YYYY-MM-DD'", _date_forward, _date_backward)
TIMESTAMP = _Kind("a timestamp 'YYYY-MM-DDTHH:MM:SS.ffffffZ'", _timestamp_forward, _timestamp_backward)
NUMERIC = _Kind("a decimal string such as '0.0700'", _numeric, _numeric)
TEXT_LIST = _Kind("an array of strings (elements may be null)", _text_list, _text_list)

# The 13 envelope columns: (Bronze column, the event field it comes from, how it converts). All required.
# The paths are what the coverage test holds against the Envelope model.
ENVELOPE_FIELDS: tuple[tuple[str, tuple[str, ...], _Kind], ...] = (
    ("op", ("op",), _Kind("'c' or 'u'", _op, _op)),
    ("event_date", ("event_date",), DATE),
    ("event_ts", ("event_ts",), TIMESTAMP),
    ("commit_lsn", ("source", "commit_lsn"), _integer_kind(64, 0)),
    ("change_lsn", ("source", "change_lsn"), _integer_kind(64, 0)),
    ("commit_ts", ("source", "commit_ts"), TIMESTAMP),
    ("transaction_position", ("transaction", "position"), _integer_kind(32, 0)),
    ("transaction_size", ("transaction", "size"), _integer_kind(32, 1)),
    ("transaction_xid", ("transaction", "id"), _integer_kind(64, 0)),
    ("ts_ms", ("ts_ms",), _integer_kind(64, 0)),
    ("ingested_at", ("ingested_at",), TIMESTAMP),
    ("flags", ("flags",), _Kind("an array of 'noop_update' or 'event_time_fallback'", _flags, _flags)),
    ("envelope_version", ("envelope_version",), _Kind("1", _version, _version)),
)
# Event fields that are deliberately not Bronze columns (G2 Q1). `before` and `after` are the images.
DROPPED_FIELDS: tuple[tuple[str, ...], ...] = (("source", "db"), ("source", "schema"), ("source", "table"))


@lru_cache(maxsize=None)
def _columns(table: str) -> tuple[tuple[str, _Kind], ...]:
    """(column, kind) for the table's own columns in order, from the generated schema."""
    schema = build_schema(table)
    plan = []
    for column in EXPECTED_COLUMNS[table]:
        field_type = schema.find_field(column).field_type
        if isinstance(field_type, StringType):
            kind = NUMERIC if EXPECTED_TYPES[table][column] == frozenset({types.NUMERIC}) else TEXT
        elif isinstance(field_type, IntegerType):
            kind = _integer_kind(32)
        elif isinstance(field_type, LongType):
            kind = _integer_kind(64)
        elif isinstance(field_type, BooleanType):
            kind = BOOLEAN
        elif isinstance(field_type, DateType):
            kind = DATE
        elif isinstance(field_type, TimestamptzType):
            kind = TIMESTAMP
        elif isinstance(field_type, ListType):
            kind = TEXT_LIST
        else:
            raise TypeError(f"no mapping for {table}.{column} of type {field_type}")
        plan.append((column, kind))
    return tuple(plan)


def _show(value) -> str:
    try:
        text = json.dumps(value, ensure_ascii=True)
    except (TypeError, ValueError):
        text = repr(value)
    return text if len(text) <= 70 else text[:67] + "..."


def _shape(table, column: str, kind: _Kind, value, field=None) -> MappingError:
    return MappingError(table, column, f"expected {kind.expected}, got {_show(value)}", field=field)


def _keys(table, what: str, found, expected: frozenset) -> None:
    """The fields of an object are exactly `expected`, in any order."""
    if type(found) is not dict:
        raise MappingError(table, what, f"expected an object, got {_show(found)}")
    if found.keys() == expected:
        return
    detail = []
    missing = sorted(expected - found.keys())
    extra = sorted(map(str, found.keys() - expected))
    if missing:
        detail.append(f"missing {', '.join(missing)}")
    if extra:
        detail.append(f"unexpected {', '.join(extra)}")
    raise MappingError(table, what, f"fields differ from the contract ({'; '.join(detail)})")


def _table_of(event) -> str:
    """The table an event is for. When it cannot be told, say what is wrong with the event's fields first."""
    if type(event) is not dict:
        raise MappingError(None, "event", f"expected an object, got {_show(event)}")
    source = event.get("source")
    table = source.get("table") if type(source) is dict else None
    if type(table) is str and table in EXPECTED_COLUMNS:
        return table
    _keys(None, "event", event, _EVENT_KEYS)
    _keys(None, "source", source, _SOURCE_KEYS)
    raise MappingError(None, "source.table", f"expected one of {', '.join(EXPECTED_COLUMNS)}, got {_show(table)}")


def _check_invariants(table: str, op: str, position: int, size: int, event_ts: datetime, event_date: date) -> None:
    if position >= size:
        raise MappingError(table, "transaction_position", f"{position} is not inside a transaction of {size} changes",
                           field="transaction.position")
    try:
        south_african_date = (event_ts + _SAST).date()
    except OverflowError:                       # the last two hours of year 9999 have no South African date
        south_african_date = None
    if south_african_date != event_date:
        raise MappingError(
            table, "event_date",
            f"{event_date.isoformat()} is not the South African date of event_ts {_timestamp_backward(event_ts)}; "
            "the partition would be wrong", field="event_date")
    if op == "u" and table not in BEFORE_TABLES:
        raise MappingError(table, "op", f"an update on {table}: only {', '.join(BEFORE_TABLES)} is ever updated")


def event_to_row(event: dict, *, source_db: str) -> dict:
    """The Bronze row of one event. Raises MappingError for anything that is not exactly the contract."""
    table = _table_of(event)
    _keys(table, "event", event, _EVENT_KEYS)
    _keys(table, "source", event["source"], _SOURCE_KEYS)
    _keys(table, "transaction", event["transaction"], _TRANSACTION_KEYS)

    row: dict = {}
    for column, path, kind in ENVELOPE_FIELDS:
        value = event[path[0]] if len(path) == 1 else event[path[0]][path[1]]
        field = None if len(path) == 1 and path[0] == column else ".".join(path)
        try:
            row[column] = kind.forward(value)
        except _Bad:
            raise _shape(table, column, kind, value, field) from None

    source = event["source"]
    if source["schema"] != SCHEMA:
        raise MappingError(table, "source.schema", f"expected {_show(SCHEMA)}, got {_show(source['schema'])}")
    if source["db"] != source_db:
        raise MappingError(table, "source.db", f"expected {_show(source_db)}, the database this warehouse was built "
                           f"from, got {_show(source['db'])}")
    op = row["op"]
    _check_invariants(table, op, row["transaction_position"], row["transaction_size"], row["event_ts"], row["event_date"])

    plan = _columns(table)
    after = event["after"]
    _keys(table, "after", after, frozenset(EXPECTED_COLUMNS[table]))
    for column, kind in plan:
        value = after[column]
        if value is None:
            row[column] = None
            continue
        try:
            row[column] = kind.forward(value)
        except _Bad:
            raise _shape(table, column, kind, value) from None

    if table in BEFORE_TABLES:
        before = event["before"]
        if op == "c":
            if before is not None:
                raise MappingError(table, "before", f"an insert has no before image, got {_show(before)}")
            for column, _ in plan:
                row[BEFORE_PREFIX + column] = None
        else:
            _keys(table, "before", before, frozenset(EXPECTED_COLUMNS[table]))
            for column, kind in plan:
                value = before[column]
                if value is None:
                    row[BEFORE_PREFIX + column] = None
                    continue
                try:
                    row[BEFORE_PREFIX + column] = kind.forward(value)
                except _Bad:
                    raise _shape(table, BEFORE_PREFIX + column, kind, value, f"before.{column}") from None
    elif event["before"] is not None:
        raise MappingError(table, "before", f"{table} has no before image, got {_show(event['before'])}")
    return row


def sort_rows(rows: list[dict]) -> list[dict]:
    """A new list of `rows` ordered by (commit_lsn, transaction_position), the key of a change.

    A courtesy and not a promise (G2 Q9, decision D-d): the writer sorts each batch it appends so that a
    re-run writes rows in the same order and files are tidy, but no reader may rely on any order in a Bronze
    table; Silver orders explicitly. Within one table a key is unique, so the order is total and does not
    depend on the order the rows arrived in. The input is not changed.
    """
    return sorted(rows, key=lambda row: (row["commit_lsn"], row["transaction_position"]))


def row_to_event(table: str, row: dict, *, source_db: str) -> dict:
    """The event a Bronze row came from, with the contract's field order and every field of the landed object.
    Strict in the same way: a value of the wrong Python type, a missing or extra column, or a before_ column
    set on an insert raises MappingError."""
    if type(table) is not str or table not in EXPECTED_COLUMNS:
        raise MappingError(None, "table", f"expected one of {', '.join(EXPECTED_COLUMNS)}, got {_show(table)}")
    plan = _columns(table)
    columns = ([column for column, _, _ in ENVELOPE_FIELDS] + [column for column, _ in plan]
               + ([BEFORE_PREFIX + column for column, _ in plan] if table in BEFORE_TABLES else []))
    _keys(table, "row", row, frozenset(columns))

    values: dict = {}
    for column, path, kind in ENVELOPE_FIELDS:
        value = row[column]
        try:
            values[column] = kind.backward(value)
        except _Bad:
            raise _shape(table, column, kind, value) from None
    op = values["op"]
    _check_invariants(table, op, values["transaction_position"], values["transaction_size"],
                      row["event_ts"].astimezone(_UTC), row["event_date"])

    def image(prefix: str) -> dict:
        out = {}
        for column, kind in plan:
            value = row[prefix + column]
            if value is None:
                out[column] = None
                continue
            try:
                out[column] = kind.backward(value)
            except _Bad:
                raise _shape(table, prefix + column, kind, value) from None
        return out

    after = image("")
    before = None
    if table in BEFORE_TABLES:
        if op == "u":
            before = image(BEFORE_PREFIX)
        else:
            for column, _ in plan:
                if row[BEFORE_PREFIX + column] is not None:
                    raise MappingError(table, BEFORE_PREFIX + column,
                                       f"an insert has no before image, got {_show(row[BEFORE_PREFIX + column])}")
    return {
        "envelope_version": values["envelope_version"],
        "op": op,
        "source": {"db": source_db, "schema": SCHEMA, "table": table, "commit_lsn": values["commit_lsn"],
                   "change_lsn": values["change_lsn"], "commit_ts": values["commit_ts"]},
        "transaction": {"id": values["transaction_xid"], "position": values["transaction_position"],
                        "size": values["transaction_size"]},
        "event_ts": values["event_ts"], "event_date": values["event_date"], "ts_ms": values["ts_ms"],
        "ingested_at": values["ingested_at"], "flags": values["flags"],
        "before": before, "after": after,
    }
