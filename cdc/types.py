"""
types.py: the value mapping from pgoutput text to JSON-ready Python values.

pgoutput (text format) delivers every value as text. The consumer maps it by the column's type
OID, following section 4 of the CDC Change Event Contract (v1). Only the eight types the five
tables use are mapped; any other type OID stops the consumer.

Each converter takes the value as a str and returns a str, int, bool, list or None-free
value; NULL is handled by the caller. Bad input raises ValueError (the decoder reports it
with the column name).
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Callable

from cdc.errors import UnknownTypeError

BOOL, INT8, INT4, TEXT, DATE, TIMESTAMPTZ, NUMERIC, TEXT_ARRAY = 16, 20, 23, 25, 1082, 1184, 1700, 1009

# Beyond this many JSON readers lose integer precision (2**53 - 1).
SAFE_INT = 9_007_199_254_740_991


def to_text(value: str) -> str:
    return value


def to_int(value: str) -> int:
    if not value.lstrip("-").isdigit():
        raise ValueError(f"not an integer: {value!r}")
    number = int(value)
    if not -SAFE_INT <= number <= SAFE_INT:
        raise ValueError(f"integer {number} is outside plus or minus {SAFE_INT}")
    return number


def to_bool(value: str) -> bool:
    if value == "t":
        return True
    if value == "f":
        return False
    raise ValueError(f"not a boolean: {value!r}")


def to_date(value: str) -> str:
    """YYYY-MM-DD, validated. 'infinity' and BC dates are refused."""
    if _DATE.fullmatch(value) is None:
        raise ValueError(f"not a plain date: {value!r}")
    return date.fromisoformat(value).isoformat()


def to_numeric(value: str) -> str:
    """The exact text with its scale ('0.0700'). Never a float."""
    return value


_DATE = re.compile(r"\d{4}-\d\d-\d\d")
_TIMESTAMP = re.compile(
    r"(\d{4})-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)(?:\.(\d{1,6}))?([+-])(\d\d)(?::(\d\d)(?::(\d\d))?)?"
)


def to_timestamptz(value: str) -> str:
    """'2025-03-01 22:30:12.5+02' -> '2025-03-01T20:30:12.500000Z'.

    The offset in the text is parsed, never assumed. The result is always UTC with six
    fractional digits, so every timestamp has one shape. 'infinity', BC and five-digit years
    are refused.
    """
    match = _TIMESTAMP.fullmatch(value)
    if match is None:
        raise ValueError(f"not a timestamptz: {value!r}")
    year, month, day, hour, minute, second, fraction, sign, off_h, off_m, off_s = match.groups()
    moment = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second),
                      int((fraction or "").ljust(6, "0")))
    offset = timedelta(hours=int(off_h), minutes=int(off_m or 0), seconds=int(off_s or 0))
    moment = moment - offset if sign == "+" else moment + offset
    return format_utc(moment)


def format_utc(moment: datetime) -> str:
    """A naive UTC datetime as YYYY-MM-DDTHH:MM:SS.ffffffZ."""
    return (f"{moment.year:04d}-{moment.month:02d}-{moment.day:02d}T"
            f"{moment.hour:02d}:{moment.minute:02d}:{moment.second:02d}.{moment.microsecond:06d}Z")


def to_text_array(value: str) -> list:
    """A one-dimensional text[] literal as a list: '{}' is [], NULL elements are None, quoted
    elements are unescaped. Nested arrays and dimension decorations are refused."""
    if len(value) < 2 or value[0] != "{" or value[-1] != "}":
        raise ValueError(f"not a one-dimensional array literal: {value!r}")
    body = value[1:-1]
    if not body:
        return []
    elements: list = []
    i, n = 0, len(body)
    while True:
        if i >= n:
            raise ValueError(f"malformed array literal: {value!r}")
        if body[i] == '"':
            i += 1
            chars = []
            while True:
                if i >= n:
                    raise ValueError(f"unterminated quote in array literal: {value!r}")
                char = body[i]
                if char == "\\":
                    i += 1
                    if i >= n:
                        raise ValueError(f"dangling escape in array literal: {value!r}")
                    chars.append(body[i])
                elif char == '"':
                    i += 1
                    break
                else:
                    chars.append(char)
                i += 1
            elements.append("".join(chars))
        else:
            j = body.find(",", i)
            j = n if j < 0 else j
            token = body[i:j]
            if not token or any(c in token for c in '{}"\\'):
                raise ValueError(f"unsupported array literal: {value!r}")
            elements.append(None if token == "NULL" else token)
            i = j
        if i == n:
            return elements
        if body[i] != ",":
            raise ValueError(f"malformed array literal: {value!r}")
        i += 1


CONVERTERS: dict[int, Callable[[str], object]] = {
    BOOL: to_bool,
    INT8: to_int,
    INT4: to_int,
    TEXT: to_text,
    DATE: to_date,
    TIMESTAMPTZ: to_timestamptz,
    NUMERIC: to_numeric,
    TEXT_ARRAY: to_text_array,
}


def converter_for(type_oid: int, column: str = "") -> Callable[[str], object]:
    try:
        return CONVERTERS[type_oid]
    except KeyError:
        raise UnknownTypeError(f"column {column!r} has type OID {type_oid}, which the contract does not map") from None
