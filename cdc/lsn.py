"""
lsn.py: log sequence numbers.

Postgres prints an LSN as two hex numbers, high 32 bits then low 32 bits ("0/1953870").
The contract carries LSNs as unsigned 64-bit integers, so they sort numerically.
"""
from __future__ import annotations

import re

from cdc.errors import DecodeError

_LSN = re.compile(r"([0-9A-Fa-f]{1,8})/([0-9A-Fa-f]{1,8})")


def parse_lsn(text: str) -> int:
    """'0/1953870' -> 26556528."""
    match = _LSN.fullmatch(text)
    if match is None:
        raise DecodeError(f"not an LSN: {text!r}")
    return (int(match.group(1), 16) << 32) + int(match.group(2), 16)


def format_lsn(value: int) -> str:
    """26556528 -> '0/1953870'."""
    if not 0 <= value < 1 << 64:
        raise ValueError(f"LSN out of range: {value}")
    return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"
