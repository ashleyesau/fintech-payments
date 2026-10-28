"""tests/bronze_synth.py: synthetic landed objects for the Bronze tests.

The events are plain dicts in the contract's field order, so thousands can be made quickly, and they
are encoded exactly as the consumer encodes them (compact JSON, ASCII, LF, gzip with mtime 0). Two
tests in test_bronze_index.py hold this to the real thing: the Envelope model accepts every event and
cdc.envelope.to_line gives the same bytes.

A Landing is a small bucket you fill with transactions:

    land = Landing()
    a = land.transaction(("merchants", "c"), ("risk_events", "c"))
    land.flush()                       # one object per table and event date, like the consumer
    land.reader()                      # a LandingReader over it

flush(txs, clock) cuts the given transactions again with a different consumer clock: the same changes
under the same names (a replay), or, if the set is bigger, under new overlapping names. Anything can be
made invalid by editing an event and passing the events to put().
"""
from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from typing import Optional

from bronze.landing import LandingReader
from cdc.columns import EXPECTED_COLUMNS
from cdc.landing import object_key
from tests.fakes import MemorySink

PREFIX = "cdc/cdc_r01/"
DAY = "2025-03-01"
FIRST_LSN = 0x2B32560
LSN_STEP = 0x100


def clock_fields(clock: int) -> dict:
    return {"ts_ms": 1_000_000 + clock, "ingested_at": f"2026-10-28T10:00:{clock // 1000 % 60:02d}.{clock % 1000:03d}000Z"}


def event(table: str, op: str, commit_lsn: int, position: int, size: int, *, date: str = DAY, xid: int = 700,
          serial: int = 0, clock: int = 0, flags: Optional[list] = None) -> dict:
    """One event as the consumer would write it. `serial` makes the row's values differ."""
    columns = EXPECTED_COLUMNS[table]
    after = {c: f"{c}:{serial}" for c in columns}
    before = {c: f"{c}:{serial}:old" for c in columns} if op == "u" else None
    return {
        "envelope_version": 1, "op": op,
        "source": {"db": "fintech_cdc", "schema": "public", "table": table, "commit_lsn": commit_lsn,
                   "change_lsn": commit_lsn - 0x40 + position, "commit_ts": f"{date}T10:00:00.000000Z"},
        "transaction": {"id": xid, "position": position, "size": size},
        "event_ts": f"{date}T08:00:00.000000Z", "event_date": date,
        **clock_fields(clock),
        "flags": list(flags or []), "before": before, "after": after,
    }


def encode(events: list[dict]) -> bytes:
    lines = b"".join(json.dumps(e, separators=(",", ":"), ensure_ascii=True).encode("ascii") + b"\n" for e in events)
    return gzip.compress(lines, mtime=0)


def name(table: str, date: str, events: list[dict], prefix: str = PREFIX) -> str:
    return object_key(prefix, table, date, events[0]["source"]["commit_lsn"], events[-1]["source"]["commit_lsn"])


@dataclass(frozen=True)
class Tx:
    commit_lsn: int
    xid: int
    changes: tuple    # (table, op, date) for each position
    serial: int       # the first change's serial; the rest follow

    def events(self, clock: int = 0) -> list[dict]:
        size = len(self.changes)
        return [event(table, op, self.commit_lsn, position, size, date=date, xid=self.xid,
                      serial=self.serial + position, clock=clock)
                for position, (table, op, date) in enumerate(self.changes)]


class Landing:
    def __init__(self, prefix: str = PREFIX) -> None:
        self.prefix = prefix
        self.sink = MemorySink()
        self.txs: list[Tx] = []
        self._pending: list[Tx] = []
        self._next_lsn = FIRST_LSN
        self._serial = 0

    def transaction(self, *changes, date: str = DAY) -> Tx:
        """A transaction of (table, op) or (table, op, date) changes, buffered until the next flush."""
        full = tuple((c[0], c[1], c[2] if len(c) > 2 else date) for c in changes)
        tx = Tx(self._next_lsn, 700 + len(self.txs), full, self._serial)
        self._next_lsn += LSN_STEP
        self._serial += len(full)
        self.txs.append(tx)
        self._pending.append(tx)
        return tx

    def flush(self, txs: Optional[list] = None, clock: int = 0) -> list[str]:
        """Cut transactions into objects, one per table and event date, and put them. Returns the names.
        With no argument it cuts everything buffered. Names already taken are left alone (a replay)."""
        txs = self._pending if txs is None else txs
        self._pending = [t for t in self._pending if t not in txs]
        groups: dict = {}
        for tx in txs:
            for e in tx.events(clock):
                groups.setdefault((e["source"]["table"], e["event_date"]), []).append(e)
        keys = []
        for (table, date), events in groups.items():
            key = name(table, date, events, self.prefix)
            self.sink.put_if_absent(key, encode(events))
            keys.append(key)
        return keys

    def put(self, table: str, date: str, events: list[dict], lsns: Optional[tuple] = None) -> str:
        """Put arbitrary events (valid or not) under the name their first and last commit LSN give, or
        under the (first, last) in `lsns`, which makes a different name for the same events."""
        key = name(table, date, events, self.prefix) if lsns is None else object_key(self.prefix, table, date, *lsns)
        self.sink.put_if_absent(key, encode(events))
        return key

    def reader(self) -> LandingReader:
        return LandingReader(self.sink, self.prefix)
