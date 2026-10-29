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

import datetime
import gzip
import json
from dataclasses import dataclass
from typing import Optional

from bronze.landing import LandingReader
from cdc.landing import object_key
from tests.bronze_contract import TABLES as CONTRACT_COLUMNS
from tests.fakes import MemorySink

PREFIX = "cdc/cdc_r01/"
SOURCE_DB = "fintech_cdc"   # source.db of every synthetic event, and CdcSettings().db
DAY = "2025-03-01"
FIRST_LSN = 0x2B32560
LSN_STEP = 0x100


def clock_fields(clock: int) -> dict:
    return {"ts_ms": 1_000_000 + clock, "ingested_at": f"2026-10-28T10:00:{clock // 1000 % 60:02d}.{clock % 1000:03d}000Z"}


def column_value(column: str, kind: str, serial: int, date: str, old: bool = False):
    """A value of the right shape for a column of the given contract type (tests/bronze_contract.py). Text
    carries the serial so rows differ; numbers are constants, small enough for an int column, which also
    keeps them clear of the sentinels that line_template substitutes."""
    suffix = ":old" if old else ""
    if kind == "string":
        return "0.0700" if column == "total_cost_percent" else f"{column}:{serial}{suffix}"
    if kind == "int":
        return 7
    if kind == "long":
        return 123456789
    if kind == "boolean":
        return not old
    if kind == "date":
        return date
    if kind == "timestamptz":
        return f"{date}T08:00:00.000000Z"
    if kind == "list of string":
        return [f"{column}:{serial}{suffix}"]
    raise ValueError(f"no value for the contract type {kind!r}")


def row_image(table: str, serial: int, date: str, old: bool = False) -> dict:
    """A full row image in the table's column order, every value shaped as the CDC contract says."""
    return {column: column_value(column, kind, serial, date, old) for column, kind in CONTRACT_COLUMNS[table]}


def event(table: str, op: str, commit_lsn: int, position: int, size: int, *, date: str = DAY, xid: int = 700,
          serial: int = 0, clock: int = 0, flags: Optional[list] = None) -> dict:
    """One event as the consumer would write it. `serial` makes the row's values differ."""
    after = row_image(table, serial, date)
    before = row_image(table, serial, date, old=True) if op == "u" else None
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


# Sentinels that turn one real event into a line template: each stands for a value that changes from
# event to event. They are distinct, far from any other number in an event, and not substrings of each other.
_S = {"lsn": 4_444_444_440, "pos": 5_555_555_551, "size": 6_666_666_663, "xid": 7_777_777_775, "serial": 8_888_888_887}
_S_CHANGE = _S["lsn"] - 0x40 + _S["pos"]
_S_DATE = "2099-12-31"
_TEMPLATES: dict = {}


def line_template(table: str, op: str) -> str:
    """The JSON line of an event with %(lsn)d, %(change)d, %(pos)d, %(size)d, %(xid)d, %(serial)d and
    %(date)s where those values go, so a line is one string format instead of a dict and a dump."""
    if (table, op) not in _TEMPLATES:
        e = event(table, op, _S["lsn"], _S["pos"], _S["size"], date=_S_DATE, xid=_S["xid"], serial=_S["serial"])
        text = json.dumps(e, separators=(",", ":"), ensure_ascii=True)
        for token, number in (("%(change)d", _S_CHANGE), ("%(lsn)d", _S["lsn"]), ("%(pos)d", _S["pos"]),
                              ("%(size)d", _S["size"]), ("%(xid)d", _S["xid"]), ("%(serial)d", _S["serial"])):
            text = text.replace(str(number), token)
        _TEMPLATES[(table, op)] = text.replace(_S_DATE, "%(date)s")
    return _TEMPLATES[(table, op)]


class LazyLanding:
    """A landing generated on demand, for sets too big to hold. It answers list_keys and read like a
    sink and keeps nothing: read(key) builds that object's bytes and returns them.

    Transaction i has `tx_size` changes: tx_size - merchants of them merchant_transactions inserts, then
    `merchants` merchants inserts, all on day i // 2 of 2025. Each transaction makes two objects, so a
    set of 600 transactions of 2,000 changes is 1.2M events in 1,200 objects, about the real run's
    shape (908 objects, 1.02M events) and larger. Its lines equal, byte for byte, the lines Landing makes
    from the same transactions (tests/test_bronze_synth.py); the gzip is level 1 here, for speed.
    """

    def __init__(self, transactions: int, tx_size: int = 2000, merchants: int = 50, prefix: str = PREFIX) -> None:
        self.transactions, self.tx_size, self.merchants, self.prefix = transactions, tx_size, merchants, prefix
        self.events = transactions * tx_size
        self._names: dict[str, tuple[int, str]] = {}
        for i in range(transactions):
            lsn, date = self.commit_lsn(i), self.date(i)
            for table in ("merchant_transactions", "merchants"):
                self._names[object_key(prefix, table, date, lsn, lsn)] = (i, table)

    @staticmethod
    def commit_lsn(i: int) -> int:
        return FIRST_LSN + i * LSN_STEP

    @staticmethod
    def date(i: int) -> str:
        return (datetime.date(2025, 1, 1) + datetime.timedelta(days=i // 2)).isoformat()

    def changes(self) -> list:
        """The (table, op) of each position of every transaction."""
        return ([("merchant_transactions", "c")] * (self.tx_size - self.merchants) + [("merchants", "c")] * self.merchants)

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(key for key in self._names if key.startswith(prefix))

    def read(self, key: str) -> bytes:
        i, table = self._names[key]
        template = line_template(table, "c")
        lsn, date, xid = self.commit_lsn(i), self.date(i), 700 + i
        first = 0 if table == "merchant_transactions" else self.tx_size - self.merchants
        last = self.tx_size - self.merchants if table == "merchant_transactions" else self.tx_size
        lines = "\n".join(
            template % {"lsn": lsn, "change": lsn - 0x40 + p, "pos": p, "size": self.tx_size, "xid": xid,
                        "serial": i * self.tx_size + p, "date": date}
            for p in range(first, last)) + "\n"
        return gzip.compress(lines.encode("ascii"), compresslevel=1, mtime=0)
