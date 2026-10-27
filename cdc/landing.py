"""
landing.py: from decoded transactions to objects, and one object into a sink.

`build_batch` turns whole transactions into the objects the contract describes (section 2):
one object per table and event date, events in (commit_lsn, position) order, NDJSON, gzipped,
named by the lowest and highest commit LSN in it. Taking whole transactions is what keeps a
transaction from being split: every event of every transaction handed in lands in this batch,
though a transaction's changes go to several objects (one per table and date).

`land_object` writes one object without ever overwriting. A replay builds the same names (the
same changes, from the same position) but not the same bytes: ts_ms and ingested_at are the
consumer's clock. So when the name is taken the existing object is read and compared with
those two fields removed (M4-Q5, option B): equal means the object already landed; different
means something is wrong and the consumer stops.
"""
from __future__ import annotations

import gzip
import json
import zlib
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Literal, Optional

from cdc.envelope import Envelope, build_events, to_line
from cdc.errors import ObjectMismatch
from cdc.sink import Sink
from cdc.transactions import Transaction

# The only fields that differ between a batch and its replay (contract section 7).
CLOCK_FIELDS = ("ts_ms", "ingested_at")


@dataclass(frozen=True, slots=True)
class Object:
    key: str
    table: str
    event_date: str
    first_lsn: int   # lowest commit_lsn in the object
    last_lsn: int    # highest commit_lsn in the object
    events: int
    data: bytes      # the gzipped NDJSON that is uploaded


def object_key(prefix: str, table: str, event_date: str, first_lsn: int, last_lsn: int) -> str:
    """cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_<first>_<last>.ndjson.gz, with each LSN
    as 16 lowercase hex digits so names sort in LSN order (contract section 2)."""
    return f"{prefix}{table}/event_date={event_date}/lsn_{first_lsn:016x}_{last_lsn:016x}.ndjson.gz"


def make_object(prefix: str, table: str, event_date: str, events: list[Envelope]) -> Object:
    first, last = events[0].source.commit_lsn, events[-1].source.commit_lsn
    # mtime=0: gzip otherwise writes the current time into its header, so the same input would
    # give different bytes from one second to the next.
    data = gzip.compress(b"".join(to_line(event) for event in events), mtime=0)
    return Object(object_key(prefix, table, event_date, first, last), table, event_date, first, last,
                  len(events), data)


def build_batch(transactions: Iterable[Transaction], database: str, prefix: str,
                now: Optional[datetime] = None) -> list[Object]:
    """The objects for these transactions, which must be whole and in commit order. A transaction
    with no published changes contributes nothing; a batch of only those returns []."""
    groups: dict[tuple[str, str], list[Envelope]] = {}
    for transaction in transactions:
        for event in build_events(transaction, database, now):
            groups.setdefault((event.source.table, event.event_date), []).append(event)
    return [make_object(prefix, table, event_date, events) for (table, event_date), events in groups.items()]


def _without_clock(data: bytes) -> list[dict]:
    text = gzip.decompress(data)
    if not text.endswith(b"\n"):
        raise ValueError("an NDJSON object ends with a line feed")
    events = [json.loads(line) for line in text[:-1].split(b"\n")]
    for event in events:
        if not isinstance(event, dict):
            raise ValueError("every line of an NDJSON object of events is a JSON object")
        for field in CLOCK_FIELDS:
            event.pop(field)
    return events


def same_events(existing: bytes, new: bytes) -> bool:
    """True if two objects hold the same events in the same order, ignoring ts_ms and
    ingested_at. An object that is not valid gzipped NDJSON of events is simply not the same."""
    try:
        return _without_clock(existing) == _without_clock(new)
    except (OSError, EOFError, zlib.error, ValueError, KeyError):
        return False


def land_object(sink: Sink, obj: Object) -> Literal["created", "replayed"]:
    """Write one object. "created": it is new. "replayed": it was already there with the same
    events (a replay of a batch that had landed). Raises ObjectMismatch if the name is taken by
    different events, and SinkError if the sink gave up."""
    if sink.put_if_absent(obj.key, obj.data):
        return "created"
    if same_events(sink.read(obj.key), obj.data):
        return "replayed"
    raise ObjectMismatch(
        f"{obj.key} already exists and holds different events from this batch; the same name should "
        "mean the same changes, so the batch boundaries or the data changed. Nothing was overwritten "
        "and the batch is not confirmed.")
