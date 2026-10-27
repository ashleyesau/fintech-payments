"""
dedup.py: reading landed events back, and the contract's integrity rules (section 7).

Delivery is at-least-once, so the same change can land in more than one object: a batch that
was uploaded but never confirmed is replayed, and if more data arrived meanwhile the replay's
objects have different names and overlap the old ones. The dedup key is
(source.commit_lsn, transaction.position). Replays differ only in ts_ms and ingested_at, so any
one copy will do; this keeps the first.
"""
from __future__ import annotations

import gzip
import json
from typing import Iterable

Key = tuple[int, int]


def events_in(data: bytes) -> list[dict]:
    """The events in one landed object, as parsed JSON, in order."""
    text = gzip.decompress(data)
    if not text.endswith(b"\n"):
        raise ValueError("an NDJSON object ends with a line feed")
    return [json.loads(line) for line in text[:-1].split(b"\n")]


def key(event: dict) -> Key:
    return event["source"]["commit_lsn"], event["transaction"]["position"]


def dedup(events: Iterable[dict]) -> list[dict]:
    """Each change once, first copy kept, in the order first seen."""
    seen: set[Key] = set()
    unique = []
    for event in events:
        k = key(event)
        if k not in seen:
            seen.add(k)
            unique.append(event)
    return unique


def incomplete_transactions(events: Iterable[dict]) -> dict[int, str]:
    """commit_lsn -> what is wrong, for every transaction whose landed events are not exactly
    positions 0 to size - 1 (contract section 7). Empty when every transaction is complete.
    Pass events from all the objects of a flush, or from the whole run, after dedup."""
    positions: dict[int, set[int]] = {}
    sizes: dict[int, set[int]] = {}
    for event in events:
        commit_lsn = event["source"]["commit_lsn"]
        positions.setdefault(commit_lsn, set()).add(event["transaction"]["position"])
        sizes.setdefault(commit_lsn, set()).add(event["transaction"]["size"])
    problems = {}
    for commit_lsn, found in positions.items():
        if len(sizes[commit_lsn]) != 1:
            problems[commit_lsn] = f"events disagree about the transaction size: {sorted(sizes[commit_lsn])}"
            continue
        (size,) = sizes[commit_lsn]
        if found != set(range(size)):
            problems[commit_lsn] = f"has positions {sorted(found)} of {size}"
    return problems
