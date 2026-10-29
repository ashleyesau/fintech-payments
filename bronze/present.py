"""
present.py: which keys a Bronze table already holds (GCS Plan (V2) section 19, G3 question 2).

Pass 2 appends only the changes Bronze does not have. This module reads the answer from Bronze itself: the
two key columns of one table (commit_lsn and transaction_position), streamed from the table's current
snapshot, and folded into a PresentKeys. A key is one change in one table, and a table holds one row per key
(Bronze contract section 6).

PresentKeys is a dict of commit_lsn to a bytearray with one byte per position. That costs about 1.2 to 1.6
bytes an event for backfill-shaped transactions (13 MB at 11M events) against 157 to 172 bytes an event for a
set of tuples (1.9 GB), measured in DECISION_LOG "G3 question 2 evidence". It supports `in` for a
(commit_lsn, position) pair, which is all Index.objects_missing needs.

The key columns are read once per table per run, before the table's first batch, at the table's current
snapshot. A table with no snapshot reads as empty. Two things stop the run, each with nothing written:
  - DuplicateBronzeKeys: the table holds a key twice;
  - UnknownBronzeKeys: the table holds a key that the landing index does not hold for this table.
The unknown keys are checked before a key is stored, so a corrupt position can never make PresentKeys
allocate for it, and they are reported first because they mean the warehouse does not belong to this landing.
"""
from __future__ import annotations

from typing import Iterable, Optional

import pyarrow as pa
from pyiceberg.table import Table

from bronze.errors import DuplicateBronzeKeys, UnknownBronzeKeys
from bronze.index import Index
from cdc.dedup import Key

KEY_COLUMNS = ("commit_lsn", "transaction_position")


class PresentKeys:
    """A set of (commit_lsn, position) keys, compact for the shape of the real data."""

    def __init__(self) -> None:
        self._positions: dict[int, bytearray] = {}
        self._count = 0

    def add(self, commit_lsn: int, position: int) -> bool:
        """Record a key. Returns False, and changes nothing, if it was already there."""
        if position < 0:
            raise ValueError(f"a position cannot be negative: {position}")
        positions = self._positions.get(commit_lsn)
        if positions is None:
            positions = self._positions[commit_lsn] = bytearray()
        if position >= len(positions):
            positions.extend(bytes(position + 1 - len(positions)))
        if positions[position]:
            return False
        positions[position] = 1
        self._count += 1
        return True

    def __contains__(self, key: object) -> bool:
        commit_lsn, position = key   # type: ignore[misc]
        positions = self._positions.get(commit_lsn)
        return positions is not None and 0 <= position < len(positions) and bool(positions[position])

    def __len__(self) -> int:
        return self._count


def _show(commit_lsn: int, position: int) -> str:
    return f"commit_lsn {commit_lsn:016x} position {position}"


def _landing_holds(index: Index, table: str, key: Key) -> bool:
    """True if the landing index has this change and it belongs to `table`."""
    try:
        return index.object_of(key).table == table
    except KeyError:
        return False


def fold_keys(table: str, batches: Iterable[pa.RecordBatch], index: Index) -> PresentKeys:
    """Fold key batches (commit_lsn, transaction_position) of the Bronze table `table` into a PresentKeys,
    checking each key against the landing index. Raises UnknownBronzeKeys, or else DuplicateBronzeKeys, after
    the last batch, so the message can count every key; a stop never returns a PresentKeys."""
    present = PresentKeys()
    unknown = duplicates = 0
    first_unknown: Optional[tuple[int, int]] = None
    first_duplicate: Optional[tuple[int, int]] = None
    for batch in batches:
        for commit_lsn, position in zip(batch.column(KEY_COLUMNS[0]).to_pylist(),
                                        batch.column(KEY_COLUMNS[1]).to_pylist()):
            if not _landing_holds(index, table, (commit_lsn, position)):
                unknown += 1
                first_unknown = first_unknown or (commit_lsn, position)
            elif not present.add(commit_lsn, position):
                duplicates += 1
                first_duplicate = first_duplicate or (commit_lsn, position)
    if unknown:
        raise UnknownBronzeKeys(
            f"Bronze table {table} holds {unknown} {'row' if unknown == 1 else 'rows'} whose key the landing does "
            f"not hold for {table}, first {_show(*first_unknown)}. An object may have been deleted from the "
            "landing, or this warehouse was built from another landing prefix, or something else wrote to the "
            "table. Nothing was written.")
    if duplicates:
        raise DuplicateBronzeKeys(
            f"Bronze table {table} holds {duplicates} {'row' if duplicates == 1 else 'rows'} whose key an earlier "
            f"row already has, first {_show(*first_duplicate)}. The contract allows one row per key, so appending "
            "is not safe. Nothing was written.")
    return present


def read_present_keys(table: str, bronze_table: Table, index: Index) -> PresentKeys:
    """The keys Bronze holds for `table` at its current snapshot: the two key columns only, streamed."""
    reader = bronze_table.scan(selected_fields=KEY_COLUMNS).to_arrow_batch_reader()
    return fold_keys(table, reader, index)
