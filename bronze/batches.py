"""
batches.py: pass 2's rows as Arrow batches (GCS Plan (V2) section 19, G3 question 1).

Pass 2 must not hold dict rows: a dict row costs about 5.8 KB and an Arrow row about 0.62 KB, so one append of
the largest table as dicts needs about 5.0 GB and as Arrow about 1.6 GB (measured, DECISION_LOG "G3 question 1
evidence"). So each landed object's events are mapped (bronze.mapping.event_to_row), turned straight into an
Arrow table in the Iceberg table's own Arrow schema, and the dict rows are dropped. Whole objects are then
gathered into a Batch that closes once it holds at least BRONZE_BATCH_ROWS rows. The batch is concatenated and
sorted in Arrow by (commit_lsn, transaction_position) and appended as one Iceberg commit by the run function.

What this module guarantees:
  - the Arrow schema is the one PyIceberg derives from the table's own schema, so a required Bronze column is a
    non-nullable Arrow field. Building the table does not check nulls (pyarrow does not), but the append does: it
    refuses a null in a non-nullable column ("declared non-nullable but contains nulls") and refuses a nullable
    field for a required column, in both cases before anything is committed. The mapping already refuses a null
    in an envelope column, so this is the second lock on that door;
  - the rows are exactly what event_to_row produces, so row_to_event takes an Arrow row back to its event
    (tests/test_bronze_batches.py, also through a real local Iceberg table);
  - the sort is by the key of a change and is a courtesy, not a promise (Bronze contract section 5): no reader
    may rely on the order of a Bronze table. Within one table a key is unique, so the order is total and does not
    depend on the order the objects were added in.

Which events to take from an object (the ones whose key Bronze lacks and whose first copy is this object) is the
run function's job; this module is given the events to write and nothing else.
"""
from __future__ import annotations

from typing import Iterable

import pyarrow as pa
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema

from bronze.errors import MappingError
from bronze.mapping import event_to_row

SORT_KEY = [("commit_lsn", "ascending"), ("transaction_position", "ascending")]


def arrow_schema_of(schema: Schema) -> pa.Schema:
    """The Arrow schema of an Iceberg table schema: required columns non-nullable, list elements as declared,
    the Iceberg field ids in the field metadata."""
    return schema_to_pyarrow(schema)


def events_to_arrow(events: Iterable[dict], arrow_schema: pa.Schema, *, source_db: str, label: str = "") -> pa.Table:
    """The events as one Arrow table in `arrow_schema`. Each event goes through bronze.mapping.event_to_row, so
    a value of the wrong shape raises a MappingError (with `label`, the landed object, in its message). The dict
    rows live only inside this call. An empty iterable gives an empty table with the schema."""
    rows = []
    for event in events:
        try:
            rows.append(event_to_row(event, source_db=source_db))
        except MappingError as error:
            error.where = f"{label}: " if label else ""
            raise
    return pa.Table.from_pylist(rows, schema=arrow_schema)


def sort_table(table: pa.Table) -> pa.Table:
    """A new table ordered by (commit_lsn, transaction_position); the input is not changed."""
    return table.sort_by(SORT_KEY)


class Batch:
    """Arrow tables of whole landed objects, gathered until there are at least `min_rows` rows.

    add() takes one object's table; `full` says whether the batch should now be committed; take() returns
    everything gathered as one table sorted by key and empties the batch. A batch is a little over `min_rows`
    because objects are never split.
    """

    def __init__(self, arrow_schema: pa.Schema, min_rows: int) -> None:
        if min_rows < 1:
            raise ValueError(f"a batch needs at least 1 row, not {min_rows}")
        self._schema = arrow_schema
        self._min_rows = min_rows
        self._tables: list[pa.Table] = []
        self._rows = 0

    @property
    def rows(self) -> int:
        return self._rows

    @property
    def full(self) -> bool:
        return self._rows >= self._min_rows

    def add(self, table: pa.Table) -> None:
        if not table.schema.equals(self._schema, check_metadata=True):
            raise ValueError("the table's Arrow schema is not the batch's: a batch holds one Bronze table's rows")
        self._tables.append(table)
        self._rows += table.num_rows

    def take(self) -> pa.Table:
        """The gathered rows, concatenated and sorted by key. The batch is empty afterwards."""
        if not self._tables:
            raise ValueError("the batch is empty")
        combined = pa.concat_tables(self._tables)
        self._tables, self._rows = [], 0
        return sort_table(combined)
