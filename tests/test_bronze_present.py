"""tests/test_bronze_present.py: G3 step 1, the keys a Bronze table already holds (bronze/present.py).

What is held here, each with a mutation that fails it (see the commit message):
  - PresentKeys: add, `in`, len, gaps, no wrap-around for a negative position, a negative position refused;
  - fold_keys: folds several batches, and stops with UnknownBronzeKeys (a key the landing lacks, a position outside
    its transaction, a key the landing holds for another table) or DuplicateBronzeKeys (within a batch and across
    batches), counting rows and naming the first key; unknown is reported first; unknown keys are never stored;
  - read_present_keys on a real local Iceberg table: a table with no snapshot reads as empty, several commits fold
    together, only the two key columns are asked for, Index.objects_missing works with the result unchanged, and
    a duplicate row or a row the landing lacks stops the read.
Everything is local: a temporary SQLite catalog and warehouse, and the in-memory landing of tests/bronze_synth.py.
"""
from dataclasses import replace

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import schema_to_pyarrow

from bronze import present as present_module
from bronze.errors import BronzeError, ContractError, DuplicateBronzeKeys, UnknownBronzeKeys
from bronze.index import build_index
from bronze.mapping import event_to_row
from bronze.present import KEY_COLUMNS, PresentKeys, fold_keys, read_present_keys
from bronze.schema import build_partition_spec, build_schema, table_properties
from tests.bronze_synth import SOURCE_DB, Landing

M, T, A = "merchants", "merchant_transactions", "loan_applications"


def keys_batch(*keys) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(
        [pa.array([k[0] for k in keys], pa.int64()), pa.array([k[1] for k in keys], pa.int32())],
        names=list(KEY_COLUMNS))


def lsn(value: int) -> str:
    return f"{value:016x}"


# ---------------------------------------------------------------------------
# PresentKeys
# ---------------------------------------------------------------------------

def test_an_empty_set_holds_nothing():
    present = PresentKeys()
    assert len(present) == 0 and (1, 0) not in present


def test_add_says_whether_the_key_was_new_and_counts_it_once():
    present = PresentKeys()
    assert present.add(16, 0) is True
    assert present.add(16, 0) is False
    assert present.add(16, 1) is True
    assert len(present) == 2


def test_in_is_true_only_for_the_keys_added():
    present = PresentKeys()
    present.add(16, 1)
    present.add(32, 0)
    assert (16, 1) in present and (32, 0) in present
    assert (16, 0) not in present and (32, 1) not in present and (48, 0) not in present


def test_a_gap_below_a_position_is_not_present():
    present = PresentKeys()
    present.add(16, 5)
    assert [p for p in range(8) if (16, p) in present] == [5]
    present.add(16, 2)
    assert [p for p in range(8) if (16, p) in present] == [2, 5]
    assert len(present) == 2


def test_a_negative_position_never_wraps_round_to_the_end():
    present = PresentKeys()
    present.add(16, 5)
    assert (16, -1) not in present and (16, -6) not in present


def test_a_position_past_the_end_is_not_present():
    present = PresentKeys()
    present.add(16, 5)
    assert (16, 6) not in present and (16, 10_000) not in present


def test_adding_a_negative_position_is_refused_and_changes_nothing():
    present = PresentKeys()
    present.add(16, 3)
    with pytest.raises(ValueError, match="negative"):
        present.add(16, -1)
    assert len(present) == 1 and (16, 3) in present


def test_a_position_added_out_of_order_keeps_the_earlier_ones():
    present = PresentKeys()
    for position in (0, 3, 1, 2):
        present.add(16, position)
    assert [(16, p) in present for p in range(5)] == [True, True, True, True, False]


# ---------------------------------------------------------------------------
# fold_keys over hand-made batches
# ---------------------------------------------------------------------------

def landing_with_two_tables():
    """Three transactions: a (merchants x2, merchant_transactions x1), b (merchant_transactions x2), c (merchants)."""
    land = Landing()
    a = land.transaction((M, "c"), (M, "c"), (T, "c"))
    b = land.transaction((T, "c"), (T, "c"))
    c = land.transaction((M, "c"))
    land.flush()
    return build_index(land.reader(), source_db=SOURCE_DB), a, b, c


def test_the_batches_are_folded_into_one_set():
    index, a, b, c = landing_with_two_tables()
    present = fold_keys(M, [keys_batch((a.commit_lsn, 0)), keys_batch((a.commit_lsn, 1), (c.commit_lsn, 0))], index)
    assert len(present) == 3
    assert {(a.commit_lsn, 0), (a.commit_lsn, 1), (c.commit_lsn, 0)} == {
        k for k in [(a.commit_lsn, p) for p in range(3)] + [(c.commit_lsn, 0)] if k in present}


def test_no_batches_is_an_empty_set():
    index = landing_with_two_tables()[0]
    assert len(fold_keys(M, [], index)) == 0
    assert len(fold_keys(M, [keys_batch()], index)) == 0


def test_a_key_the_landing_never_had_stops_the_fold_and_is_counted_with_its_first_example():
    index, a, *_ = landing_with_two_tables()
    batches = [keys_batch((a.commit_lsn, 0)), keys_batch((0x999, 0), (0x888, 3))]
    with pytest.raises(UnknownBronzeKeys) as raised:
        fold_keys(M, batches, index)
    text = str(raised.value)
    assert isinstance(raised.value, ContractError) and isinstance(raised.value, BronzeError)
    assert "Bronze table merchants holds 2 rows whose key the landing does not hold for merchants" in text
    assert f"first commit_lsn {lsn(0x999)} position 0" in text
    assert "Nothing was written" in text


def test_one_unknown_row_is_worded_in_the_singular():
    index = landing_with_two_tables()[0]
    with pytest.raises(UnknownBronzeKeys, match="holds 1 row whose key"):
        fold_keys(M, [keys_batch((0x999, 0))], index)


def test_a_position_outside_its_transaction_is_unknown():
    index, a, *_ = landing_with_two_tables()
    with pytest.raises(UnknownBronzeKeys, match="holds 2 rows"):
        fold_keys(M, [keys_batch((a.commit_lsn, 3), (a.commit_lsn, -1))], index)


def test_a_key_the_landing_holds_for_another_table_is_unknown():
    index, a, *_ = landing_with_two_tables()
    # (a, 2) is a merchant_transactions change in the landing; a merchants table may not hold it.
    with pytest.raises(UnknownBronzeKeys) as raised:
        fold_keys(M, [keys_batch((a.commit_lsn, 2))], index)
    assert f"first {'commit_lsn ' + lsn(a.commit_lsn)} position 2" in str(raised.value)
    assert len(fold_keys(T, [keys_batch((a.commit_lsn, 2))], index)) == 1


def test_a_key_twice_in_one_batch_stops_the_fold():
    index, a, *_ = landing_with_two_tables()
    with pytest.raises(DuplicateBronzeKeys) as raised:
        fold_keys(M, [keys_batch((a.commit_lsn, 0), (a.commit_lsn, 1), (a.commit_lsn, 0))], index)
    text = str(raised.value)
    assert isinstance(raised.value, ContractError) and isinstance(raised.value, BronzeError)
    assert "Bronze table merchants holds 1 row whose key an earlier row already has" in text
    assert f"first commit_lsn {lsn(a.commit_lsn)} position 0" in text
    assert "Nothing was written" in text


def test_a_key_in_two_batches_stops_the_fold_and_every_extra_row_is_counted():
    index, a, b, c = landing_with_two_tables()
    batches = [keys_batch((a.commit_lsn, 1), (c.commit_lsn, 0)),
               keys_batch((c.commit_lsn, 0), (a.commit_lsn, 1)),
               keys_batch((a.commit_lsn, 1))]
    with pytest.raises(DuplicateBronzeKeys) as raised:
        fold_keys(M, batches, index)
    text = str(raised.value)
    assert "holds 3 rows whose key an earlier row already has" in text
    assert f"first commit_lsn {lsn(c.commit_lsn)} position 0" in text


def test_unknown_keys_are_reported_before_duplicates():
    index, a, *_ = landing_with_two_tables()
    batch = keys_batch((a.commit_lsn, 0), (a.commit_lsn, 0), (0x999, 0))
    with pytest.raises(UnknownBronzeKeys):
        fold_keys(M, [batch], index)


def test_an_unknown_key_is_never_stored_so_a_corrupt_position_cannot_make_it_allocate(monkeypatch):
    index, a, *_ = landing_with_two_tables()
    stored = []
    real_add = PresentKeys.add

    def spy(self, commit_lsn, position):
        stored.append((commit_lsn, position))
        return real_add(self, commit_lsn, position)

    monkeypatch.setattr(PresentKeys, "add", spy)
    batch = keys_batch((a.commit_lsn, 0), (a.commit_lsn, 100_000_000), (0x999, 5), (a.commit_lsn, 2))
    with pytest.raises(UnknownBronzeKeys, match="holds 3 rows"):
        fold_keys(M, [batch], index)
    assert stored == [(a.commit_lsn, 0)]


def test_a_stop_hands_back_no_set(monkeypatch):
    index, a, *_ = landing_with_two_tables()
    got = []
    with pytest.raises(DuplicateBronzeKeys):
        got.append(fold_keys(M, [keys_batch((a.commit_lsn, 0), (a.commit_lsn, 0))], index))
    assert got == []


# ---------------------------------------------------------------------------
# read_present_keys on a real local Iceberg table
# ---------------------------------------------------------------------------

def catalog_in(path) -> SqlCatalog:
    path.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog("bronze", uri=f"sqlite:///{path / 'catalog.db'}", warehouse=f"file://{path / 'warehouse'}")
    catalog.create_namespace("bronze")
    return catalog


def create(path, table):
    schema = build_schema(table)
    return catalog_in(path).create_table(f"bronze.{table}", schema=schema,
                                         partition_spec=build_partition_spec(schema),
                                         properties=table_properties("cdc/cdc_r01/", "fintech_cdc"))


def append(table, events):
    """Append landed events to a Bronze table as rows, through the real mapping."""
    rows = [event_to_row(e, source_db=SOURCE_DB) for e in events]
    table.append(pa.Table.from_pylist(rows, schema=schema_to_pyarrow(table.schema())))


def events_of(tx, table):
    return [e for e in tx.events() if e["source"]["table"] == table]


def landing_of_transactions():
    """Five merchant_transactions-only transactions over two days, flushed as two objects (one per day)."""
    land = Landing()
    first = [land.transaction((T, "c"), (T, "c")), land.transaction((T, "c"))]
    second = [land.transaction((T, "c"), date="2025-03-02"), land.transaction((T, "c"), (T, "c"), date="2025-03-02")]
    land.flush()
    return land, first, second


def test_a_table_with_no_snapshot_reads_as_empty(tmp_path):
    land, *_ = landing_of_transactions()
    index = build_index(land.reader(), source_db=SOURCE_DB)
    made = create(tmp_path, T)
    assert made.current_snapshot() is None
    assert len(read_present_keys(T, made, index)) == 0


def test_the_keys_of_several_commits_are_folded_together(tmp_path):
    land, first, second = landing_of_transactions()
    index = build_index(land.reader(), source_db=SOURCE_DB)
    made = create(tmp_path, T)
    for tx in first:
        append(made, events_of(tx, T))
    append(made, [e for tx in second for e in events_of(tx, T)])
    present = read_present_keys(T, made, index)
    assert len(present) == 6
    assert all((tx.commit_lsn, p) in present for tx in first + second for p in range(len(tx.changes)))
    assert (first[0].commit_lsn, 2) not in present and (0x1, 0) not in present


def test_the_table_is_read_at_its_current_snapshot_each_time(tmp_path):
    land, first, second = landing_of_transactions()
    index = build_index(land.reader(), source_db=SOURCE_DB)
    made = create(tmp_path, T)
    append(made, events_of(first[0], T))
    assert len(read_present_keys(T, made, index)) == 2
    append(made, events_of(first[1], T))
    assert len(read_present_keys(T, made, index)) == 3


def test_only_the_two_key_columns_are_asked_for(tmp_path):
    land, first, _ = landing_of_transactions()
    index = build_index(land.reader(), source_db=SOURCE_DB)
    made = create(tmp_path, T)
    append(made, events_of(first[0], T))
    asked = []

    class Spy:
        def __init__(self, real):
            self._real = real

        def scan(self, **kwargs):
            asked.append(kwargs)
            return self._real.scan(**kwargs)

    assert len(read_present_keys(T, Spy(made), index)) == 2
    assert asked == [{"selected_fields": ("commit_lsn", "transaction_position")}]


def test_objects_missing_works_with_the_result_unchanged(tmp_path):
    land, first, second = landing_of_transactions()
    index = build_index(land.reader(), source_db=SOURCE_DB)
    made = create(tmp_path, T)
    assert [o.key for o in index.objects_missing(T, read_present_keys(T, made, index))] == [
        o.key for o in index.objects]
    for tx in first:
        append(made, events_of(tx, T))
    missing = index.objects_missing(T, read_present_keys(T, made, index))
    assert [o.key for o in missing] == [o.key for o in index.objects if o.key.split("event_date=")[1][:10] == "2025-03-02"]
    append(made, [e for tx in second for e in events_of(tx, T)])
    assert index.objects_missing(T, read_present_keys(T, made, index)) == []


def test_a_row_appended_twice_stops_the_read_with_the_table_named(tmp_path):
    land, first, _ = landing_of_transactions()
    index = build_index(land.reader(), source_db=SOURCE_DB)
    made = create(tmp_path, T)
    append(made, events_of(first[0], T))
    append(made, events_of(first[0], T))
    with pytest.raises(DuplicateBronzeKeys, match="Bronze table merchant_transactions holds 2 rows whose key an earlier"):
        read_present_keys(T, made, index)


def test_a_row_the_landing_does_not_hold_stops_the_read(tmp_path):
    land, first, _ = landing_of_transactions()
    index = build_index(land.reader(), source_db=SOURCE_DB)
    other = Landing()
    stranger = replace(other.transaction((T, "c")), commit_lsn=0x4000000)
    made = create(tmp_path, T)
    append(made, events_of(first[0], T))
    append(made, [e for e in stranger.events()])
    with pytest.raises(UnknownBronzeKeys, match="Bronze table merchant_transactions holds 1 row whose key the landing"):
        read_present_keys(T, made, index)


def test_the_module_reads_the_key_columns_the_contract_calls_the_key():
    assert KEY_COLUMNS == ("commit_lsn", "transaction_position")
    assert set(KEY_COLUMNS) <= {f.name for f in build_schema(T).fields}
    assert present_module.KEY_COLUMNS is KEY_COLUMNS
