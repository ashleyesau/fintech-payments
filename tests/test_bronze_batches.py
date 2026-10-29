"""tests/test_bronze_batches.py: G3 step 2, pass 2's rows as Arrow batches (bronze/batches.py).

What is held here, each with a mutation that fails it (see the commit message):
  - events_to_arrow gives exactly the rows event_to_row gives, in the table's own Arrow schema, with required
    columns non-nullable and optional ones nullable, for all five tables;
  - the round trip: Arrow row -> row_to_event equals the event, over awkward values in every column, in memory and
    through a real local Iceberg table (append, scan, map back);
  - a null in a required column is refused before an append; a mapping error names the landed object;
  - sort_table orders by (commit_lsn, transaction_position), numerically, leaves its input alone and keeps the schema;
  - Batch gathers whole objects, closes at the row count, refuses another table's rows and an empty take, and gives
    the same sorted content for any arrival order and any way the objects were cut.
Everything is local: a temporary SQLite catalog and warehouse, and the in-memory landing of tests/bronze_synth.py.
"""
import random

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from bronze import batches
from bronze.batches import Batch, arrow_schema_of, events_to_arrow, sort_table
from bronze.errors import MappingError
from bronze.mapping import event_to_row, row_to_event
from bronze.schema import INTEGER_WIDTHS, build_partition_spec, build_schema, table_properties
from cdc.columns import EXPECTED_COLUMNS
from cdc.dedup import events_in
from tests import bronze_contract as contract
from tests.bronze_synth import FIRST_LSN, LSN_STEP, SOURCE_DB, Landing, event
from tests.test_bronze_mapping import changed, same_event

TABLES = tuple(EXPECTED_COLUMNS)
M, T, A = "merchants", "merchant_transactions", "loan_applications"


def schema_of(table):
    return arrow_schema_of(build_schema(table))


def events_for(table, count=1, op="c", first=1):
    """`count` events of `table` with distinct commit LSNs, one change each."""
    return [event(table, op, FIRST_LSN + LSN_STEP * (first + i), 0, 1, serial=first + i) for i in range(count)]


def to_arrow(table, events, **kw):
    return events_to_arrow(events, schema_of(table), source_db=SOURCE_DB, **kw)


# ---------------------------------------------------------------------------
# events_to_arrow
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table", TABLES)
def test_the_arrow_rows_are_exactly_the_mapped_rows(table):
    events = events_for(table, 3)
    arrow = to_arrow(table, events)
    assert arrow.num_rows == 3
    assert arrow.to_pylist() == [event_to_row(e, source_db=SOURCE_DB) for e in events]


def test_an_update_gives_a_row_with_its_before_columns():
    e = events_for(A, 1, op="u")[0]
    row = to_arrow(A, [e]).to_pylist()[0]
    assert row["op"] == "u"
    for column in ("application_id", "merchant_id", "decision", "months_trading", "declared_monthly_revenue",
                   "decline_reasons", "total_cost_percent"):
        assert row["before_" + column] == e["before"][column] and row[column] == e["after"][column], column


@pytest.mark.parametrize("table", TABLES)
def test_the_arrow_schema_is_the_tables_with_required_columns_non_nullable(table):
    iceberg = build_schema(table)
    arrow = to_arrow(table, events_for(table, 2)).schema
    assert arrow.equals(arrow_schema_of(iceberg), check_metadata=True)
    assert [f.name for f in arrow] == [f.name for f in iceberg.fields]
    assert {f.name: f.nullable for f in arrow} == {f.name: not f.required for f in iceberg.fields}
    assert sum(not f.nullable for f in arrow) == 13


def test_the_thirteen_required_envelope_columns_are_the_non_nullable_ones():
    arrow = schema_of(M)
    assert [f.name for f in arrow if not f.nullable] == [name for name, _ in contract.ENVELOPE]


def test_an_empty_iterable_gives_an_empty_table_with_the_schema():
    empty = to_arrow(M, [])
    assert empty.num_rows == 0 and empty.schema.equals(schema_of(M), check_metadata=True)


def test_events_can_be_given_as_a_generator_and_are_consumed_once():
    events = events_for(M, 4)
    assert to_arrow(M, (e for e in events)).num_rows == 4


def test_a_null_in_a_required_column_is_refused_by_the_append_and_nothing_is_committed(tmp_path, monkeypatch):
    """Building the table does not check nulls (pyarrow does not); the non-nullable Arrow field makes the append
    refuse it, before a snapshot exists."""
    real = batches.event_to_row

    def nulled(event, **kw):
        row = real(event, **kw)
        row["op"] = None
        return row

    monkeypatch.setattr(batches, "event_to_row", nulled)
    arrow = to_arrow(M, events_for(M, 1))
    assert arrow.column("op").null_count == 1
    made = create(tmp_path, M)
    with pytest.raises(pa.ArrowInvalid, match="Column 'op' is declared non-nullable but contains nulls"):
        made.append(arrow)
    assert made.current_snapshot() is None


def test_a_nullable_field_for_a_required_column_is_refused_by_the_append(tmp_path):
    loose = pa.schema([f.with_nullable(True) for f in schema_of(M)], metadata=schema_of(M).metadata)
    made = create(tmp_path, M)
    with pytest.raises(ValueError, match="Mismatch in fields"):
        made.append(events_to_arrow(events_for(M, 1), loose, source_db=SOURCE_DB))
    assert made.current_snapshot() is None


def test_a_mapping_error_names_the_landed_object():
    bad = changed(events_for(M, 1)[0], ("after", "risk_score"), "not a number")
    key = "cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_0000000002b32660_0000000002b32660.ndjson.gz"
    with pytest.raises(MappingError) as raised:
        to_arrow(M, [bad], label=key)
    assert str(raised.value).startswith(f"{key}: merchants.risk_score:")


def test_a_mapping_error_without_a_label_has_no_prefix():
    bad = changed(events_for(M, 1)[0], ("after", "risk_score"), "not a number")
    with pytest.raises(MappingError) as raised:
        to_arrow(M, [bad])
    assert str(raised.value).startswith("merchants.risk_score:")


def test_an_event_from_another_source_database_is_refused():
    with pytest.raises(MappingError, match="source.db"):
        events_to_arrow(events_for(M, 1), schema_of(M), source_db="another_db")


# ---------------------------------------------------------------------------
# the round trip, over awkward values
# ---------------------------------------------------------------------------

AWKWARD_TEXT = ["Zoë Müller 日本語 \U0001F680", 'quote " and \\ backslash\nline\tbreak\x01', "", " ", "x" * 5000, "null"]


def varied(table, op, serial, picker):
    """An event of `table` whose every table column is set by picker(column, kind, number); None keeps it."""
    e = event(table, op, FIRST_LSN + LSN_STEP * serial, 0, 1, serial=serial)
    images = ("after", "before") if op == "u" else ("after",)
    for number, (column, kind) in enumerate(contract.TABLES[table]):
        value = picker(column, kind, number)
        if value is not KEEP:
            for image in images:
                e = changed(e, (image, column), value)
    return e


KEEP = object()


def awkward_events(table):
    ops = ("c", "u") if table == A else ("c",)
    out = []
    serial = 1
    for op in ops:
        def limit(low):
            def pick(column, kind, number):
                if kind in ("int", "long"):
                    bits = INTEGER_WIDTHS[(table, column)]
                    return -(1 << (bits - 1)) if low else (1 << (bits - 1)) - 1
                return KEEP
            return pick

        def text(column, kind, number):
            if kind == "string":
                return "-12345.6789" if column == "total_cost_percent" else AWKWARD_TEXT[number % len(AWKWARD_TEXT)]
            if kind == "boolean":
                return number % 2 == 0
            if kind == "date":
                return "2024-02-29"
            if kind == "timestamptz":
                return "2024-02-29T23:59:59.999999Z"
            if kind == "list of string":
                return ["a", None, "日本", ""]
            return KEEP

        pickers = [lambda c, k, n: KEEP, lambda c, k, n: None, limit(True), limit(False), text]
        for picker in pickers:
            out.append(varied(table, op, serial, picker))
            serial += 1
    return out


@pytest.mark.parametrize("table", TABLES)
def test_arrow_rows_map_back_to_the_events_they_came_from(table):
    events = awkward_events(table)
    arrow = to_arrow(table, events)
    back = [row_to_event(table, row, source_db=SOURCE_DB) for row in arrow.to_pylist()]
    assert len(back) == len(events) >= 5
    for original, restored in zip(events, back):
        same_event(restored, original)


def test_the_awkward_events_really_are_awkward():
    nulls = awkward_events(M)[1]
    assert all(v is None for v in nulls["after"].values())
    assert awkward_events(T)[3]["after"]["amount_minor"] == (1 << 63) - 1
    assert awkward_events(A)[-1]["before"]["decline_reasons"] == ["a", None, "日本", ""]


def catalog_in(path) -> SqlCatalog:
    path.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog("bronze", uri=f"sqlite:///{path / 'catalog.db'}", warehouse=f"file://{path / 'warehouse'}")
    catalog.create_namespace("bronze")
    return catalog


def create(path, table):
    schema = build_schema(table)
    return catalog_in(path).create_table(f"bronze.{table}", schema=schema, partition_spec=build_partition_spec(schema),
                                         properties=table_properties("cdc/cdc_r01/", SOURCE_DB))


@pytest.mark.parametrize("table", TABLES)
def test_the_rows_survive_a_real_local_iceberg_table(tmp_path, table):
    """Append the Arrow table, read it back through a scan and map every row to its event: the same events."""
    events = awkward_events(table)
    made = create(tmp_path, table)
    made.append(to_arrow(table, events))
    read = made.scan().to_arrow().to_pylist()
    assert len(read) == len(events)
    back = sorted((row_to_event(table, row, source_db=SOURCE_DB) for row in read),
                  key=lambda e: e["source"]["commit_lsn"])
    for original, restored in zip(sorted(events, key=lambda e: e["source"]["commit_lsn"]), back):
        same_event(restored, original)


def test_a_batch_of_several_objects_appended_as_one_commit_reads_back_sorted_content(tmp_path):
    made = create(tmp_path, T)
    tables = [to_arrow(T, events_for(T, 3, first=first)) for first in (7, 1, 4)]
    batch = Batch(schema_of(T), min_rows=1)
    for t in tables:
        batch.add(t)
    made.append(batch.take())
    assert len(made.snapshots()) == 1
    lsns = [r["commit_lsn"] for r in made.scan().to_arrow().to_pylist()]
    assert sorted(lsns) == [FIRST_LSN + LSN_STEP * n for n in range(1, 10)]


# ---------------------------------------------------------------------------
# sort_table
# ---------------------------------------------------------------------------

def keyed(*pairs):
    """A merchants table of rows with the given (commit_lsn, position) keys, otherwise typical."""
    events = [changed(event(M, "c", lsn, position, position + 1, serial=i), ("source", "change_lsn"), lsn)
              for i, (lsn, position) in enumerate(pairs)]
    return to_arrow(M, events)


def keys_of(table):
    return list(zip(table.column("commit_lsn").to_pylist(), table.column("transaction_position").to_pylist()))


def test_the_sort_orders_by_commit_lsn_then_position():
    table = keyed((30, 1), (10, 2), (30, 0), (20, 0), (10, 0), (10, 1))
    assert keys_of(sort_table(table)) == [(10, 0), (10, 1), (10, 2), (20, 0), (30, 0), (30, 1)]


def test_the_sort_is_ascending_in_both_parts():
    assert keys_of(sort_table(keyed((2, 5), (1, 9)))) == [(1, 9), (2, 5)]
    assert keys_of(sort_table(keyed((1, 9), (1, 2)))) == [(1, 2), (1, 9)]


def test_the_sort_is_numeric_not_textual():
    assert keys_of(sort_table(keyed((100, 0), (9, 0), (9, 10), (9, 2)))) == [(9, 0), (9, 2), (9, 10), (100, 0)]


def test_the_sort_handles_the_largest_commit_lsn():
    top = (1 << 63) - 1
    assert keys_of(sort_table(keyed((top, 0), (top - 1, 3), (5, 0)))) == [(5, 0), (top - 1, 3), (top, 0)]


@pytest.mark.parametrize("seed", range(8))
def test_the_sort_gives_the_same_order_for_any_arrival_order(seed):
    pairs = [(lsn, position) for lsn in range(1, 25) for position in range(lsn % 4 + 1)]
    shuffled = pairs[:]
    random.Random(seed).shuffle(shuffled)
    assert keys_of(sort_table(keyed(*shuffled))) == sorted(pairs)


def test_the_sort_returns_a_new_table_keeps_the_schema_and_leaves_its_input_alone():
    table = keyed((3, 0), (1, 0), (2, 0))
    before = table.to_pylist()
    result = sort_table(table)
    assert table.to_pylist() == before and result is not table
    assert result.schema.equals(table.schema, check_metadata=True) and result.num_rows == 3


def test_the_sort_carries_every_column_with_its_row():
    table = keyed((3, 0), (1, 0), (2, 0))
    by_lsn = {r["commit_lsn"]: r for r in table.to_pylist()}
    assert sort_table(table).to_pylist() == [by_lsn[1], by_lsn[2], by_lsn[3]]


def test_the_sort_of_nothing_and_of_one_row():
    assert sort_table(keyed()).num_rows == 0
    assert keys_of(sort_table(keyed((4, 2)))) == [(4, 2)]


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------

def test_a_batch_is_not_full_until_it_holds_the_minimum_and_then_is():
    batch = Batch(schema_of(M), min_rows=5)
    assert batch.rows == 0 and not batch.full
    batch.add(to_arrow(M, events_for(M, 2)))
    assert batch.rows == 2 and not batch.full
    batch.add(to_arrow(M, events_for(M, 2, first=3)))
    assert batch.rows == 4 and not batch.full
    batch.add(to_arrow(M, events_for(M, 1, first=5)))
    assert batch.rows == 5 and batch.full


def test_a_batch_closes_a_little_over_the_minimum_because_objects_are_not_split():
    batch = Batch(schema_of(M), min_rows=5)
    batch.add(to_arrow(M, events_for(M, 4)))
    assert not batch.full
    batch.add(to_arrow(M, events_for(M, 3, first=5)))
    assert batch.full and batch.rows == 7
    assert batch.take().num_rows == 7


def test_take_returns_every_row_sorted_and_empties_the_batch():
    batch = Batch(schema_of(M), min_rows=1)
    batch.add(to_arrow(M, events_for(M, 2, first=5)))
    batch.add(to_arrow(M, events_for(M, 2, first=1)))
    taken = batch.take()
    assert [lsn for lsn, _ in keys_of(taken)] == [FIRST_LSN + LSN_STEP * n for n in (1, 2, 5, 6)]
    assert batch.rows == 0 and not batch.full
    with pytest.raises(ValueError, match="empty"):
        batch.take()


def test_a_batch_can_be_filled_again_after_a_take():
    batch = Batch(schema_of(M), min_rows=2)
    batch.add(to_arrow(M, events_for(M, 3)))
    batch.take()
    batch.add(to_arrow(M, events_for(M, 1, first=9)))
    assert batch.rows == 1 and not batch.full
    assert keys_of(batch.take()) == [(FIRST_LSN + LSN_STEP * 9, 0)]


def test_taking_from_an_empty_batch_is_an_error():
    with pytest.raises(ValueError, match="empty"):
        Batch(schema_of(M), min_rows=1).take()


def test_a_batch_of_zero_rows_is_refused():
    for bad in (0, -1):
        with pytest.raises(ValueError, match="at least 1 row"):
            Batch(schema_of(M), min_rows=bad)


def test_a_batch_refuses_another_tables_rows():
    batch = Batch(schema_of(M), min_rows=1)
    with pytest.raises(ValueError, match="one Bronze table"):
        batch.add(to_arrow(T, events_for(T, 1)))
    assert batch.rows == 0


def test_a_batch_refuses_a_table_whose_schema_lacks_the_iceberg_field_ids():
    """The same columns without the field-id metadata are not the table's own Arrow schema."""
    bare = pa.schema([pa.field(f.name, f.type, f.nullable) for f in schema_of(M)])
    batch = Batch(schema_of(M), min_rows=1)
    with pytest.raises(ValueError, match="one Bronze table"):
        batch.add(pa.Table.from_pylist([], schema=bare))
    assert batch.rows == 0


def test_an_object_with_no_rows_adds_nothing():
    batch = Batch(schema_of(M), min_rows=1)
    batch.add(to_arrow(M, []))
    assert batch.rows == 0 and not batch.full


def test_the_batch_concatenates_whole_objects_without_changing_a_value():
    objects = [to_arrow(M, events_for(M, 2, first=first)) for first in (1, 3, 5)]
    batch = Batch(schema_of(M), min_rows=1)
    for obj in objects:
        batch.add(obj)
    taken = batch.take()
    assert sorted(taken.to_pylist(), key=lambda r: r["commit_lsn"]) == [r for obj in objects for r in obj.to_pylist()]


def test_a_rerun_from_differently_cut_objects_gives_the_same_sorted_content():
    """Two consumers cut the same transactions into objects at different times: the sorted batch holds the same
    rows in the same order, apart from the consumer's own clock. Compared as content, not as file bytes."""
    def sorted_rows(cuts):
        land = Landing()
        txs = [land.transaction((M, "c"), (M, "c")) for _ in range(5)]
        for clock, members in cuts:
            land.flush([txs[i] for i in members], clock=clock)
        reader = land.reader()
        batch = Batch(schema_of(M), min_rows=1)
        seen = set()
        for obj in reader.list_keys():
            fresh = [e for e in events_in(reader.read(obj.key))
                     if (e["source"]["commit_lsn"], e["transaction"]["position"]) not in seen]
            seen.update((e["source"]["commit_lsn"], e["transaction"]["position"]) for e in fresh)
            batch.add(to_arrow(M, fresh))
        return [{k: v for k, v in r.items() if k not in ("ts_ms", "ingested_at")} for r in batch.take().to_pylist()]

    one_cut = sorted_rows([(0, [0, 1, 2, 3, 4])])
    assert one_cut == sorted_rows([(5, [0, 1]), (9, [2, 3, 4])]) == sorted_rows([(1, [0]), (2, [1, 2]), (3, [3]), (4, [4])])
    assert len(one_cut) == 10


def test_objects_listed_by_date_still_sort_by_key():
    """A table's objects list by event date first, so they are not in LSN order; the batch still sorts by key."""
    land = Landing()
    for i in range(6):
        land.transaction((T, "c"), (T, "c"), date="2025-03-02" if i % 2 == 0 else "2025-03-01")
    land.flush()
    reader = land.reader()
    batch = Batch(schema_of(T), min_rows=1)
    listed = []
    for obj in reader.list_keys():
        table = to_arrow(T, events_in(reader.read(obj.key)))
        listed += keys_of(table)
        batch.add(table)
    assert listed != sorted(listed)
    assert keys_of(batch.take()) == sorted(listed) and len(listed) == 12
