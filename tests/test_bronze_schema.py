"""tests/test_bronze_schema.py: G2 step 1, the Bronze schemas, partition spec and table properties (bronze/schema.py).

What is held here, each with a mutation that fails it (see the commit message):
  - the generated schema equals the contract's column list (tests/bronze_contract.py is a hand-typed copy);
  - the integer widths, against the models and against a blank database built from the migrations;
  - natural field ids, nullability, the month partition spec, the table properties;
  - the collision guard, with planted names;
  - the schema difference function;
  - a real local create_table that returns exactly the generated schema, and a local write that proves the
    required flags and the month partitions do what the contract says.
Everything is local: a temporary SQLite catalog and warehouse, no bucket.
"""
import datetime
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, MonthTransform
from pyiceberg.types import IntegerType, ListType, LongType, NestedField, StringType

from bronze import schema as bronze_schema
from bronze.errors import SchemaError
from bronze.schema import (BEFORE_PREFIX, CONTRACT_VERSION, ENVELOPE_COLUMNS, INTEGER_WIDTHS, build_partition_spec,
                           build_schema, schema_differences, table_properties)
from cdc import types
from cdc.columns import EXPECTED_COLUMNS, EXPECTED_TYPES
from tests import bronze_contract as contract
from tests.helpers import require_postgres

ROOT = Path(__file__).resolve().parent.parent
TABLES = tuple(EXPECTED_COLUMNS)
WORDS = {"string": "string", "int": "int", "long": "long", "boolean": "boolean", "date": "date",
         "timestamptz": "timestamptz", "list of string": "list<string>"}


def expected_columns(table: str) -> list[tuple[str, str, bool]]:
    """(name, iceberg type text, required) for every top level column the contract promises, in order."""
    columns = [(name, WORDS[kind], True) for name, kind in contract.ENVELOPE]
    columns += [(name, WORDS[kind], False) for name, kind in contract.TABLES[table]]
    if table in contract.BEFORE_TABLES:
        columns += [(contract.BEFORE_PREFIX + name, WORDS[kind], False) for name, kind in contract.TABLES[table]]
    return columns


# ---------------------------------------------------------------------------
# the generated schema equals the contract
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table", TABLES)
def test_the_generated_schema_equals_the_contract_column_for_column(table):
    generated = [(f.name, str(f.field_type), f.required) for f in build_schema(table).fields]
    assert generated == expected_columns(table)


def test_the_contract_fixture_has_the_column_counts_the_contract_states():
    assert {t: len(c) for t, c in contract.TABLES.items()} == contract.COLUMN_COUNTS
    assert len(contract.ENVELOPE) == 13


@pytest.mark.parametrize("table", TABLES)
def test_the_contract_fixture_and_the_models_list_the_same_columns(table):
    assert tuple(name for name, _ in contract.TABLES[table]) == EXPECTED_COLUMNS[table]


@pytest.mark.parametrize("table,total", [("merchants", 24), ("merchant_transactions", 45), ("loan_applications", 61),
                                         ("loan_repayments", 35), ("risk_events", 26)])
def test_the_column_totals(table, total):
    assert len(build_schema(table).fields) == total


def test_only_loan_applications_has_before_columns_and_there_are_24_of_them():
    for table in TABLES:
        names = [f.name for f in build_schema(table).fields]
        before = [n for n in names if n.startswith(BEFORE_PREFIX)]
        assert len(before) == (24 if table == "loan_applications" else 0)


def test_transaction_xid_is_the_envelope_name_and_transaction_id_is_left_to_the_source_column():
    names = [f.name for f in build_schema("merchant_transactions").fields]
    assert names.count("transaction_id") == 1 and names.count("transaction_xid") == 1
    assert "transaction_id" not in [n for n, _ in ENVELOPE_COLUMNS]


def test_source_db_schema_and_table_are_not_columns():
    for table in TABLES:
        names = {f.name for f in build_schema(table).fields}
        assert not names & {"db", "schema", "table", "source_db", "source_schema", "source_table"}


def test_an_unknown_table_is_refused():
    with pytest.raises(SchemaError, match="not one of the five tables"):
        build_schema("transactions")


# ---------------------------------------------------------------------------
# nullability (Q3)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table", TABLES)
def test_exactly_the_thirteen_envelope_columns_are_required(table):
    required = [f.name for f in build_schema(table).fields if f.required]
    assert required == [name for name, _ in contract.ENVELOPE]


def test_flags_is_a_required_list_of_required_strings():
    flags = build_schema("merchants").find_field("flags")
    assert flags.required and isinstance(flags.field_type, ListType)
    assert flags.field_type.element_required and isinstance(flags.field_type.element_type, StringType)


def test_decline_reasons_and_its_before_copy_are_optional_lists_of_optional_strings():
    schema = build_schema("loan_applications")
    for name in ("decline_reasons", "before_decline_reasons"):
        field = schema.find_field(name)
        assert not field.required and isinstance(field.field_type, ListType)
        assert not field.field_type.element_required and isinstance(field.field_type.element_type, StringType)


@pytest.mark.parametrize("table", TABLES)
def test_every_table_column_and_every_before_column_is_optional(table):
    envelope = {name for name, _ in contract.ENVELOPE}
    assert all(not f.required for f in build_schema(table).fields if f.name not in envelope)


# ---------------------------------------------------------------------------
# natural field ids (Q5)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table", TABLES)
def test_top_level_ids_run_from_1_in_column_order(table):
    fields = build_schema(table).fields
    assert [f.field_id for f in fields] == list(range(1, len(fields) + 1))


@pytest.mark.parametrize("table,elements", [
    ("merchants", {"flags": 25}),
    ("merchant_transactions", {"flags": 46}),
    ("risk_events", {"flags": 27}),
    ("loan_repayments", {"flags": 36}),
    ("loan_applications", {"flags": 62, "decline_reasons": 63, "before_decline_reasons": 64}),
])
def test_list_element_ids_follow_the_top_level_ids_in_order_of_appearance(table, elements):
    schema = build_schema(table)
    found = {f.name: f.field_type.element_id for f in schema.fields if isinstance(f.field_type, ListType)}
    assert found == elements


def test_event_date_is_field_2_in_every_table():
    assert all(build_schema(t).find_field("event_date").field_id == 2 for t in TABLES)


# ---------------------------------------------------------------------------
# integer widths (Q2)
# ---------------------------------------------------------------------------

def integer_columns() -> set[tuple[str, str]]:
    """Every column the models can store as an integer: the ones whose type OIDs are INT4 and INT8."""
    both = frozenset({types.INT4, types.INT8})
    return {(t, c) for t, cols in EXPECTED_TYPES.items() for c, oids in cols.items() if oids == both}


def test_the_width_table_has_26_entries_12_int_and_14_long():
    assert len(INTEGER_WIDTHS) == 26
    assert Counter(INTEGER_WIDTHS.values()) == {32: 12, 64: 14}


def test_the_width_table_per_table_counts():
    per_table = Counter((t, w) for (t, _), w in INTEGER_WIDTHS.items())
    assert per_table == {("merchants", 32): 2, ("merchant_transactions", 32): 1, ("merchant_transactions", 64): 4,
                         ("loan_applications", 32): 4, ("loan_applications", 64): 4, ("loan_repayments", 32): 3,
                         ("loan_repayments", 64): 6, ("risk_events", 32): 2}


def test_the_width_table_names_every_integer_column_of_the_models_and_nothing_else():
    assert set(INTEGER_WIDTHS) == integer_columns()


def test_widths_are_32_or_64():
    assert set(INTEGER_WIDTHS.values()) <= {32, 64}


def test_the_generated_types_follow_the_width_table():
    for (table, column), width in INTEGER_WIDTHS.items():
        schema = build_schema(table)
        expected = IntegerType() if width == 32 else LongType()
        assert schema.find_field(column).field_type == expected
        if table in contract.BEFORE_TABLES:
            assert schema.find_field(BEFORE_PREFIX + column).field_type == expected


def test_an_integer_column_with_no_entry_is_refused_and_named(monkeypatch):
    monkeypatch.delitem(INTEGER_WIDTHS, ("merchants", "loan_count"))
    with pytest.raises(SchemaError, match="merchants.loan_count.*no entry in INTEGER_WIDTHS"):
        build_schema("merchants")


@pytest.mark.parametrize("width", [16, 0, None, 32.0 + 1])
def test_a_width_that_is_not_32_or_64_is_refused(monkeypatch, width):
    monkeypatch.setitem(INTEGER_WIDTHS, ("merchants", "loan_count"), width)
    with pytest.raises(SchemaError, match="merchants.loan_count"):
        build_schema("merchants")


def test_a_column_type_with_no_mapping_is_refused(monkeypatch):
    monkeypatch.setitem(EXPECTED_TYPES["merchants"], "province", frozenset({types.TEXT, types.BOOL}))
    with pytest.raises(SchemaError, match="merchants.province has no Bronze type"):
        build_schema("merchants")


def test_the_width_table_equals_a_blank_database_built_from_the_migrations(blank_dsn):
    """Both directions: every table entry matches the column's type, and every integer or bigint column of
    the database has an entry. This is what stops a migration from changing a width unnoticed."""
    with psycopg.connect(blank_dsn) as conn:
        rows = conn.execute(
            "SELECT table_name, column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' AND data_type IN ('integer', 'bigint')").fetchall()
    in_database = {(table, column): 32 if kind == "integer" else 64 for table, column, kind in rows}
    assert in_database == INTEGER_WIDTHS


def unreachable():
    raise psycopg.OperationalError("connection refused")


def test_the_database_guard_skips_locally_when_postgres_is_unreachable():
    with pytest.raises(pytest.skip.Exception):
        require_postgres(unreachable, ci=False)


def test_the_database_guard_fails_under_ci_when_postgres_is_unreachable():
    with pytest.raises(pytest.fail.Exception, match="CI is set"):
        require_postgres(unreachable, ci=True)


def test_the_database_guard_returns_the_connection_when_it_is_reachable():
    sentinel = object()
    assert require_postgres(lambda: sentinel, ci=True) is sentinel


GUARD = "tests/test_bronze_schema.py::test_the_width_table_equals_a_blank_database_built_from_the_migrations"


def run_guard_with_postgres_down(ci: str | None) -> subprocess.CompletedProcess:
    """Run the width guard in a nested pytest whose Postgres port has nothing listening on it."""
    env = {k: v for k, v in os.environ.items() if k != "CI"}
    env.update(POSTGRES_HOST="127.0.0.1", POSTGRES_PORT="1")
    if ci is not None:
        env["CI"] = ci
    return subprocess.run([sys.executable, "-m", "pytest", GUARD, "-q", "-rs", "-p", "no:cacheprovider",
                           "-W", "ignore::FutureWarning"], cwd=ROOT, env=env, capture_output=True, text=True, timeout=120)


def test_the_real_width_guard_fails_under_ci_when_postgres_is_down():
    run = run_guard_with_postgres_down("true")
    assert run.returncode != 0 and "CI is set" in run.stdout and "1 failed" in run.stdout and "skipped" not in run.stdout


def test_the_real_width_guard_skips_locally_when_postgres_is_down():
    run = run_guard_with_postgres_down(None)
    assert run.returncode == 0 and "1 skipped" in run.stdout and "Postgres not reachable" in run.stdout


# ---------------------------------------------------------------------------
# the partition spec (Q6)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table", TABLES)
def test_one_partition_field_the_month_of_event_date_named_event_month_id_1000(table):
    spec = build_partition_spec(build_schema(table))
    assert len(spec.fields) == 1
    field = spec.fields[0]
    assert (field.name, field.field_id, field.source_id) == ("event_month", 1000, 2)
    assert field.transform == MonthTransform() and field.transform != DayTransform()


def test_the_spec_is_the_same_for_all_five_tables():
    specs = {build_partition_spec(build_schema(t)) for t in TABLES}
    assert len(specs) == 1


# ---------------------------------------------------------------------------
# table properties (Q1, Q5)
# ---------------------------------------------------------------------------

def test_the_table_properties_and_their_names_are_pinned():
    assert table_properties("cdc/cdc_r01/", "fintech_cdc") == {
        "write.parquet.compression-codec": "zstd",
        "bronze.source-prefix": "cdc/cdc_r01/",
        "bronze.source-db": "fintech_cdc",
        "bronze.contract-version": "1",
    }
    assert CONTRACT_VERSION == 1


def test_the_properties_carry_the_values_they_are_given():
    properties = table_properties("cdc/cdc_r02/", "other_db")
    assert properties["bronze.source-prefix"] == "cdc/cdc_r02/" and properties["bronze.source-db"] == "other_db"


# ---------------------------------------------------------------------------
# the collision guard (Q8), with planted names
# ---------------------------------------------------------------------------

def plant(monkeypatch, table, column, oids=frozenset({types.TEXT})):
    monkeypatch.setitem(EXPECTED_COLUMNS, table, (*EXPECTED_COLUMNS[table], column))
    monkeypatch.setitem(EXPECTED_TYPES[table], column, oids)


def test_the_real_five_tables_pass_the_guard():
    for table in TABLES:
        build_schema(table)


@pytest.mark.parametrize("name", ["op", "OP", "Op", "commit_lsn", "COMMIT_LSN", "flags", "transaction_xid", "envelope_version"])
def test_a_source_column_named_like_an_envelope_column_is_refused_in_any_case(monkeypatch, name):
    plant(monkeypatch, "merchants", name)
    with pytest.raises(SchemaError, match=f"merchants: column names clash.*{name}"):
        build_schema("merchants")


@pytest.mark.parametrize("name", ["event_month", "EVENT_MONTH"])
def test_a_source_column_named_like_the_partition_field_is_refused(monkeypatch, name):
    plant(monkeypatch, "risk_events", name)
    with pytest.raises(SchemaError, match="partition field"):
        build_schema("risk_events")


def test_a_source_column_that_starts_with_before_is_refused_even_without_a_clash(monkeypatch):
    plant(monkeypatch, "merchants", "before_note")
    with pytest.raises(SchemaError, match="before_note.*starts with before_"):
        build_schema("merchants")


def test_the_before_prefix_check_ignores_case(monkeypatch):
    plant(monkeypatch, "merchants", "BEFORE_note")
    with pytest.raises(SchemaError, match="starts with before_"):
        build_schema("merchants")


def test_a_source_column_that_clashes_with_a_before_copy_is_refused(monkeypatch):
    plant(monkeypatch, "loan_applications", "before_merchant_id")
    with pytest.raises(SchemaError, match="before_merchant_id \\(source\\) and before_merchant_id \\(before image\\)"):
        build_schema("loan_applications")


def test_an_envelope_column_that_clashes_with_a_before_copy_is_refused(monkeypatch):
    monkeypatch.setattr(bronze_schema, "ENVELOPE_COLUMNS", (*ENVELOPE_COLUMNS, ("before_merchant_id", StringType())))
    with pytest.raises(SchemaError, match="before_merchant_id \\(envelope\\) and before_merchant_id \\(before image\\)"):
        build_schema("loan_applications")


def test_a_column_listed_twice_in_a_table_is_refused(monkeypatch):
    monkeypatch.setitem(EXPECTED_COLUMNS, "risk_events", (*EXPECTED_COLUMNS["risk_events"], "alert_id"))
    with pytest.raises(SchemaError, match="alert_id \\(source\\) and alert_id \\(source\\)"):
        build_schema("risk_events")


def test_a_clash_error_lists_every_clash(monkeypatch):
    plant(monkeypatch, "merchants", "op")
    plant(monkeypatch, "merchants", "TS_MS")
    with pytest.raises(SchemaError) as caught:
        build_schema("merchants")
    assert "op (envelope) and op (source)" in str(caught.value)
    assert "ts_ms (envelope) and TS_MS (source)" in str(caught.value)


# ---------------------------------------------------------------------------
# the schema difference function (Q5)
# ---------------------------------------------------------------------------

def rebuilt(schema: Schema, change) -> Schema:
    """A copy of `schema` with every NestedField passed through change(field) (None drops it)."""
    fields = [new for new in (change(f) for f in schema.fields) if new is not None]
    return Schema(*fields)


def field_copy(f, **changes):
    values = dict(field_id=f.field_id, name=f.name, field_type=f.field_type, required=f.required)
    values.update(changes)
    return NestedField(**values)


@pytest.mark.parametrize("table", TABLES)
def test_identical_schemas_have_no_differences(table):
    assert schema_differences(table, build_schema(table), build_schema(table)) == []


def test_a_missing_column_is_named():
    existing = rebuilt(build_schema("merchants"), lambda f: None if f.name == "province" else f)
    found = schema_differences("merchants", existing, build_schema("merchants"))
    assert any("merchants.province: missing from the table" == d for d in found)


def test_an_extra_column_is_named():
    generated = build_schema("merchants")
    existing = Schema(*generated.fields, NestedField(99, "surprise", StringType(), required=False))
    found = schema_differences("merchants", existing, generated)
    assert found == ["merchants.surprise: in the table but not in the contract"]


def test_a_renamed_column_shows_as_one_missing_and_one_extra():
    existing = rebuilt(build_schema("merchants"), lambda f: field_copy(f, name="state") if f.name == "merchant_state" else f)
    found = schema_differences("merchants", existing, build_schema("merchants"))
    assert "merchants.merchant_state: missing from the table" in found
    assert "merchants.state: in the table but not in the contract" in found
    assert len(found) == 2


def test_a_retyped_column_is_named_with_both_types():
    existing = rebuilt(build_schema("merchants"), lambda f: field_copy(f, field_type=LongType()) if f.name == "risk_score" else f)
    assert schema_differences("merchants", existing, build_schema("merchants")) == [
        "merchants.risk_score: type is long, the contract's is int"]


def test_a_changed_required_flag_is_named_in_both_directions():
    flip = lambda name: (lambda f: field_copy(f, required=not f.required) if f.name == name else f)
    generated = build_schema("merchants")
    assert schema_differences("merchants", rebuilt(generated, flip("op")), generated) == [
        "merchants.op: optional in the table, required in the contract"]
    assert schema_differences("merchants", rebuilt(generated, flip("province")), generated) == [
        "merchants.province: required in the table, optional in the contract"]


def test_a_changed_field_id_is_named():
    existing = rebuilt(build_schema("merchants"), lambda f: field_copy(f, field_id=77) if f.name == "op" else f)
    assert schema_differences("merchants", existing, build_schema("merchants")) == [
        "merchants.op: field id is 77, the contract's is 1"]


def test_a_changed_list_element_is_named():
    def change(f):
        if f.name != "decline_reasons":
            return f
        element = ListType(element_id=63, element_type=StringType(), element_required=True)
        return field_copy(f, field_type=element)
    generated = build_schema("loan_applications")
    assert schema_differences("loan_applications", rebuilt(generated, change), generated) == [
        "loan_applications.decline_reasons.element: required in the table, optional in the contract"]


def test_a_changed_list_element_id_is_named():
    def change(f):
        if f.name != "flags":
            return f
        return field_copy(f, field_type=ListType(element_id=90, element_type=StringType(), element_required=True))
    generated = build_schema("merchants")
    assert schema_differences("merchants", rebuilt(generated, change), generated) == [
        "merchants.flags.element: field id is 90, the contract's is 25"]


# ---------------------------------------------------------------------------
# a real local create_table
# ---------------------------------------------------------------------------

def catalog_in(path) -> SqlCatalog:
    path.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog("bronze", uri=f"sqlite:///{path / 'catalog.db'}", warehouse=f"file://{path / 'warehouse'}")
    catalog.create_namespace("bronze")
    return catalog


def create(catalog, table):
    schema = build_schema(table)
    return catalog.create_table(f"bronze.{table}", schema=schema, partition_spec=build_partition_spec(schema),
                                properties=table_properties("cdc/cdc_r01/", "fintech_cdc"))


@pytest.mark.parametrize("table", TABLES)
def test_create_table_returns_exactly_the_generated_schema_spec_and_properties(tmp_path, table):
    """Every field id, name, type and required flag survives create_table, which renumbers any ids that are
    not the ones it would assign; this also catches a future PyIceberg changing its numbering rule."""
    generated = build_schema(table)
    created = create(catalog_in(tmp_path), table)
    assert created.schema() == generated
    assert schema_differences(table, created.schema(), generated) == []
    assert created.spec() == build_partition_spec(generated)
    for name, value in table_properties("cdc/cdc_r01/", "fintech_cdc").items():
        assert created.properties[name] == value


def test_the_table_loaded_back_from_the_catalog_is_the_same(tmp_path):
    catalog = catalog_in(tmp_path)
    create(catalog, "loan_applications")
    loaded = catalog.load_table("bronze.loan_applications")
    assert loaded.schema() == build_schema("loan_applications")
    assert loaded.spec() == build_partition_spec(build_schema("loan_applications"))


def test_two_fresh_catalogs_give_identical_schemas(tmp_path):
    first = create(catalog_in(tmp_path / "one"), "loan_applications")
    second = create(catalog_in(tmp_path / "two"), "loan_applications")
    assert first.schema() == second.schema() and first.spec() == second.spec()


# ---------------------------------------------------------------------------
# a local write: required flags and month partitions do what the contract says
# ---------------------------------------------------------------------------

DEFAULTS = {"string": "x", "int": 1, "long": 1, "boolean": True, "date": datetime.date(2025, 1, 15),
            "timestamptz": datetime.datetime(2025, 1, 15, 8, 0, tzinfo=datetime.timezone.utc)}


def rows(schema: Schema, count: int = 1, **columns) -> pa.Table:
    """An Arrow table in the Iceberg schema's own Arrow form (required columns non-nullable). Required
    columns get a default and optional ones null unless `columns` gives a list of `count` values."""
    arrow = schema_to_pyarrow(schema)
    arrays = []
    for field in schema.fields:
        if field.name in columns:
            values = columns[field.name]
        elif not field.required:
            values = [None] * count
        elif isinstance(field.field_type, ListType):
            values = [["noop_update"]] * count
        else:
            values = [DEFAULTS[str(field.field_type)]] * count
        arrays.append(pa.array(values, type=arrow.field(field.name).type))
    return pa.Table.from_arrays(arrays, schema=arrow)


@pytest.mark.parametrize("table", TABLES)
def test_the_arrow_form_of_the_schema_is_non_nullable_exactly_where_the_contract_says_required(table):
    schema = build_schema(table)
    arrow = schema_to_pyarrow(schema)
    assert [f.name for f in arrow if not f.nullable] == [name for name, _ in contract.ENVELOPE]


def test_a_null_in_a_required_envelope_column_is_rejected_at_write(tmp_path):
    table = create(catalog_in(tmp_path), "merchants")
    bad = rows(table.schema(), op=["c"]).to_pylist()
    bad[0]["op"] = None
    arrow = schema_to_pyarrow(table.schema())
    with pytest.raises((pa.ArrowInvalid, ValueError)):
        table.append(pa.Table.from_pylist(bad, schema=arrow))


def test_a_null_in_every_optional_column_is_accepted_and_read_back_as_null(tmp_path):
    table = create(catalog_in(tmp_path), "merchants")
    table.append(rows(table.schema()))
    back = table.scan().to_arrow().to_pylist()[0]
    optional = [f.name for f in table.schema().fields if not f.required]
    assert all(back[name] is None for name in optional)


def test_a_null_list_and_a_null_element_survive_in_decline_reasons(tmp_path):
    table = create(catalog_in(tmp_path), "loan_applications")
    table.append(rows(table.schema(), 2, decline_reasons=[None, ["a", None, "b"]]))
    back = sorted(table.scan().to_arrow().column("decline_reasons").to_pylist(), key=lambda v: (v is not None, v))
    assert back == [None, ["a", None, "b"]]


def test_a_long_column_written_to_an_int_field_is_rejected_which_is_why_the_widths_matter(tmp_path):
    table = create(catalog_in(tmp_path), "merchants")
    wide = schema_to_pyarrow(table.schema()).set(
        table.schema().find_field("risk_score").field_id - 1, pa.field("risk_score", pa.int64(), nullable=True))
    values = rows(table.schema()).to_pylist()
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        table.append(pa.Table.from_pylist(values, schema=wide))


def month_files(table) -> dict[int, int]:
    """Months since 1970-01 -> number of data files, from the partition value of each planned file."""
    counts: Counter = Counter()
    for task in table.scan().plan_files():
        counts[task.file.partition[0]] += 1
    return dict(counts)


def test_rows_across_three_months_make_three_files_each_in_the_month_of_event_date(tmp_path):
    table = create(catalog_in(tmp_path), "merchants")
    dates = [datetime.date(2025, 1, 31), datetime.date(2025, 2, 28), datetime.date(2025, 3, 1), datetime.date(2025, 3, 31)]
    table.append(rows(table.schema(), 4, event_date=dates))
    assert month_files(table) == {660: 1, 661: 1, 662: 1}      # January, February and March 2025


def test_a_row_at_22_30_utc_on_the_last_day_of_february_is_in_march_because_event_date_says_march(tmp_path):
    """event_date is the South African date (UTC+2), so event_ts 2025-02-28T22:30Z has event_date 2025-03-01
    and lands in the March partition; the partition follows event_date, not event_ts."""
    table = create(catalog_in(tmp_path), "merchants")
    event_ts = datetime.datetime(2025, 2, 28, 22, 30, tzinfo=datetime.timezone.utc)
    table.append(rows(table.schema(), event_ts=[event_ts], event_date=[datetime.date(2025, 3, 1)]))
    assert month_files(table) == {662: 1}


def test_a_loan_applications_update_with_the_inserts_event_date_shares_its_partition(tmp_path):
    table = create(catalog_in(tmp_path), "loan_applications")
    table.append(rows(table.schema(), 2, op=["c", "u"], event_date=[datetime.date(2025, 6, 30)] * 2))
    assert month_files(table) == {665: 1}


def test_a_file_never_holds_two_months(tmp_path):
    table = create(catalog_in(tmp_path), "merchants")
    dates = [datetime.date(2025, 1, 1), datetime.date(2025, 2, 1)] * 3
    table.append(rows(table.schema(), 6, event_date=dates))
    paths = [task.file.file_path.removeprefix("file://") for task in table.scan().plan_files()]
    assert len(paths) == 2
    for path in paths:
        months = {(d.year, d.month) for d in pq.read_table(path).column("event_date").to_pylist()}
        assert len(months) == 1
