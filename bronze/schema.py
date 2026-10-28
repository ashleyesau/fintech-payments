"""
schema.py: the Iceberg schema, partition spec and table properties of the five Bronze tables.

Bronze Iceberg Table Contract (v1) sections 3, 4 and 5 as code, generated from the model column lists
(cdc.columns.EXPECTED_COLUMNS and EXPECTED_TYPES) so there is one copy of the table definitions. Nothing
here touches a bucket or a catalog; G3 uses these functions to create tables and to check an existing one.

What is generated (GCS Plan (V2) section 8, DECISION_LOG "G2 Q1" to "G2 Q5 to Q9"):
  - 13 envelope columns, then the table's columns under their raw names, then for loan_applications the
    same columns again with the prefix before_. All 13 envelope columns are required; every table and
    before_ column is optional; flags is a required list of required strings; the source list columns
    are optional lists of optional strings.
  - Integer widths (int against long) come from INTEGER_WIDTHS, because the models call every integer int.
    tests/test_bronze_schema.py checks the table against a blank database built from the migrations.
  - Field ids are natural: top level fields 1 to N in column order, then list element ids N + 1 onward in
    order of appearance. create_table renumbers any other ids, so these are the ids a table will have.
  - One partition field per table: the month transform on event_date, named event_month, id 1000.
  - Table properties: the codec, the landing prefix and source database the table was built from, and the
    contract version. One Bronze warehouse holds one landing prefix and one slot (Q1).
  - A name collision (envelope against source or before_ names, case-insensitive) raises SchemaError when
    a schema is built, not only in a test.
"""
from __future__ import annotations

from collections import defaultdict

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import (BooleanType, DateType, IcebergType, IntegerType, ListType, LongType, NestedField,
                             StringType, TimestamptzType)

from bronze.errors import SchemaError
from cdc import types
from cdc.columns import EXPECTED_COLUMNS, EXPECTED_TYPES

CONTRACT_VERSION = 1

BEFORE_PREFIX = "before_"
# Only loan_applications is ever updated (the consumer stops on any other update), so only it has before_ columns.
BEFORE_TABLES = ("loan_applications",)

PARTITION_FIELD = "event_month"
PARTITION_FIELD_ID = 1000
PARTITION_SOURCE = "event_date"

CODEC_PROPERTY = "write.parquet.compression-codec"
CODEC = "zstd"
PREFIX_PROPERTY = "bronze.source-prefix"
DB_PROPERTY = "bronze.source-db"
VERSION_PROPERTY = "bronze.contract-version"

# The 13 envelope columns in contract section 3 order: (name, type). All required; a list's ids are set in build_schema.
ENVELOPE_COLUMNS: tuple[tuple[str, IcebergType], ...] = (
    ("op", StringType()),
    ("event_date", DateType()),
    ("event_ts", TimestamptzType()),
    ("commit_lsn", LongType()),
    ("change_lsn", LongType()),
    ("commit_ts", TimestamptzType()),
    ("transaction_position", IntegerType()),
    ("transaction_size", IntegerType()),
    ("transaction_xid", LongType()),
    ("ts_ms", LongType()),
    ("ingested_at", TimestamptzType()),
    ("flags", ListType(element_id=0, element_type=StringType(), element_required=True)),
    ("envelope_version", IntegerType()),
)

# Every integer column of the five tables and its width in bits (Q2). The models call both widths int and
# cdc.columns accepts either type OID, so this table is the only place the distinction lives.
INTEGER_WIDTHS: dict[tuple[str, str], int] = {
    ("merchants", "risk_score"): 32,
    ("merchants", "loan_count"): 32,
    ("merchant_transactions", "amount_minor"): 64,
    ("merchant_transactions", "interchange_fee_minor"): 64,
    ("merchant_transactions", "scheme_fee_minor"): 64,
    ("merchant_transactions", "acquirer_markup_minor"): 64,
    ("merchant_transactions", "risk_score"): 32,
    ("loan_applications", "months_trading"): 32,
    ("loan_applications", "declared_monthly_revenue"): 64,
    ("loan_applications", "requested_amount"): 64,
    ("loan_applications", "requested_term_months"): 32,
    ("loan_applications", "owner_credit_score"): 32,
    ("loan_applications", "approved_amount"): 64,
    ("loan_applications", "approved_term_months"): 32,
    ("loan_applications", "disbursement_amount_minor"): 64,
    ("loan_repayments", "installment_number"): 32,
    ("loan_repayments", "principal_due"): 64,
    ("loan_repayments", "interest_due"): 64,
    ("loan_repayments", "total_due"): 64,
    ("loan_repayments", "amount_paid"): 64,
    ("loan_repayments", "outstanding_before"): 64,
    ("loan_repayments", "outstanding_after"): 64,
    ("loan_repayments", "retry_attempt"): 32,
    ("loan_repayments", "days_past_due"): 32,
    ("risk_events", "risk_score"): 32,
    ("risk_events", "risk_score_delta"): 32,
}

_INTEGERS = frozenset({types.INT4, types.INT8})
# Types with a single Postgres type OID. NUMERIC is kept as its exact text (contract section 4).
_BY_OID: dict[int, IcebergType] = {
    types.BOOL: BooleanType(),
    types.TEXT: StringType(),
    types.DATE: DateType(),
    types.TIMESTAMPTZ: TimestamptzType(),
    types.NUMERIC: StringType(),
}


def _column_type(table: str, column: str, oids: frozenset[int]) -> IcebergType:
    """The Iceberg type of one source column. A list column gets an element id of 0 for now."""
    if oids == _INTEGERS:
        width = INTEGER_WIDTHS.get((table, column))
        if width is None:
            raise SchemaError(f"{table}.{column} is an integer column with no entry in INTEGER_WIDTHS: "
                              "add its width (32 or 64) after checking the migration")
        if width == 32:
            return IntegerType()
        if width == 64:
            return LongType()
        raise SchemaError(f"INTEGER_WIDTHS has {width} for {table}.{column}: the width must be 32 or 64")
    if oids == frozenset({types.TEXT_ARRAY}):
        return ListType(element_id=0, element_type=StringType(), element_required=False)
    if len(oids) == 1:
        found = _BY_OID.get(next(iter(oids)))
        if found is not None:
            return found
    raise SchemaError(f"{table}.{column} has no Bronze type for the Postgres type OIDs {sorted(oids)}")


def _check_names(table: str, entries: list[tuple[str, str]]) -> None:
    """Raise SchemaError if two names are equal ignoring case, or a source column starts with before_.

    `entries` is (name, where it comes from). BigQuery, which reads Bronze, treats column names as
    case-insensitive, so the comparison folds case.
    """
    clashes = []
    seen: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for name, origin in entries:
        seen[name.casefold()].append((name, origin))
    for group in seen.values():
        if len(group) > 1:
            clashes.append(" and ".join(f"{name} ({origin})" for name, origin in group))
    for name, origin in entries:
        if origin == "source" and name.casefold().startswith(BEFORE_PREFIX):
            clashes.append(f"{name} (source) starts with {BEFORE_PREFIX}, which is reserved for the before image")
    if clashes:
        raise SchemaError(f"{table}: column names clash: {'; '.join(clashes)}")


def _top_level(table: str) -> list[tuple[str, IcebergType, bool]]:
    """(name, type, required) for every top level column of `table`, in order."""
    if table not in EXPECTED_COLUMNS:
        raise SchemaError(f"{table!r} is not one of the five tables: {', '.join(EXPECTED_COLUMNS)}")
    source = [(column, _column_type(table, column, EXPECTED_TYPES[table][column])) for column in EXPECTED_COLUMNS[table]]
    entries = ([(name, "envelope") for name, _ in ENVELOPE_COLUMNS]
               + [(name, "source") for name, _ in source]
               + [(PARTITION_FIELD, "partition field")])
    before = []
    if table in BEFORE_TABLES:
        before = [(BEFORE_PREFIX + name, kind) for name, kind in source]
        entries += [(name, "before image") for name, _ in before]
    _check_names(table, entries)
    return ([(name, kind, True) for name, kind in ENVELOPE_COLUMNS]
            + [(name, kind, False) for name, kind in source]
            + [(name, kind, False) for name, kind in before])


def build_schema(table: str) -> Schema:
    """The Iceberg schema of one Bronze table, with natural field ids."""
    columns = _top_level(table)
    next_element_id = len(columns) + 1
    fields = []
    for field_id, (name, kind, required) in enumerate(columns, start=1):
        if isinstance(kind, ListType):
            kind = ListType(element_id=next_element_id, element_type=kind.element_type,
                            element_required=kind.element_required)
            next_element_id += 1
        fields.append(NestedField(field_id=field_id, name=name, field_type=kind, required=required))
    return Schema(*fields)


def build_partition_spec(schema: Schema) -> PartitionSpec:
    """The month of event_date, as the field event_month with id 1000, for any Bronze table."""
    source = schema.find_field(PARTITION_SOURCE)
    return PartitionSpec(PartitionField(source_id=source.field_id, field_id=PARTITION_FIELD_ID,
                                        transform=MonthTransform(), name=PARTITION_FIELD))


def table_properties(source_prefix: str, source_db: str) -> dict[str, str]:
    """The properties a Bronze table is created with. The prefix and database tie one warehouse to one
    landing prefix and slot; G3 checks them on every run."""
    return {
        CODEC_PROPERTY: CODEC,
        PREFIX_PROPERTY: source_prefix,
        DB_PROPERTY: source_db,
        VERSION_PROPERTY: str(CONTRACT_VERSION),
    }


def _flatten(schema: Schema) -> dict[str, tuple[int, str, bool]]:
    """name -> (field id, type, required) for every field, a list's element as 'name.element'."""
    flat = {}
    for field in schema.fields:
        flat[field.name] = (field.field_id, str(field.field_type), field.required)
        if isinstance(field.field_type, ListType):
            element = field.field_type
            flat[f"{field.name}.element"] = (element.element_id, str(element.element_type), element.element_required)
    return flat


def schema_differences(table: str, existing: Schema, generated: Schema) -> list[str]:
    """How an existing table's schema differs from the generated one, by name, id, type and required flag.
    Empty when they are the same. G3 stops the run with a TableDriftError when this is not empty."""
    have, want = _flatten(existing), _flatten(generated)
    differences = []
    for name in want:
        if name not in have:
            differences.append(f"{table}.{name}: missing from the table")
    for name in have:
        if name not in want:
            differences.append(f"{table}.{name}: in the table but not in the contract")
    for name in want:
        if name not in have:
            continue
        (have_id, have_type, have_required), (want_id, want_type, want_required) = have[name], want[name]
        if have_id != want_id:
            differences.append(f"{table}.{name}: field id is {have_id}, the contract's is {want_id}")
        if have_type != want_type:
            differences.append(f"{table}.{name}: type is {have_type}, the contract's is {want_type}")
        if have_required != want_required:
            differences.append(f"{table}.{name}: {'required' if have_required else 'optional'} in the table, "
                               f"{'required' if want_required else 'optional'} in the contract")
    return differences

