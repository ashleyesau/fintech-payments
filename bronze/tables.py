"""
tables.py: the start-of-run check of an existing Bronze table (GCS Plan (V2) section 19, G3 question 3).

A run loads each of the five tables that already exist and compares it with what bronze.schema generates:
the schema (field ids, types, required flags), the partition spec, the four table properties and the
location. Any difference raises TableDriftError, before pass 1 and before any write, so the run never
appends to a table that has drifted from the contract. A table that does not exist yet is not checked here:
the run creates it after pass 1, with exactly the generated schema, spec and properties.

What is checked, and what is deliberately not:
  - The schema is compared by name, field id, type and required flag (bronze.schema.schema_differences), which
    also pins the column order, because the field ids are natural.
  - The partition spec is compared by source field, field id, transform and name. The spec id is not compared:
    two specs with the same fields write the same data.
  - The four properties (codec, source prefix, source db, contract version) must be present with the generated
    values. A property this code does not know is left alone: PyIceberg or an operator may add one, and it does
    not change what is written. The codec matters because it is read at write time.
  - The location must equal <warehouse root>/bronze/<table> (contract section 2), ignoring a trailing slash.
    The caller passes the expected location, which it builds from the warehouse root it created the catalog with,
    so the same check works on gs://fintech_bronze and on a local test warehouse.

Nothing here writes, and nothing here touches a bucket: the table object is already loaded.
"""
from __future__ import annotations

from typing import Mapping

from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table import Table

from bronze.errors import TableDriftError
from bronze.schema import build_partition_spec, build_schema, schema_differences, table_properties

REPORTED = 10   # differences named in the error message


def _describe_spec(fields: list[tuple]) -> str:
    if not fields:
        return "unpartitioned"
    return ", ".join(f"{name} = {transform}(field {source_id}) with id {field_id}"
                     for source_id, field_id, transform, name in fields)


def _spec_fields(spec: PartitionSpec) -> list[tuple]:
    return [(field.source_id, field.field_id, str(field.transform), field.name) for field in spec.fields]


def partition_differences(table: str, existing: PartitionSpec, generated: PartitionSpec) -> list[str]:
    """How an existing partition spec differs from the generated one. Empty when the fields are the same."""
    have, want = _spec_fields(existing), _spec_fields(generated)
    if have == want:
        return []
    return [f"{table}: the partition spec is {_describe_spec(have)}, the contract's is {_describe_spec(want)}"]


def property_differences(table: str, existing: Mapping[str, str], generated: Mapping[str, str]) -> list[str]:
    """How the table's properties differ from the generated four. Properties that are not among the four are
    ignored."""
    differences = []
    for name, want in generated.items():
        if name not in existing:
            differences.append(f"{table}: property {name} is missing, the contract's is {want!r}")
        elif existing[name] != want:
            differences.append(f"{table}: property {name} is {existing[name]!r}, the contract's is {want!r}")
    return differences


def location_differences(table: str, existing: str, expected: str) -> list[str]:
    if existing.rstrip("/") == expected.rstrip("/"):
        return []
    return [f"{table}: the location is {existing}, the contract's is {expected}"]


def table_differences(name: str, table: Table, *, source_prefix: str, source_db: str, location: str) -> list[str]:
    """Every way the loaded table `name` differs from the contract, in a fixed order: schema, partition spec,
    properties, location. Empty when it is exactly what the run would have created."""
    generated = build_schema(name)
    return (schema_differences(name, table.schema(), generated)
            + partition_differences(name, table.spec(), build_partition_spec(generated))
            + property_differences(name, table.properties, table_properties(source_prefix, source_db))
            + location_differences(name, table.location(), location))


def check_table(name: str, table: Table, *, source_prefix: str, source_db: str, location: str) -> None:
    """Raise TableDriftError unless the loaded table is exactly what the contract generates."""
    differences = table_differences(name, table, source_prefix=source_prefix, source_db=source_db, location=location)
    if not differences:
        return
    more = f" (and {len(differences) - REPORTED} more)" if len(differences) > REPORTED else ""
    raise TableDriftError(
        f"Bronze table {name} differs from the contract in {len(differences)} "
        f"{'place' if len(differences) == 1 else 'places'}{more}: " + "; ".join(differences[:REPORTED]) +
        ". Nothing was written. A Bronze table is never changed in place: this needs a new contract version "
        "or a rebuilt warehouse.")
