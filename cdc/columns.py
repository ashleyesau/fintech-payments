"""
columns.py: the drift check on Relation messages (decision D6, extended to column types).

There is no schema registry. Each Relation message is compared with the Pydantic model for
that table in simulator/models.py, which tests already hold equal to the migrations: the
column names and their order, and the Postgres type of each column. A mismatch means the
schema changed under the consumer, so it stops without confirming (contract section 8).

The type check closes a gap the name check leaves: a column that changes to another type the
contract maps (TEXT to INTEGER, say) decodes without error but changes that field's JSON
type. A column that changes to a type the contract does not map already stops at the Relation
message (UnknownTypeError). INTEGER and BIGINT are interchangeable: both are JSON integers.
"""
from __future__ import annotations

import types as pytypes
import typing
from datetime import date, datetime

from cdc import types
from cdc.errors import SchemaDriftError
from cdc.pgoutput import Relation
from simulator.db import STREAMS

SCHEMA = "public"

# Table name -> column names in table order. The models list their fields in table order, so
# the contract's "keys in the table's column order" holds for the model and for the table.
EXPECTED_COLUMNS: dict[str, tuple[str, ...]] = {
    stream.table: tuple(stream.model.model_fields) for stream in STREAMS.values()
}


_TYPE_NAMES = {
    types.BOOL: "boolean", types.INT8: "bigint", types.INT4: "integer", types.TEXT: "text",
    types.DATE: "date", types.TIMESTAMPTZ: "timestamptz", types.NUMERIC: "numeric",
    types.TEXT_ARRAY: "text[]",
}
_INTEGERS = frozenset({types.INT4, types.INT8})


def _oids_for(annotation) -> frozenset[int]:
    """The Postgres type OIDs a model field's annotation can be stored in."""
    origin = typing.get_origin(annotation)
    if origin is typing.Union or origin is pytypes.UnionType:
        present = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(present) != 1:
            raise TypeError(f"no column type for the union {annotation!r}")
        return _oids_for(present[0])
    if origin is typing.Literal:
        kinds = {type(v) for v in typing.get_args(annotation)}
        if kinds == {str}:
            return frozenset({types.TEXT})
        if kinds == {int}:
            return _INTEGERS
        raise TypeError(f"no column type for the literal {annotation!r}")
    if origin is list and typing.get_args(annotation) == (str,):
        return frozenset({types.TEXT_ARRAY})
    simple = {str: {types.TEXT}, bool: {types.BOOL}, int: _INTEGERS, datetime: {types.TIMESTAMPTZ},
              date: {types.DATE}, float: {types.NUMERIC}}  # the model's float is a NUMERIC column
    if annotation in simple:
        return frozenset(simple[annotation])
    raise TypeError(f"no column type for the annotation {annotation!r}")


# Table -> column -> the OIDs the column may have. Built at import, so a model field whose
# annotation cannot be mapped fails here rather than being skipped.
EXPECTED_TYPES: dict[str, dict[str, frozenset[int]]] = {
    stream.table: {name: _oids_for(field.annotation) for name, field in stream.model.model_fields.items()}
    for stream in STREAMS.values()
}


def _type_name(oid: int) -> str:
    return _TYPE_NAMES.get(oid, f"OID {oid}")


def check_relation(relation: Relation) -> None:
    """Raise SchemaDriftError unless the relation is one of the five tables with exactly the
    model's columns, in the model's order."""
    if relation.schema != SCHEMA:
        raise SchemaDriftError(f"{relation.schema}.{relation.name} is not in the {SCHEMA} schema")
    expected = EXPECTED_COLUMNS.get(relation.name)
    if expected is None:
        raise SchemaDriftError(f"{relation.schema}.{relation.name} is not one of the five tables: {', '.join(EXPECTED_COLUMNS)}")
    actual = tuple(c.name for c in relation.columns)
    if actual == expected:
        _check_types(relation)
        return
    extra = [c for c in actual if c not in expected]
    missing = [c for c in expected if c not in actual]
    detail = []
    if extra:
        detail.append(f"in the database but not the model: {', '.join(extra)}")
    if missing:
        detail.append(f"in the model but not the database: {', '.join(missing)}")
    if not detail:
        detail.append("the same columns in a different order")
    raise SchemaDriftError(
        f"{relation.name}: columns differ from the model ({'; '.join(detail)}). "
        "A schema change needs a new migration, an updated model and a deliberate decision."
    )


def _check_types(relation: Relation) -> None:
    allowed = EXPECTED_TYPES[relation.name]
    wrong = [c for c in relation.columns if c.type_oid not in allowed[c.name]]
    if wrong:
        detail = "; ".join(
            f"{c.name} is {_type_name(c.type_oid)}, the model expects {' or '.join(sorted(map(_type_name, allowed[c.name])))}"
            for c in wrong)
        raise SchemaDriftError(
            f"{relation.name}: column types differ from the model ({detail}). "
            "A schema change needs a new migration, an updated model and a deliberate decision."
        )
