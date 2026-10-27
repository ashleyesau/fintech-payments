"""
columns.py: the drift check on Relation messages (decision D6).

There is no schema registry. Each Relation message is compared with the column list of the
Pydantic model for that table in simulator/models.py, which tests already hold equal to the
migrations. A mismatch means the schema changed under the consumer, so it stops without
confirming (contract section 8).
"""
from __future__ import annotations

from cdc.errors import SchemaDriftError
from cdc.pgoutput import Relation
from simulator.db import STREAMS

SCHEMA = "public"

# Table name -> column names in table order. The models list their fields in table order, so
# the contract's "keys in the table's column order" holds for the model and for the table.
EXPECTED_COLUMNS: dict[str, tuple[str, ...]] = {
    stream.table: tuple(stream.model.model_fields) for stream in STREAMS.values()
}


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
