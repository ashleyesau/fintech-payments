"""tests/test_bronze_tables.py: G3 step 1, the start-of-run check of an existing Bronze table (bronze/tables.py).

What is held here, each with a mutation that fails it (see the commit message):
  - a table made exactly as the run makes it passes, for all five tables, also after a reload from the catalog;
  - each kind of difference is found and named: schema, partition spec (a different transform, none, a different
    field id or name), each of the four properties (changed or missing), the location;
  - a property nobody asked for is left alone, a trailing slash on the location is ignored;
  - check_table raises TableDriftError (a ContractError), names the table, shows the first ten differences and
    counts the rest, and changes nothing in the table.
Everything is local: a temporary SQLite catalog and warehouse, no bucket.
"""
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, MonthTransform
from pyiceberg.types import NestedField, StringType

from bronze.errors import BronzeError, ContractError, TableDriftError
from bronze.schema import (CODEC_PROPERTY, DB_PROPERTY, PREFIX_PROPERTY, VERSION_PROPERTY, build_partition_spec,
                           build_schema, table_properties)
from bronze.tables import REPORTED, check_table, location_differences, partition_differences, property_differences, \
    table_differences
from cdc.columns import EXPECTED_COLUMNS

TABLES = tuple(EXPECTED_COLUMNS)
PREFIX, DB = "cdc/cdc_r01/", "fintech_cdc"
KWARGS = {"source_prefix": PREFIX, "source_db": DB}


def catalog_in(path) -> SqlCatalog:
    path.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog("bronze", uri=f"sqlite:///{path / 'catalog.db'}", warehouse=f"file://{path / 'warehouse'}")
    catalog.create_namespace("bronze")
    return catalog


def location_of(path, table: str) -> str:
    """Where the run puts a table: <warehouse root>/bronze/<table>."""
    return f"file://{path / 'warehouse'}/bronze/{table}"


def create(path, table, *, schema=None, spec=None, properties=None, location=None):
    """A table as the run creates it, unless an argument changes one part."""
    catalog = catalog_in(path)
    generated = build_schema(table)
    return catalog.create_table(
        f"bronze.{table}", schema=schema or generated,
        partition_spec=build_partition_spec(generated) if spec is None else spec,
        properties=table_properties(PREFIX, DB) if properties is None else properties,
        **({"location": location} if location else {}))


def differences(path, table, loaded) -> list[str]:
    return table_differences(table, loaded, location=location_of(path, table), **KWARGS)


# ---------------------------------------------------------------------------
# a table made exactly as the run makes it
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table", TABLES)
def test_a_table_made_as_the_run_makes_it_has_no_differences(tmp_path, table):
    made = create(tmp_path, table)
    assert differences(tmp_path, table, made) == []
    check_table(table, made, location=location_of(tmp_path, table), **KWARGS)


def test_a_table_loaded_back_from_the_catalog_has_no_differences(tmp_path):
    create(tmp_path, "loan_applications")
    loaded = catalog_in_existing(tmp_path).load_table("bronze.loan_applications")
    assert differences(tmp_path, "loan_applications", loaded) == []


def catalog_in_existing(path) -> SqlCatalog:
    return SqlCatalog("bronze", uri=f"sqlite:///{path / 'catalog.db'}", warehouse=f"file://{path / 'warehouse'}")


# ---------------------------------------------------------------------------
# the schema
# ---------------------------------------------------------------------------

def test_an_extra_column_is_a_difference_and_names_the_column(tmp_path):
    made = create(tmp_path, "risk_events")
    with made.update_schema() as update:
        update.add_column("surprise", StringType())
    assert differences(tmp_path, "risk_events", made) == ["risk_events.surprise: in the table but not in the contract"]


def test_a_missing_column_is_a_difference(tmp_path):
    generated = build_schema("risk_events")
    short = Schema(*[f for f in generated.fields if f.name != "outcome"])
    made = create(tmp_path, "risk_events", schema=short, spec=build_partition_spec(short))
    found = differences(tmp_path, "risk_events", made)
    assert "risk_events.outcome: missing from the table" in found


def test_a_retyped_column_is_a_difference(tmp_path):
    generated = build_schema("risk_events")
    retyped = Schema(*[NestedField(f.field_id, f.name, StringType(), required=f.required)
                       if f.name == "risk_score" else f for f in generated.fields])
    made = create(tmp_path, "risk_events", schema=retyped, spec=build_partition_spec(generated))
    assert differences(tmp_path, "risk_events", made) == [
        "risk_events.risk_score: type is string, the contract's is int"]


# ---------------------------------------------------------------------------
# the partition spec
# ---------------------------------------------------------------------------

def event_date_id(table="risk_events") -> int:
    return build_schema(table).find_field("event_date").field_id


def spec_of(transform, *, field_id=1000, name="event_month", source_id=None):
    return PartitionSpec(PartitionField(source_id=source_id or event_date_id(), field_id=field_id,
                                        transform=transform, name=name))


@pytest.mark.parametrize("spec,says", [
    (spec_of(DayTransform(), name="event_month"),
     "event_month = day(field 2) with id 1000, the contract's is event_month = month(field 2) with id 1000"),
    (spec_of(MonthTransform(), name="month_of_event"),
     "month_of_event = month(field 2) with id 1000, the contract's is event_month = month(field 2) with id 1000"),
    (spec_of(MonthTransform(), source_id=3),
     "event_month = month(field 3) with id 1000, the contract's is event_month = month(field 2) with id 1000"),
])
def test_a_different_partition_spec_is_a_difference(tmp_path, spec, says):
    made = create(tmp_path, "risk_events", spec=spec)
    assert differences(tmp_path, "risk_events", made) == [f"risk_events: the partition spec is {says}"]


def test_an_unpartitioned_table_is_a_difference(tmp_path):
    made = create(tmp_path, "risk_events", spec=UNPARTITIONED_PARTITION_SPEC)
    assert differences(tmp_path, "risk_events", made) == [
        "risk_events: the partition spec is unpartitioned, the contract's is "
        "event_month = month(field 2) with id 1000"]


def test_a_different_partition_field_id_is_a_difference():
    """create_table renumbers a partition field id back to 1000, so a table with another id can only come from
    another writer; the comparison is tested on the specs themselves."""
    generated = build_partition_spec(build_schema("risk_events"))
    assert partition_differences("risk_events", spec_of(MonthTransform(), field_id=1001), generated) == [
        "risk_events: the partition spec is event_month = month(field 2) with id 1001, "
        "the contract's is event_month = month(field 2) with id 1000"]


def test_the_spec_id_alone_is_not_a_difference():
    generated = build_partition_spec(build_schema("risk_events"))
    renumbered = PartitionSpec(*generated.fields, spec_id=7)
    assert partition_differences("risk_events", renumbered, generated) == []


# ---------------------------------------------------------------------------
# the four properties
# ---------------------------------------------------------------------------

FOUR = [CODEC_PROPERTY, PREFIX_PROPERTY, DB_PROPERTY, VERSION_PROPERTY]


@pytest.mark.parametrize("name", FOUR)
def test_a_missing_property_is_a_difference_and_names_the_property(tmp_path, name):
    properties = table_properties(PREFIX, DB)
    del properties[name]
    made = create(tmp_path, "risk_events", properties=properties)
    wanted = table_properties(PREFIX, DB)[name]
    assert differences(tmp_path, "risk_events", made) == [
        f"risk_events: property {name} is missing, the contract's is {wanted!r}"]


@pytest.mark.parametrize("name", FOUR)
def test_a_changed_property_is_a_difference_and_shows_both_values(tmp_path, name):
    properties = table_properties(PREFIX, DB)
    properties[name] = "something-else"
    made = create(tmp_path, "risk_events", properties=properties)
    wanted = table_properties(PREFIX, DB)[name]
    assert differences(tmp_path, "risk_events", made) == [
        f"risk_events: property {name} is 'something-else', the contract's is {wanted!r}"]


def test_a_table_built_from_another_landing_prefix_or_database_is_caught(tmp_path):
    made = create(tmp_path, "risk_events", properties=table_properties("cdc/cdc_r02/", "fintech_other"))
    found = differences(tmp_path, "risk_events", made)
    assert len(found) == 2
    assert any("bronze.source-prefix is 'cdc/cdc_r02/'" in f and "'cdc/cdc_r01/'" in f for f in found)
    assert any("bronze.source-db is 'fintech_other'" in f and "'fintech_cdc'" in f for f in found)


def test_a_property_the_contract_does_not_know_is_left_alone(tmp_path):
    made = create(tmp_path, "risk_events", properties={**table_properties(PREFIX, DB), "owner": "someone"})
    assert differences(tmp_path, "risk_events", made) == []


def test_property_differences_ignore_extras_and_report_in_the_generated_order():
    generated = table_properties(PREFIX, DB)
    assert property_differences("t", {**generated, "x": "y"}, generated) == []
    assert [d.split()[2] for d in property_differences("t", {}, generated)] == list(generated)


# ---------------------------------------------------------------------------
# the location
# ---------------------------------------------------------------------------

def test_a_table_at_another_location_is_a_difference(tmp_path):
    elsewhere = f"file://{tmp_path / 'elsewhere'}/risk_events"
    made = create(tmp_path, "risk_events", location=elsewhere)
    assert differences(tmp_path, "risk_events", made) == [
        f"risk_events: the location is {elsewhere}, the contract's is {location_of(tmp_path, 'risk_events')}"]


def test_a_trailing_slash_on_either_location_is_ignored():
    assert location_differences("t", "gs://b/bronze/t/", "gs://b/bronze/t") == []
    assert location_differences("t", "gs://b/bronze/t", "gs://b/bronze/t/") == []
    assert location_differences("t", "gs://b/bronze/t", "gs://b/bronze/u") == [
        "t: the location is gs://b/bronze/t, the contract's is gs://b/bronze/u"]


def test_a_location_that_only_starts_the_same_is_a_difference():
    assert location_differences("t", "gs://b/bronze/t2", "gs://b/bronze/t")


# ---------------------------------------------------------------------------
# check_table
# ---------------------------------------------------------------------------

def test_check_table_raises_a_contract_error_that_names_the_table_and_the_difference(tmp_path):
    made = create(tmp_path, "risk_events", properties={**table_properties(PREFIX, DB), VERSION_PROPERTY: "2"})
    with pytest.raises(TableDriftError) as raised:
        check_table("risk_events", made, location=location_of(tmp_path, "risk_events"), **KWARGS)
    assert isinstance(raised.value, ContractError) and isinstance(raised.value, BronzeError)
    text = str(raised.value)
    assert "Bronze table risk_events differs from the contract in 1 place" in text
    assert "property bronze.contract-version is '2'" in text
    assert "Nothing was written" in text


def test_check_table_checks_the_schema_the_spec_the_properties_and_the_location_together(tmp_path):
    elsewhere = f"file://{tmp_path / 'elsewhere'}/risk_events"
    properties = {**table_properties(PREFIX, DB), CODEC_PROPERTY: "snappy"}
    made = create(tmp_path, "risk_events", spec=UNPARTITIONED_PARTITION_SPEC, properties=properties,
                  location=elsewhere)
    with made.update_schema() as update:
        update.add_column("surprise", StringType())
    found = differences(tmp_path, "risk_events", made)
    assert len(found) == 4
    assert found[0].startswith("risk_events.surprise: ")
    assert found[1].startswith("risk_events: the partition spec is ")
    assert found[2].startswith("risk_events: property write.parquet.compression-codec is 'snappy'")
    assert found[3].startswith("risk_events: the location is ")
    with pytest.raises(TableDriftError) as raised:
        check_table("risk_events", made, location=location_of(tmp_path, "risk_events"), **KWARGS)
    assert "in 4 places" in str(raised.value)


def test_check_table_shows_the_first_ten_differences_and_counts_the_rest(tmp_path):
    generated = build_schema("merchants")
    envelope_only = Schema(*generated.fields[:13])
    made = create(tmp_path, "merchants", schema=envelope_only, spec=build_partition_spec(envelope_only))
    found = differences(tmp_path, "merchants", made)
    assert len(found) > REPORTED
    with pytest.raises(TableDriftError) as raised:
        check_table("merchants", made, location=location_of(tmp_path, "merchants"), **KWARGS)
    text = str(raised.value)
    assert f"in {len(found)} places (and {len(found) - REPORTED} more)" in text
    assert all(d in text for d in found[:REPORTED])
    assert found[REPORTED] not in text


@pytest.mark.parametrize("kept,count,tail", [(15, REPORTED, ""), (14, REPORTED + 1, " (and 1 more)"),
                                             (16, REPORTED - 1, "")])
def test_the_more_count_appears_only_past_the_limit(tmp_path, kept, count, tail):
    """merchants cut to its first `kept` columns differs from the contract in `count` places: the 11 source
    columns it lacks, less the ones kept, and the list element id of flags, which create_table renumbers."""
    generated = build_schema("merchants")
    cut = Schema(*generated.fields[:kept])
    made = create(tmp_path, "merchants", schema=cut, spec=build_partition_spec(cut))
    assert len(differences(tmp_path, "merchants", made)) == count
    with pytest.raises(TableDriftError) as raised:
        check_table("merchants", made, location=location_of(tmp_path, "merchants"), **KWARGS)
    assert f"in {count} places{tail}: " in str(raised.value)


def test_check_table_does_not_change_the_table(tmp_path):
    made = create(tmp_path, "risk_events", spec=UNPARTITIONED_PARTITION_SPEC)
    before = made.metadata_location
    with pytest.raises(TableDriftError):
        check_table("risk_events", made, location=location_of(tmp_path, "risk_events"), **KWARGS)
    assert made.metadata_location == before
    assert catalog_in_existing(tmp_path).load_table("bronze.risk_events").metadata_location == before


def test_one_difference_is_worded_in_the_singular(tmp_path):
    made = create(tmp_path, "risk_events", properties={**table_properties(PREFIX, DB), DB_PROPERTY: "other"})
    with pytest.raises(TableDriftError, match="differs from the contract in 1 place:"):
        check_table("risk_events", made, location=location_of(tmp_path, "risk_events"), **KWARGS)
