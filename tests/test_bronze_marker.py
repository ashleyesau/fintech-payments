"""tests/test_bronze_marker.py: G3 step 4, the run marker (bronze/marker.py).

What is held here, each with a mutation that fails it (see the commit message):
  - a marker is named for the run's UTC time to the microsecond, encodes to the contract's fields in order and
    parses back to itself; every way the content can be wrong is a MarkerError naming the marker and the field;
  - the newest marker is the last name of one listing, only it is read, an object under _runs/ that is not a marker
    stops the run, and a newest marker that does not parse is never skipped for an older one;
  - a marker is created once and never replaced;
  - "the tables are at the marker" compares every table's snapshot id (None for a table with no snapshot);
  - the catalog must fit the marker: every table it names is there and has the marker's snapshot in its history.
"""
import json
from datetime import datetime, timezone

import pytest

from bronze.batches import arrow_schema_of, events_to_arrow
from bronze.errors import ContractError, MarkerError
from bronze.marker import (TABLES, Marker, TableMark, build_marker, check_against_marker, describes, encode,
                           newest_marker, parse, write_marker)
from bronze.run import TABLE_ORDER
from tests.bronze_env import Env, busy_landing
from tests.bronze_synth import SOURCE_DB, event
from tests.fakes import MemorySink

NOW = datetime(2026, 10, 29, 15, 30, 0, 123456, tzinfo=timezone.utc)
RUN_ID = "20261029T153000123456Z"
KEY = f"_runs/{RUN_ID}.json"


def marks(snapshot=1234, rows=7):
    """A mark for each table: snapshot ids snapshot, snapshot + 1, ... (all None for None), rows 7, 8, ... (all 0 for 0)."""
    return {name: TableMark(snapshot + i if snapshot is not None else None, rows + i if rows else 0,
                            f"gs://fintech_bronze/bronze/{name}/metadata/00001-x.metadata.json")
            for i, name in enumerate(TABLES)}


def a_marker(**kwargs):
    args = dict(landed_objects=908, highest_commit_lsn=0x2B32560, tables=marks())
    args.update(kwargs)
    return build_marker(NOW, **args)


def good() -> dict:
    return json.loads(encode(a_marker()))


def put(store, body, key=KEY):
    store.objects[key] = body if isinstance(body, bytes) else json.dumps(body).encode()


# ---------------------------------------------------------------------------
# building, encoding, parsing
# ---------------------------------------------------------------------------

def test_a_marker_is_named_for_the_utc_time_to_the_microsecond():
    marker = a_marker()
    assert marker.run_id == RUN_ID and marker.written_at == "2026-10-29T15:30:00.123456Z" and marker.key == KEY
    assert marker.bronze_contract_version == 1
    assert build_marker(NOW.replace(microsecond=7), landed_objects=0, highest_commit_lsn=None,
                        tables=marks()).run_id == "20261029T153000000007Z"


def test_the_tables_of_a_marker_are_in_table_order_whatever_order_they_were_given_in():
    reversed_marks = dict(reversed(list(marks().items())))
    assert list(a_marker(tables=reversed_marks).tables) == list(TABLES)
    assert tuple(TABLES) == tuple(TABLE_ORDER) or set(TABLES) == set(TABLE_ORDER)


def test_a_marker_encodes_to_the_contracts_fields_in_order_and_ends_with_a_line_feed():
    data = encode(a_marker())
    assert data.endswith(b"\n") and data.isascii()
    body = json.loads(data)
    assert list(body) == ["run_id", "bronze_contract_version", "written_at", "landed_objects", "highest_commit_lsn", "tables"]
    assert list(body["tables"]) == list(TABLES)
    assert list(body["tables"]["merchants"]) == ["snapshot_id", "rows", "metadata_location"]
    assert body["highest_commit_lsn"] == 0x2B32560 and body["landed_objects"] == 908


def test_a_marker_is_readable_json_one_field_to_a_line():
    lines = encode(a_marker()).decode().splitlines()
    assert lines[0] == "{" and lines[-1] == "}" and '  "run_id": "20261029T153000123456Z",' in lines
    assert len(lines) > 30


def test_a_marker_parses_back_to_itself():
    marker = a_marker()
    assert parse(marker.key, encode(marker)) == marker


def test_a_marker_for_a_landing_with_nothing_in_it_keeps_its_nulls():
    marker = a_marker(landed_objects=0, highest_commit_lsn=None, tables=marks(snapshot=None, rows=0))
    again = parse(marker.key, encode(marker))
    assert again == marker and again.highest_commit_lsn is None
    assert all(t.snapshot_id is None and t.rows == 0 for t in again.tables.values())


def test_the_largest_commit_lsn_is_accepted():
    marker = a_marker(highest_commit_lsn=2 ** 63 - 1)
    assert parse(marker.key, encode(marker)).highest_commit_lsn == 2 ** 63 - 1


def with_change(change):
    body = good()
    change(body)
    return json.dumps(body).encode()


def table(body, name="risk_events"):
    return body["tables"][name]


BAD = {
    "not utf-8": b"\xff\xfe{",
    "not json": b"{not json",
    "a list": b"[]",
    "a number": b"7",
    "an empty object": b"{}",
    "a field missing": with_change(lambda b: b.pop("landed_objects")),
    "a field added": with_change(lambda b: b.update(extra=1)),
    "the run_id of another marker": with_change(lambda b: b.update(run_id="20260101T000000000000Z")),
    "another contract version": with_change(lambda b: b.update(bronze_contract_version=2)),
    "a version that is a string": with_change(lambda b: b.update(bronze_contract_version="1")),
    "a version that is a boolean": with_change(lambda b: b.update(bronze_contract_version=True)),
    "written_at without microseconds": with_change(lambda b: b.update(written_at="2026-10-29T15:30:00Z")),
    "written_at with text after it": with_change(lambda b: b.update(written_at="2026-10-29T15:30:00.123456Zjunk")),
    "written_at with a line feed after it": with_change(lambda b: b.update(written_at="2026-10-29T15:30:00.123456Z\n")),
    "written_at not a string": with_change(lambda b: b.update(written_at=20261029)),
    "landed_objects negative": with_change(lambda b: b.update(landed_objects=-1)),
    "landed_objects a string": with_change(lambda b: b.update(landed_objects="908")),
    "landed_objects a boolean": with_change(lambda b: b.update(landed_objects=True)),
    "landed_objects a float": with_change(lambda b: b.update(landed_objects=908.0)),
    "highest_commit_lsn negative": with_change(lambda b: b.update(highest_commit_lsn=-1)),
    "highest_commit_lsn at 2^63": with_change(lambda b: b.update(highest_commit_lsn=2 ** 63)),
    "highest_commit_lsn above 2^63": with_change(lambda b: b.update(highest_commit_lsn=2 ** 64)),
    "highest_commit_lsn a string": with_change(lambda b: b.update(highest_commit_lsn="0x2b")),
    "highest_commit_lsn a boolean": with_change(lambda b: b.update(highest_commit_lsn=False)),
    "tables not an object": with_change(lambda b: b.update(tables=[])),
    "a table missing": with_change(lambda b: b["tables"].pop("risk_events")),
    "a table added": with_change(lambda b: b["tables"].update(surprise=dict(table(b)))),
    "a table renamed": with_change(lambda b: b["tables"].update(advances=b["tables"].pop("risk_events"))),
    "a table entry not an object": with_change(lambda b: b["tables"].update(risk_events=5)),
    "a table field missing": with_change(lambda b: table(b).pop("rows")),
    "a table field added": with_change(lambda b: table(b).update(size=1)),
    "snapshot_id a string": with_change(lambda b: table(b).update(snapshot_id="12")),
    "snapshot_id a boolean": with_change(lambda b: table(b).update(snapshot_id=True)),
    "snapshot_id negative": with_change(lambda b: table(b).update(snapshot_id=-5)),
    "rows negative": with_change(lambda b: table(b).update(rows=-1)),
    "rows a string": with_change(lambda b: table(b).update(rows="7")),
    "rows a boolean": with_change(lambda b: table(b).update(rows=True)),
    "rows null": with_change(lambda b: table(b).update(rows=None)),
    "metadata_location empty": with_change(lambda b: table(b).update(metadata_location="")),
    "metadata_location a number": with_change(lambda b: table(b).update(metadata_location=7)),
    "metadata_location null": with_change(lambda b: table(b).update(metadata_location=None)),
}


@pytest.mark.parametrize("what", list(BAD))
def test_a_marker_that_breaks_the_contract_is_a_marker_error_that_names_it(what):
    with pytest.raises(MarkerError) as stop:
        parse(KEY, BAD[what])
    assert KEY in str(stop.value) and isinstance(stop.value, ContractError)


def test_the_error_for_a_field_names_the_field_and_the_table():
    body = good()
    table(body, "loan_repayments")["rows"] = -3
    with pytest.raises(MarkerError) as stop:
        parse(KEY, json.dumps(body).encode())
    assert "loan_repayments rows" in str(stop.value) and "-3" in str(stop.value)


@pytest.mark.parametrize("key", ["_runs/notes.json", "_runs/20261029T153000123456Z.json.bak", "_runs/", "_runs/x/y.json",
                                 "elsewhere/20261029T153000123456Z.json"])
def test_an_object_whose_name_is_not_a_marker_name_is_not_parsed(key):
    with pytest.raises(MarkerError) as stop:
        parse(key, encode(a_marker()))
    assert "not a run marker" in str(stop.value)


# ---------------------------------------------------------------------------
# the newest marker
# ---------------------------------------------------------------------------

def test_with_no_marker_there_is_no_newest_one_and_nothing_is_read():
    store = MemorySink()
    assert newest_marker(store) is None
    assert store.listings == 1 and store.reads == 0


def test_the_newest_marker_is_the_last_name_of_one_listing_and_only_it_is_read():
    store = MemorySink()
    stamps = ["20261028T100000000000Z", "20261030T100000000000Z", "20261029T100000000000Z"]    # written out of order
    for stamp in stamps:
        marker = build_marker(datetime.strptime(stamp, "%Y%m%dT%H%M%S%fZ").replace(tzinfo=timezone.utc),
                              landed_objects=int(stamp[6:8]), highest_commit_lsn=1, tables=marks())
        assert store.put_if_absent(marker.key, encode(marker)) is True
    found = newest_marker(store)
    assert found.run_id == "20261030T100000000000Z" and found.landed_objects == 30
    assert store.listings == 1 and store.reads == 1


@pytest.mark.parametrize("stray", ["_runs/notes.txt", "_runs/20261029T153000123456Z.json.bak", "_runs/0000", "_runs/zzz.json",
                                   "_runs/20250101T000000000000Z.json.bak", "_runs/20250101T000000000000Z.json\n",
                                   "_runs/20250101T000000000000Z.JSON"])
def test_an_object_under_runs_that_is_not_a_marker_stops_the_search_wherever_it_sorts(stray):
    store = MemorySink()
    marker = a_marker()
    store.put_if_absent(marker.key, encode(marker))
    store.objects[stray] = b"hello"
    with pytest.raises(MarkerError) as stop:
        newest_marker(store)
    assert stray in str(stop.value)


def test_a_newest_marker_that_does_not_parse_is_never_skipped_for_an_older_one():
    store = MemorySink()
    older = a_marker()
    store.put_if_absent(older.key, encode(older))
    put(store, b"{broken", key="_runs/20270101T000000000000Z.json")
    with pytest.raises(MarkerError) as stop:
        newest_marker(store)
    assert "20270101T000000000000Z" in str(stop.value)


def test_the_marker_found_is_the_parsed_content():
    store = MemorySink()
    marker = a_marker()
    store.put_if_absent(marker.key, encode(marker))
    assert newest_marker(store) == marker


# ---------------------------------------------------------------------------
# writing, and whether the tables are at a marker
# ---------------------------------------------------------------------------

def test_a_marker_is_created_once_and_never_replaced():
    store = MemorySink()
    marker = a_marker()
    write_marker(store, marker)
    assert store.objects == {KEY: encode(marker)}
    with pytest.raises(MarkerError) as stop:
        write_marker(store, a_marker(landed_objects=1))
    assert "never replaced" in str(stop.value) and KEY in str(stop.value)
    assert store.objects == {KEY: encode(marker)}


def snapshots_of(marker, **changes):
    found = {name: mark.snapshot_id for name, mark in marker.tables.items()}
    found.update(changes)
    return found


def test_tables_are_at_a_marker_when_every_snapshot_id_is_the_markers():
    marker = a_marker()
    assert describes(marker, snapshots_of(marker)) is True


@pytest.mark.parametrize("name", TABLES)
def test_one_table_at_another_snapshot_means_the_marker_does_not_describe_the_tables(name):
    marker = a_marker()
    assert describes(marker, snapshots_of(marker, **{name: marker.tables[name].snapshot_id + 1})) is False
    assert describes(marker, snapshots_of(marker, **{name: None})) is False


def test_tables_with_no_snapshot_are_at_a_marker_that_says_they_have_none():
    marker = a_marker(tables=marks(snapshot=None, rows=0))
    assert describes(marker, {name: None for name in TABLES}) is True
    assert describes(marker, {**{name: None for name in TABLES}, "merchants": 5}) is False


# ---------------------------------------------------------------------------
# the catalog must fit the marker
# ---------------------------------------------------------------------------

@pytest.fixture
def written(tmp_path):
    env = Env(tmp_path)
    env.run(busy_landing().reader())
    marker = newest_marker(env.store)
    return env, marker, {name: env.table(name) for name in TABLES}


def test_a_catalog_that_fits_the_marker_passes(written):
    _, marker, tables = written
    check_against_marker(marker, tables)


def test_a_catalog_that_is_ahead_of_the_marker_passes(written):
    env, marker, _ = written
    table = env.table("merchants")
    table.append(events_to_arrow([event("merchants", "c", 0x7FFFFF00, 0, 1)], arrow_schema_of(table.schema()),
                                 source_db=SOURCE_DB))
    assert table.current_snapshot().snapshot_id != marker.tables["merchants"].snapshot_id
    check_against_marker(marker, {name: env.table(name) for name in TABLES})


@pytest.mark.parametrize("name", TABLES)
def test_a_catalog_that_lacks_a_table_the_marker_names_stops_and_says_to_rebuild(written, name):
    _, marker, tables = written
    del tables[name]
    with pytest.raises(MarkerError) as stop:
        check_against_marker(marker, tables)
    assert name in str(stop.value) and marker.run_id in str(stop.value)
    assert "rebuild it from the marker" in str(stop.value) and "Nothing was written" in str(stop.value)


def test_a_catalog_with_no_tables_at_all_stops_on_the_first(written):
    _, marker, _ = written
    with pytest.raises(MarkerError):
        check_against_marker(marker, {})


@pytest.mark.parametrize("name", TABLES)
def test_a_table_without_the_markers_snapshot_stops(written, name):
    _, marker, tables = written
    marks_ = dict(marker.tables)
    marks_[name] = TableMark(999_999_999, marks_[name].rows, marks_[name].metadata_location)
    tampered = Marker(marker.run_id, 1, marker.written_at, marker.landed_objects, marker.highest_commit_lsn, marks_)
    with pytest.raises(MarkerError) as stop:
        check_against_marker(tampered, tables)
    assert name in str(stop.value) and "999999999" in str(stop.value) and "no such snapshot" in str(stop.value)


def test_a_marker_that_says_a_table_had_no_snapshot_accepts_a_table_that_has_some(written):
    _, marker, tables = written
    marks_ = {name: TableMark(None, 0, mark.metadata_location) for name, mark in marker.tables.items()}
    emptied = Marker(marker.run_id, 1, marker.written_at, marker.landed_objects, marker.highest_commit_lsn, marks_)
    check_against_marker(emptied, tables)
