"""
marker.py: the run marker, Bronze's consistency note (Bronze contract section 8, GCS Plan (V2) section 19, G3 question 4).

Iceberg has no multi-table commit, so between two table commits the tables of a running or crashed run are at
different points. When a run ends with all five tables holding every key the landing holds, it writes one object,

    gs://fintech_bronze/_runs/<run_id>.json

and a reader who needs a consistent set of tables reads each table at the snapshot ids of the NEWEST marker. The
marker is created once (put_if_absent) and never replaced; a new one is written only when the tables' snapshots
differ from the newest marker's, so a run that changes nothing writes nothing.

The name is the run_id: the UTC time to the microsecond as YYYYMMDDTHHMMSSffffffZ, fixed width, so the newest marker
is the last name of ONE listing of _runs/. An object under _runs/ that is not a marker stops the run (MarkerError):
skipping it could mean skipping the real newest.

The content (the exact field names are mine; the contract names the fields):

    {"run_id": "...", "bronze_contract_version": 1, "written_at": "2026-10-29T15:30:00.123456Z",
     "landed_objects": 908, "highest_commit_lsn": 12345678,
     "tables": {"<table>": {"snapshot_id": 1234 or null, "rows": 20078, "metadata_location": "gs://..."}, ...}}

metadata_location is the exact metadata file of that snapshot. A catalog rebuild registers these files, never "the
newest metadata file by name", because two files can share a number (bronze/catalog_copy.py). parse() is strict: a
field missing or added, a wrong type, a table set that is not the five, a run_id that is not the object's name, or
another contract version is a MarkerError.

check_against_marker is the other half of "the catalog is a cache": when a marker exists, every table it names must be
in the catalog and have the marker's snapshot in its history (it may be ahead, from a run that crashed before its
marker). A catalog that has lost a table, or is from another warehouse, would otherwise make the run create the tables
again beside the old ones; it stops, and says to rebuild from the marker.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Optional

from pyiceberg.table import Table

from bronze.errors import MarkerError
from bronze.schema import CONTRACT_VERSION
from bronze.store import MARKER_KEY, RUNS_PREFIX, Store
from cdc.columns import EXPECTED_COLUMNS

TABLES = tuple(EXPECTED_COLUMNS)
LSN_LIMIT = 2 ** 63
_FIELDS = ("run_id", "bronze_contract_version", "written_at", "landed_objects", "highest_commit_lsn", "tables")
_TABLE_FIELDS = ("snapshot_id", "rows", "metadata_location")
_WRITTEN_AT = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z")


@dataclass(frozen=True, slots=True)
class TableMark:
    snapshot_id: Optional[int]    # None: the table has no snapshot (nothing landed for it)
    rows: int
    metadata_location: str


@dataclass(frozen=True, slots=True)
class Marker:
    run_id: str
    bronze_contract_version: int
    written_at: str
    landed_objects: int
    highest_commit_lsn: Optional[int]
    tables: dict            # table -> TableMark, in table order

    @property
    def key(self) -> str:
        return f"{RUNS_PREFIX}{self.run_id}.json"


def build_marker(now: datetime, *, landed_objects: int, highest_commit_lsn: Optional[int],
                 tables: Mapping[str, TableMark]) -> Marker:
    """The marker for a run that ends at `now` (a UTC datetime): its run_id is the time to the microsecond."""
    return Marker(run_id=now.strftime("%Y%m%dT%H%M%S%fZ"), bronze_contract_version=CONTRACT_VERSION,
                  written_at=now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), landed_objects=landed_objects,
                  highest_commit_lsn=highest_commit_lsn, tables={name: tables[name] for name in TABLES})


def encode(marker: Marker) -> bytes:
    body = {"run_id": marker.run_id, "bronze_contract_version": marker.bronze_contract_version,
            "written_at": marker.written_at, "landed_objects": marker.landed_objects,
            "highest_commit_lsn": marker.highest_commit_lsn,
            "tables": {name: {"snapshot_id": t.snapshot_id, "rows": t.rows, "metadata_location": t.metadata_location}
                       for name, t in marker.tables.items()}}
    return (json.dumps(body, indent=2) + "\n").encode("utf-8")


def _int(value, what: str, key: str, *, low: int = 0, high: Optional[int] = None, nullable: bool = False):
    if value is None and nullable:
        return None
    if type(value) is not int or value < low or (high is not None and value >= high):
        raise MarkerError(f"{key}: {what} is {value!r}, which is not a whole number"
                          f"{' or null' if nullable else ''} from {low}{'' if high is None else f' below {high}'}")
    return value


def parse(key: str, data: bytes) -> Marker:
    """The marker in the object `key`. Raises MarkerError for anything the contract does not allow."""
    match = MARKER_KEY.fullmatch(key)
    if match is None:
        raise MarkerError(f"{key} is under {RUNS_PREFIX} but is not a run marker ({RUNS_PREFIX}<run_id>.json); "
                          "the newest marker cannot be found while it is there")
    try:
        body = json.loads(data.decode("utf-8"))
    except ValueError as error:      # a UnicodeDecodeError is a ValueError
        raise MarkerError(f"{key} is not valid JSON ({type(error).__name__}: {error})") from error
    if not isinstance(body, dict) or tuple(sorted(body)) != tuple(sorted(_FIELDS)):
        found = sorted(body) if isinstance(body, dict) else type(body).__name__
        raise MarkerError(f"{key}: the fields are {found}, the contract's are {sorted(_FIELDS)}")
    if body["run_id"] != match["run_id"]:
        raise MarkerError(f"{key}: run_id is {body['run_id']!r}, but the object's name says {match['run_id']}")
    if type(body["bronze_contract_version"]) is not int or body["bronze_contract_version"] != CONTRACT_VERSION:
        raise MarkerError(f"{key}: written under contract version {body['bronze_contract_version']!r}, "
                          f"this code reads version {CONTRACT_VERSION}")
    if type(body["written_at"]) is not str or _WRITTEN_AT.fullmatch(body["written_at"]) is None:
        raise MarkerError(f"{key}: written_at is {body['written_at']!r}, not YYYY-MM-DDTHH:MM:SS.ffffffZ")
    landed = _int(body["landed_objects"], "landed_objects", key)
    highest = _int(body["highest_commit_lsn"], "highest_commit_lsn", key, high=LSN_LIMIT, nullable=True)
    tables = body["tables"]
    if not isinstance(tables, dict) or tuple(sorted(tables)) != tuple(sorted(TABLES)):
        raise MarkerError(f"{key}: the tables are {sorted(tables) if isinstance(tables, dict) else tables!r}, "
                          f"the contract's are {sorted(TABLES)}")
    marks = {}
    for name in TABLES:
        mark = tables[name]
        if not isinstance(mark, dict) or tuple(sorted(mark)) != tuple(sorted(_TABLE_FIELDS)):
            raise MarkerError(f"{key}: table {name} has {sorted(mark) if isinstance(mark, dict) else mark!r}, "
                              f"the contract's fields are {sorted(_TABLE_FIELDS)}")
        location = mark["metadata_location"]
        if type(location) is not str or not location:
            raise MarkerError(f"{key}: table {name} metadata_location is {location!r}, not a location")
        marks[name] = TableMark(_int(mark["snapshot_id"], f"{name} snapshot_id", key, nullable=True),
                                _int(mark["rows"], f"{name} rows", key), location)
    return Marker(match["run_id"], CONTRACT_VERSION, body["written_at"], landed, highest, marks)


def newest_marker(store: Store) -> Optional[Marker]:
    """The newest marker, or None when there is none: one listing of _runs/ and one read. Every name under _runs/
    must be a marker's (so none is skipped), and the newest must parse."""
    keys = store.list_keys(RUNS_PREFIX)
    for key in keys:
        if MARKER_KEY.fullmatch(key) is None:
            raise MarkerError(f"{key} is under {RUNS_PREFIX} but is not a run marker "
                              f"({RUNS_PREFIX}<run_id>.json); the newest marker cannot be found while it is there")
    if not keys:
        return None
    newest = keys[-1]
    return parse(newest, store.read(newest))


def describes(marker: Marker, snapshots: Mapping[str, Optional[int]]) -> bool:
    """True if the tables are at the snapshots the marker names (None for a table with no snapshot)."""
    return all(marker.tables[name].snapshot_id == snapshots[name] for name in TABLES)


def write_marker(store: Store, marker: Marker) -> None:
    """Create the marker. A name that is already taken is an error: the run_id is the time to the microsecond, so a
    clash means two runs at once (a second writer, unsupported) or a clock that went back, and replacing is never right."""
    if not store.put_if_absent(marker.key, encode(marker)):
        raise MarkerError(f"{marker.key} already exists: a marker is never replaced. Another run may be writing "
                          "to this warehouse at the same time, which is not supported.")


def check_against_marker(marker: Marker, existing: Mapping[str, Table]) -> None:
    """Stop unless the catalog's tables fit the marker: each is there, and has the marker's snapshot in its history
    (or the marker says it had none)."""
    for name in TABLES:
        mark, table = marker.tables[name], existing.get(name)
        if table is None:
            raise MarkerError(
                f"the newest marker ({marker.run_id}) says table {name} exists, but the catalog does not have it. "
                "The catalog is a cache of the bucket: move the catalog file aside and run python -m bronze.main "
                "--rebuild-catalog, do not create the tables again beside the old ones. Nothing was written.")
        if mark.snapshot_id is not None and table.snapshot_by_id(mark.snapshot_id) is None:
            raise MarkerError(
                f"the newest marker ({marker.run_id}) says table {name} had snapshot {mark.snapshot_id}, but the "
                "catalog's table has no such snapshot: the catalog is from another warehouse or an older state than "
                "the marker. Move the catalog file aside and run python -m bronze.main --rebuild-catalog. "
                "Nothing was written.")
