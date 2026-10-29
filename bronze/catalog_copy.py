"""
catalog_copy.py: the copy of the local catalog, and the rebuild of a lost one (GCS Plan (V2) section 19, G3 question 4).

The Iceberg catalog is a local SQLite file (BRONZE_CATALOG_PATH) whose only content that matters is, for each table,
the location of its current metadata file. The bucket's metadata files are the truth; the file is a cache of them.

copy_catalog: after a run that changed something, the SQLite file is copied with SQLite's backup API (a consistent
snapshot even while the catalog has the file open, and never a half-written page) and the bytes replace
gs://fintech_bronze/_catalog/catalog.db. A failed copy is a SinkError (exit 3) whose message says the data is committed.
The decided order is marker first, then this copy, so a copy that fails does not stop the marker; the price is that a
run that has nothing to change does not repeat a copy that failed, and the copy stays behind until a run changes
something. That is harmless because the repair of a lost or stale catalog is the rebuild below, which does not use the
copy.

rebuild_catalog: registers, for each of the five tables, the EXACT metadata file named in the newest marker
(Catalog.register_table), never "the newest file by name" (two files can share a number). It refuses unless there is a
marker and the catalog has none of the five tables, and it checks each registered table against the marker (the snapshot
id and the row count) and drops every table it tried to register if any does not match or cannot be read (drop_table forgets the
catalog entry and deletes no file). After it, a normal run resumes: the tables hold every key, so it appends nothing.
"""
from __future__ import annotations

import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table

from bronze.errors import ConfigError, MarkerError
from bronze.marker import TABLES, Marker, newest_marker
from bronze.store import CATALOG_KEY, Store

NAMESPACE = "bronze"


def catalog_bytes(catalog_file: Path) -> bytes:
    """A consistent copy of the SQLite catalog file, by the backup API, as bytes. The file is opened read only, so a
    file that is not there is an error and not a new empty database."""
    try:
        source = sqlite3.connect(f"file:{catalog_file}?mode=ro", uri=True)
    except sqlite3.OperationalError as error:
        raise ConfigError(f"the catalog file {catalog_file} cannot be opened, so there is nothing to copy "
                          f"({error})") from error
    with closing(source), tempfile.TemporaryDirectory(prefix="bronze_catalog_copy_") as directory:
        copy = Path(directory) / "catalog.db"
        with closing(sqlite3.connect(copy)) as target:
            source.backup(target)
        return copy.read_bytes()


def copy_catalog(catalog_file: Path, store: Store) -> None:
    """Replace the bucket's catalog copy with the catalog file as it is now."""
    store.overwrite(CATALOG_KEY, catalog_bytes(catalog_file))


def _snapshot_id(table: Table):
    snapshot = table.current_snapshot()
    return snapshot.snapshot_id if snapshot is not None else None


def _row_count(table: Table) -> int:
    return sum(batch.num_rows for batch in table.scan(selected_fields=("commit_lsn",)).to_arrow_batch_reader())


def rebuild_catalog(catalog: Catalog, store: Store) -> Marker:
    """Register the five tables from the newest marker in an empty catalog. Returns the marker used."""
    marker = newest_marker(store)
    if marker is None:
        raise ConfigError("there is no run marker in the bucket, so there is nothing to rebuild the catalog from. "
                          "A first run creates the tables.")
    present = []
    for name in TABLES:
        try:
            catalog.load_table(f"{NAMESPACE}.{name}")
            present.append(name)
        except NoSuchTableError:
            pass
    if present:
        raise ConfigError(f"the catalog already has {', '.join(present)}: a rebuild is for a catalog that has lost its "
                          "tables, and registering over them could point a table at an older state")
    catalog.create_namespace_if_not_exists(NAMESPACE)
    attempted = []
    try:
        for name in TABLES:
            mark = marker.tables[name]
            attempted.append(name)      # before the call: register_table adds the entry before it reads the file
            table = catalog.register_table(f"{NAMESPACE}.{name}", mark.metadata_location)
            found, rows = _snapshot_id(table), _row_count(table)
            if found != mark.snapshot_id or rows != mark.rows:
                raise MarkerError(
                    f"table {name} registered from {mark.metadata_location} is at snapshot {found} with {rows:,} rows, "
                    f"but marker {marker.run_id} says snapshot {mark.snapshot_id} with {mark.rows:,} rows")
    except BaseException:
        for name in attempted:
            try:
                catalog.drop_table(f"{NAMESPACE}.{name}")     # the catalog's entry only: no file is deleted
            except NoSuchTableError:
                pass
        raise
    return marker
