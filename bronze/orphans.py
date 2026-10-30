"""
orphans.py: python -m bronze.orphans, what is in the Bronze bucket that nothing uses, and what a table uses that is not
there (GCS Plan (V2) section 20, G4 decisions 3 and 4)

    python -m bronze.orphans                         list: read only, writes and deletes nothing
    python -m bronze.orphans --delete                also delete the orphans that are old enough
    python -m bronze.orphans --delete --min-age-hours 0   ... however young (see the risk below)

An Iceberg commit writes data files, then a manifest and a manifest list, then a new metadata file, and only then moves
the catalog's row to it. A run that dies part way leaves files nothing refers to (orphans). They are never visible to a
reader, but they cost storage and they are evidence of what a crash left behind. PyIceberg 0.12 has no cleanup for
them, so this is it.

What the files are "used" by is asked of PyIceberg and never of the file names (two metadata files can share a number):
for each table the current metadata file, every earlier one in the metadata log, every snapshot's manifest list, every
manifest and every data file of every snapshot. The bucket is listed once, and every object in it is accounted for as
exactly one of: a file of a table that is used, an orphan (a table file that is not used), the newest run marker or an
older one, the catalog copy, or unexpected (anything else, which is reported and never touched). It also reports the
other way: a file a table uses that is not in the bucket (data loss: exit 2).

It also checks the bucket against the newest run marker: the catalog must hold the marker's tables and snapshots (a
catalog BEHIND the marker would make real files look like orphans, so the command stops, exit 2, and deletes nothing),
the marker's metadata files must be used and present, and the catalog copy in the bucket must point at the metadata files
the marker names.

--delete removes a file only if ALL hold: it is a table file under bronze/<table>/data/ or bronze/<table>/metadata/
(the store refuses anything else, in code: BronzeStore.delete); nothing uses it; it is at least --min-age-hours old
(default 24, so a write running now, whose files are not used until its commit, is never touched); and nothing in the
whole check is clean (no used file missing, no catalog behind the marker, nothing unexpected in the bucket, the marker and
the catalog copy agreeing): with any problem nothing is deleted. It never touches _runs/, _catalog/ or the landing
bucket, and never removes a snapshot or an old metadata file that is still in a table's history. The list is printed before
anything is deleted; on the real bucket Ashley says go after reading it. Deletes cost no Class A or Class B operation,
and soft delete is off, so a deleted file is gone. RISK: with --min-age-hours 0 a file of a commit in progress looks like
an orphan; use it only when nothing is writing (a single writer is assumed throughout).

Run it with GOOGLE_APPLICATION_CREDENTIALS pointing at the bronze-writer credentials file (it may delete in
fintech_bronze); it refuses to start without it. Cost: one listing of the bucket (Class A), and reads (Class B) of the
newest marker, the catalog copy and each table's metadata, manifest lists and manifests. Exit codes:

    0    nothing is wrong; orphans, if any, are listed (and deleted with --delete)
    1    configuration or usage: missing credentials, a negative --min-age-hours, a bug (traceback printed)
    2    something is wrong: a used file is missing, the catalog is behind the marker, the catalog copy or a marker file
         does not fit, an unexpected object is in the bucket. With --delete nothing is deleted
    3    the bucket failed
    130  interrupted with Ctrl-C
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
import time
import traceback
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping, Optional, Protocol, TextIO

import click
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table

from bronze.check import EXIT_ERROR, EXIT_OK, machine_memory_bytes, peak_memory_bytes
from bronze.config import BronzeSettings, require_credentials
from bronze.errors import BronzeError, ConfigError, ContractError
from bronze.main import exit_code, open_catalog, reason
from bronze.marker import Marker, check_against_marker, describes, newest_marker
from bronze.run import NAMESPACE, TABLE_ORDER
from bronze.store import CATALOG_KEY, MARKER_KEY, is_table_file, open_store
from cdc.gcs import ObjectInfo

EXIT_PROBLEM = 2
DEFAULT_MIN_AGE_HOURS = 24.0
SHOWN = 50           # orphans listed one by one before "and N more"


class Bucket(Protocol):
    """What the command needs from the Bronze bucket: BronzeStore, or a fake with the same calls."""
    def list_keys(self, prefix: str) -> list[str]: ...
    def read(self, key: str) -> bytes: ...
    def list_objects(self, prefix: str) -> list[ObjectInfo]: ...
    def delete(self, key: str) -> bool: ...


class DataLoss(ContractError):
    """A file that a table, or a file of a table, points at is not in the bucket, so what the table uses cannot even be
    listed (exit 2)."""


@dataclass(frozen=True)
class Orphan:
    """A file under a table's data or metadata folder that no table uses."""
    key: str
    table: str
    size: int
    age: timedelta

    @property
    def kind(self) -> str:
        name = self.key.rsplit("/", 1)[-1]
        if name.endswith(".parquet"):
            return "data file"
        if name.endswith(".metadata.json"):
            return "metadata file"
        return "manifest list" if name.startswith("snap-") else "manifest"


@dataclass
class Scan:
    """What one listing of the bucket, set against the catalog and the newest marker, found."""
    objects: int = 0                                         # objects in the bucket
    used: dict[str, set[str]] = field(default_factory=dict)   # table -> the keys it uses
    in_bucket: dict[str, int] = field(default_factory=dict)   # table -> table files listed under it
    orphans: list[Orphan] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)         # used, and not in the bucket
    outside: list[str] = field(default_factory=list)         # used, and not under the warehouse at all
    unexpected: list[str] = field(default_factory=list)
    markers: int = 0
    copy_listed: bool = False                                # the catalog copy is in the bucket
    root: str = ""                                           # the warehouse: where every used file must be
    marker: Optional[Marker] = None
    at_marker: Optional[bool] = None                         # the tables are at the newest marker (None: no marker)
    copy: str = "no marker yet, so nothing to check the catalog copy against"
    problems: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems


def _snapshot_id(table: Table) -> Optional[int]:
    current = table.current_snapshot()
    return current.snapshot_id if current else None


def used_by(table: Table) -> set[str]:
    """Every file one table uses, as the locations PyIceberg holds: the current metadata file, every earlier one in
    the metadata log, each snapshot's manifest list, every manifest and every data (or delete) file of every snapshot."""
    paths = {table.metadata_location}
    paths |= {entry.metadata_file for entry in table.metadata.metadata_log}
    paths |= {snapshot.manifest_list for snapshot in table.snapshots() if snapshot.manifest_list}
    paths |= {row["path"] for row in table.inspect.all_manifests().to_pylist()}
    paths |= {row["file_path"] for row in table.inspect.all_files().to_pylist()}
    paths |= {statistics.statistics_path for statistics in table.metadata.statistics}
    paths |= {statistics.statistics_path for statistics in table.metadata.partition_statistics}
    return paths


def copy_differences(data: bytes, marker: Marker) -> list[str]:
    """How the catalog copy (the bytes of the SQLite file in the bucket) differs from the newest marker: each table
    must be in it and must point at the metadata file the marker names. Empty when they agree."""
    with tempfile.TemporaryDirectory(prefix="bronze_orphans_") as directory:
        path = Path(directory) / "catalog.db"
        path.write_bytes(data)
        try:
            with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as conn:
                rows = dict(conn.execute("SELECT table_name, metadata_location FROM iceberg_tables "
                                         "WHERE table_namespace = ?", (NAMESPACE,)).fetchall())
        except sqlite3.Error as error:
            return [f"the catalog copy is not a readable catalog ({type(error).__name__}: {error})"]
    differences = []
    for name in TABLE_ORDER:
        if name not in rows:
            differences.append(f"the catalog copy has no table {name}")
        elif rows[name] != marker.tables[name].metadata_location:
            differences.append(f"{name}: the catalog copy points at {rows[name]}, the newest marker names "
                               f"{marker.tables[name].metadata_location}")
    return differences


def _opened(name: str, read):
    """Run `read` (opening a table, or listing what it uses). A file the catalog or a metadata file points at that is not
    in the bucket makes that impossible: say so as data loss (exit 2) and stop, instead of as a failed bucket call."""
    try:
        return read()
    except FileNotFoundError as error:
        raise DataLoss(f"table {name} cannot be read because a file it depends on is not in the bucket ({error}). "
                       "This is data loss, not an orphan: nothing was listed or deleted.") from error


def scan(catalog: Catalog, bucket: Bucket, *, clock: Callable[[], datetime]) -> Scan:
    """List the bucket once and account for every object. Raises MarkerError (exit 2) if the catalog is behind the
    newest marker: then what is used cannot be told, and the listing is not made."""
    result = Scan()
    marker = newest_marker(bucket)
    tables = {}
    for name in TABLE_ORDER:
        try:
            tables[name] = _opened(name, lambda: catalog.load_table(f"{NAMESPACE}.{name}"))
        except NoSuchTableError:
            continue
    if marker is not None:
        check_against_marker(marker, tables)
        result.marker = marker
        result.at_marker = describes(marker, {name: _snapshot_id(table) for name, table in tables.items()})

    root = catalog.properties["warehouse"].rstrip("/") + "/"
    result.root = root
    everything: set[str] = set()
    for name, table in tables.items():
        keys = set()
        for path in _opened(name, lambda: used_by(table)):
            if path.startswith(root):
                keys.add(path[len(root):])
            else:
                result.outside.append(path)
        result.used[name] = keys
        everything |= keys

    listing = bucket.list_objects("")
    result.objects = len(listing)
    listed = {info.key for info in listing}
    result.in_bucket = {name: 0 for name in TABLE_ORDER}
    now = clock()
    for info in listing:
        if MARKER_KEY.fullmatch(info.key):
            result.markers += 1
        elif info.key == CATALOG_KEY:
            result.copy_listed = True
        elif is_table_file(info.key):
            table = info.key.split("/")[1]
            result.in_bucket[table] += 1
            if info.key not in everything:
                result.orphans.append(Orphan(info.key, table, info.size, now - info.created))
        else:
            result.unexpected.append(info.key)
    result.missing = sorted(everything - listed)

    if result.missing:
        result.problems.append(f"{len(result.missing):,} files a table uses are not in the bucket (data loss), for example "
                               f"{result.missing[0]}")
    if result.outside:
        result.problems.append(f"{len(result.outside):,} files a table uses are outside the warehouse {root}, for example "
                               f"{sorted(result.outside)[0]}")
    if result.unexpected:
        result.problems.append(f"{len(result.unexpected):,} objects are in the bucket that belong to nothing Bronze writes, "
                               f"for example {result.unexpected[0]} (never deleted by this command)")
    if marker is not None:
        for name in TABLE_ORDER:
            location = marker.tables[name].metadata_location
            if not location.startswith(root):
                result.problems.append(f"the newest marker names {location} for {name}, which is not in this warehouse")
            elif location[len(root):] not in result.used.get(name, set()):
                result.problems.append(f"the newest marker names {location} for {name}, which the catalog does not use")
            elif location[len(root):] not in listed:
                result.problems.append(f"the newest marker names {location} for {name}, which is not in the bucket")
        if CATALOG_KEY not in listed:
            result.copy = "MISSING"
            result.problems.append(f"the bucket has no catalog copy ({CATALOG_KEY}) though the marker {marker.run_id} exists")
        else:
            differences = copy_differences(bucket.read(CATALOG_KEY), marker)
            result.copy = "agrees with the newest marker" if not differences else "DIFFERS from the newest marker"
            result.problems.extend(differences)
    return result


def deletable(orphan: Orphan, found: Scan, min_age: timedelta) -> bool:
    """All of: a table file (the store checks this again), used by no table, and old enough."""
    return (is_table_file(orphan.key) and orphan.age >= min_age
            and all(orphan.key not in keys for keys in found.used.values()))


def _size(bytes_: int) -> str:
    if bytes_ < 10_000:
        return f"{bytes_:,} B"
    return f"{bytes_ / 1_000:,.1f} KB" if bytes_ < 10_000_000 else f"{bytes_ / 1_000_000:,.1f} MB"


def _age(age: timedelta) -> str:
    hours = age.total_seconds() / 3600
    return f"{hours / 24:.1f} days" if hours >= 48 else f"{hours:.1f} hours"


def render(found: Scan, min_age: timedelta, bucket_name: str) -> str:
    """The accounting of the bucket, as a block of text."""
    width = max(len(name) for name in TABLE_ORDER)
    lines = [f"{found.objects:,} objects in gs://{bucket_name}: {sum(found.in_bucket.values()):,} table files, "
             f"{found.markers:,} run markers, {1 if found.copy_listed else 0} catalog copy",
             "", f"{'table':<{width}}  {'used':>8}  {'in bucket':>9}  {'orphans':>8}"]
    for name in TABLE_ORDER:
        orphans = sum(1 for o in found.orphans if o.table == name)
        lines.append(f"{name:<{width}}  {len(found.used.get(name, ())):>8,}  {found.in_bucket.get(name, 0):>9,}  {orphans:>8,}")
    lines += ["", "run marker: " + ("none yet" if found.marker is None else
                                    f"newest {found.marker.run_id}; the tables are "
                                    + ("at it" if found.at_marker else "AHEAD of it (a run committed after its last marker: "
                                       "the next run writes a new one)")),
              f"catalog copy: {found.copy}", ""]
    if not found.orphans:
        lines.append("orphans: none")
    else:
        old = [o for o in found.orphans if o.age >= min_age]
        lines.append(f"orphans: {len(found.orphans):,} files, {_size(sum(o.size for o in found.orphans))}; "
                     f"{len(old):,} are at least {min_age.total_seconds() / 3600:g} hours old")
        for orphan in sorted(found.orphans, key=lambda o: o.key)[:SHOWN]:
            lines.append(f"  {orphan.key}  {_size(orphan.size)}  {orphan.kind}  {_age(orphan.age)} old"
                         + ("" if orphan.age >= min_age else "  (too young to delete)"))
        if len(found.orphans) > SHOWN:
            lines.append(f"  (and {len(found.orphans) - SHOWN:,} more)")
    return "\n".join(lines)


def _megabytes(size: Optional[int]) -> str:
    return "unknown" if size is None else f"{size / 1_000_000:,.0f} MB"


def execute(settings: BronzeSettings, *, delete: bool = False, min_age_hours: float = DEFAULT_MIN_AGE_HOURS,
            store_factory: Optional[Callable[[BronzeSettings, Optional[Mapping[str, str]]], Bucket]] = None,
            catalog_factory: Optional[Callable[[BronzeSettings], Catalog]] = None,
            clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
            environ: Optional[Mapping[str, str]] = None, out: Optional[TextIO] = None, err: Optional[TextIO] = None,
            timer: Callable[[], float] = time.monotonic) -> int:
    """Run the command and return the exit code. The factories exist so tests can supply a bucket and a local catalog;
    the command line uses the real ones."""
    out, err = out or sys.stdout, err or sys.stderr

    def say(message: str) -> None:
        print(message, file=out, flush=True)

    def complain(message: str) -> None:
        print(message, file=err, flush=True)

    started = timer()
    deleted = 0
    try:
        if min_age_hours < 0:
            raise ConfigError("--min-age-hours cannot be negative")
        credentials = require_credentials(environ)
        bucket = (store_factory or open_store)(settings, environ)
        catalog = (catalog_factory or open_catalog)(settings)
        min_age = timedelta(hours=min_age_hours)
        say(f"{'listing and deleting' if delete else 'listing (nothing will be deleted)'}: gs://{settings.bucket}, "
            f"catalog {settings.catalog_file} (credentials file {credentials})")
        found = scan(catalog, bucket, clock=clock)
        say(render(found, min_age, settings.bucket))
        if found.ok and delete:
            targets = [o for o in sorted(found.orphans, key=lambda o: o.key) if deletable(o, found, min_age)]
            say("")
            if not targets:
                say("--delete: nothing to delete" + (f" ({len(found.orphans):,} orphans are too young)"
                                                      if found.orphans else ""))
            else:
                say(f"--delete: deleting {len(targets):,} files, {_size(sum(o.size for o in targets))}")
                for orphan in targets:
                    if bucket.delete(orphan.key):
                        deleted += 1
                        complain(f"  deleted {orphan.key}")
                say(f"deleted {deleted:,} files" + ("" if deleted == len(targets)
                                                     else f" ({len(targets) - deleted:,} were already gone)"))
    except BaseException as error:               # every way out becomes an exit code and one line
        if isinstance(error, SystemExit):
            raise
        text = reason(error)
        if deleted:
            text += (f". {deleted:,} files had been deleted before it stopped; run it again to continue "
                     "(a file that is already gone is not an error).")
        complain(f"error: {text}")
        if exit_code(error) == EXIT_ERROR and not isinstance(error, BronzeError):
            traceback.print_exception(error, file=err)       # an unexpected failure is a bug: show where
        return exit_code(error)

    seconds = timer() - started
    say("")
    say(f"time {int(seconds // 60)} min {int(seconds % 60)} s, peak memory {_megabytes(peak_memory_bytes())} "
        f"of {_megabytes(machine_memory_bytes())} on this machine")
    if not found.ok:
        for problem in found.problems:
            print(f"PROBLEM: {problem}", file=err, flush=True)
        complain(f"error: NOT CLEAN: {len(found.problems)} problems" + ("; nothing was deleted" if delete else ""))
        return EXIT_PROBLEM
    if delete:
        say(f"OK: deleted {deleted:,} orphans; the rest of the bucket is accounted for")
    else:
        say("OK: every object in the bucket is accounted for"
            + (f"; {len(found.orphans):,} orphans are listed above (run with --delete to remove the old ones)"
               if found.orphans else "; there are no orphans"))
    return EXIT_OK


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--delete", "do_delete", is_flag=True,
              help="Also delete the orphans: unused files under bronze/TABLE/data or metadata that are at least "
                   "--min-age-hours old. Without it nothing is deleted. Soft delete is off: a deleted file is gone.")
@click.option("--min-age-hours", type=float, default=DEFAULT_MIN_AGE_HOURS, show_default=True,
              help="With --delete, the youngest orphan to delete. 0 deletes however young: only when nothing is writing.")
def cli(do_delete: bool, min_age_hours: float) -> None:
    """List what is in the Bronze bucket that no table uses, and what a table uses that is missing, and check the bucket
    against the newest run marker and the catalog copy. Needs GOOGLE_APPLICATION_CREDENTIALS set to the
    bronze-writer credentials file."""
    sys.exit(execute(BronzeSettings(), delete=do_delete, min_age_hours=min_age_hours))


def main(argv: Optional[list[str]] = None) -> int:
    """Run the command line; returns the exit code. A usage error is exit 1, not click's usual 2, because 2 means
    "something is wrong" here."""
    try:
        cli.main(args=argv, standalone_mode=False)
    except SystemExit as stop:
        return int(stop.code or 0)
    except click.ClickException as error:
        print(f"error: {error.format_message()}", file=sys.stderr)
        return EXIT_ERROR
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
