"""
main.py: python -m bronze.main, the Bronze command (GCS Plan (V2) section 19, G3 question 5).

    python -m bronze.main                    plan only: read, check, count, print; write nothing to the bucket
    python -m bronze.main --write            do the work: create the tables, append what is missing, write the run
                                             marker, copy the catalog
    python -m bronze.main --rebuild-catalog  register the five tables in an empty local catalog from the newest run
                                             marker (after the catalog file was lost)

A thin command over bronze.run.run: it builds the landing reader, the catalog and the bucket's store, passes the
settings, prints, and turns every way out into an exit code and one line. The plan is the default because a write
is the one thing in this phase that cannot be undone cheaply (soft delete is off, and every commit costs Class A
operations); a plan reads each landed object once and writes nothing to the bucket (the local catalog file is
created if it is not there, which is only SQLite's own tables).

Run it with GOOGLE_APPLICATION_CREDENTIALS pointing at the bronze-writer credentials file; it refuses to start
without it (bronze.config.require_credentials). Settings are the BRONZE_ ones and, for the landing bucket, slot and
source database, the CDC_ ones (bronze/config.py, .env). Exit codes:

    0    done, or nothing to do (a plan always ends 0 when it could read everything)
    1    configuration or usage: missing credentials, both flags, a bug (the traceback is printed)
    2    a contract stop: what was landed breaks the contract, a table differs from the contract, a marker or the
         catalog does not fit, pass 2 did not match pass 1 (bronze.errors.ContractError)
    3    the bucket or a file operation failed: a list, read or write gave up after its retries (SinkError), or
         PyIceberg or pyarrow raised an OSError (what a failed GCS call is). A write says how many batches it had
         committed; running again continues from there
    130  interrupted with Ctrl-C (same note for a write)

Every non-zero exit prints one line starting "error:" to stderr. Progress goes to stderr: pass 1 every 100 objects,
and one line per committed batch. The report goes to stdout.
"""
from __future__ import annotations

import sys
import time
import traceback
from typing import Callable, Mapping, Optional, TextIO

import click
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog

from bronze.catalog_copy import rebuild_catalog
from bronze.check import (EXIT_BUCKET, EXIT_CONTRACT, EXIT_ERROR, EXIT_INTERRUPTED, EXIT_OK, PROGRESS_EVERY,
                          machine_memory_bytes, peak_memory_bytes, render)
from bronze.config import BronzeSettings, require_credentials
from bronze.errors import BronzeError, ConfigError, ContractError
from bronze.landing import LandingReader, open_landing
from bronze.run import NAMESPACE, TABLE_ORDER, BatchCommitted, RunReport, run
from bronze.store import CATALOG_KEY, Store, open_store
from cdc.errors import SinkError


def open_catalog(settings: BronzeSettings) -> Catalog:
    """The local SQLite catalog (BRONZE_CATALOG_PATH) with its tables in gs://BRONZE_BUCKET. PyIceberg reaches the
    bucket through pyarrow's GcsFileSystem, which uses the application-default credentials and needs no property."""
    settings.catalog_file.parent.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(NAMESPACE, uri=f"sqlite:///{settings.catalog_file}", warehouse=f"gs://{settings.bucket}")


def exit_code(error: BaseException) -> int:
    if isinstance(error, KeyboardInterrupt):
        return EXIT_INTERRUPTED
    if isinstance(error, (SinkError, OSError)):
        return EXIT_BUCKET
    if isinstance(error, ContractError):
        return EXIT_CONTRACT
    return EXIT_ERROR


def reason(error: BaseException) -> str:
    """One plain line saying why the command stopped."""
    text = " ".join(str(error).split())
    if isinstance(error, KeyboardInterrupt):
        return "interrupted"
    if isinstance(error, (BronzeError, SinkError)):
        return text
    if isinstance(error, OSError):
        return f"a bucket or file operation failed ({type(error).__name__}: {text})"
    return f"unexpected {type(error).__name__}: {text}"


def _megabytes(size: Optional[int]) -> str:
    return "unknown" if size is None else f"{size / 1_000_000:,.0f} MB"


def _count(number: int, word: str, plural: Optional[str] = None) -> str:
    return f"{number:,} {word if number == 1 else (plural or word + 's')}"


def render_tables(report: RunReport) -> str:
    """One line per table: what the landing holds, what Bronze held, what is to be (or was) appended."""
    header = ("table", "landed", "in Bronze", "to append", "objects", "batches", "")
    lines = [header]
    for name in TABLE_ORDER:
        t = report.tables[name]
        if report.write:
            note = "created" if t.created else ("up to date" if t.to_append == 0 else "")
            batches = f"{t.batches:,}"
        else:
            note = "new table" if not t.existed else ("up to date" if t.to_append == 0 else "")
            batches = "-"
        lines.append((name, f"{t.landed:,}", f"{t.rows_before:,}", f"{t.to_append:,}", f"{t.objects_to_read:,}",
                      batches, note))
    widths = [max(len(line[i]) for line in lines) for i in range(len(header))]
    out = []
    for n, line in enumerate(lines):
        cells = [line[0].ljust(widths[0])] + [cell.rjust(widths[i]) for i, cell in enumerate(line[1:-1], start=1)]
        out.append("  ".join(cells + [line[-1]]).rstrip())
        if n == 0:
            out.append("  ".join("-" * w for w in widths[:-1]))
    return "\n".join(out)


def _would_write(report: RunReport) -> str:
    created = sum(1 for t in report.tables.values() if not t.existed)
    rows = sum(t.to_append for t in report.tables.values())
    marker = rows > 0 or not report.marker.current        # with no marker at all, current is False
    steps = []
    if created:
        steps.append(f"create {_count(created, 'table')}")
    if rows:
        steps.append(f"append {_count(rows, 'row')} in {_count(sum(t.objects_to_read for t in report.tables.values()), 'object')}")
    if marker:
        steps.append("write a run marker")
        steps.append("copy the catalog to the bucket")
    return "a write would " + (", ".join(steps) if steps else "do nothing: Bronze is up to date")


def _marker_line(report: RunReport) -> str:
    m = report.marker
    if m.newest is None:
        return "run marker: none yet"
    return f"run marker: newest {m.newest}, the tables were {'at it' if m.current else 'not at it'} when the run started"


def execute(settings: BronzeSettings, *, write: bool = False, rebuild: bool = False,
            landing_factory: Optional[Callable[[BronzeSettings, Optional[Mapping[str, str]]], LandingReader]] = None,
            store_factory: Optional[Callable[[BronzeSettings, Optional[Mapping[str, str]]], Store]] = None,
            catalog_factory: Optional[Callable[[BronzeSettings], Catalog]] = None,
            environ: Optional[Mapping[str, str]] = None, out: Optional[TextIO] = None, err: Optional[TextIO] = None,
            clock: Callable[[], float] = time.monotonic) -> int:
    """Run the command and return the exit code. The factories exist so tests can supply a fake landing, a store
    and a local catalog; the command line uses the real ones."""
    out, err = out or sys.stdout, err or sys.stderr
    committed = 0

    def say(message: str) -> None:
        print(message, file=out, flush=True)

    def complain(message: str) -> None:
        print(message, file=err, flush=True)

    def on_batch(batch: BatchCommitted) -> None:
        nonlocal committed
        committed += 1
        complain(f"  {batch.table}: batch {batch.batch} committed, {_count(batch.rows, 'row')} "
                 f"({batch.rows_done:,} of {batch.rows_total:,})")

    def progress(done: int, total: int) -> None:
        if done % PROGRESS_EVERY == 0 or done == total:
            complain(f"  read {done:,} of {total:,} objects")

    started = clock()
    try:
        if write and rebuild:
            raise ConfigError("--write and --rebuild-catalog cannot be used together: a rebuild only registers the "
                              "tables in an empty catalog and writes nothing to the bucket")
        credentials = require_credentials(environ)
        store = (store_factory or open_store)(settings, environ)
        catalog = (catalog_factory or open_catalog)(settings)
        if rebuild:
            marker = rebuild_catalog(catalog, store)
            say(f"rebuilt the catalog {settings.catalog_file} from run marker {marker.run_id} "
                f"(credentials file {credentials})")
            for name in TABLE_ORDER:
                mark = marker.tables[name]
                say(f"  {name}: snapshot {mark.snapshot_id if mark.snapshot_id is not None else 'none'}, "
                    f"{mark.rows:,} rows")
            say("OK: run again without --rebuild-catalog to check Bronze against the landing")
            return EXIT_OK
        reader = (landing_factory or open_landing)(settings, environ)
        say(f"{'writing' if write else 'planning (nothing will be written)'}: gs://{settings.landing_bucket}/{reader.prefix} "
            f"into gs://{settings.bucket}, catalog {settings.catalog_file}, {settings.read_workers} reads at a time "
            f"(credentials file {credentials})")
        say(f"mapping every event to its Bronze row, source database {settings.source_db}")
        report = run(reader, catalog, store, catalog_file=settings.catalog_file,
                     warehouse=catalog.properties["warehouse"], source_db=settings.source_db, write=write,
                     workers=settings.read_workers, batch_rows=settings.batch_rows, progress=progress,
                     on_batch=on_batch)
    except BaseException as error:               # every way out becomes an exit code and one line
        if isinstance(error, SystemExit):
            raise
        text = reason(error)
        if committed and "committed" not in text:         # only a write commits batches
            text += (f". This run had committed {_count(committed, 'batch', 'batches')} before it stopped; "
                     "running again continues from there.")
        complain(f"error: {text}")
        if exit_code(error) == EXIT_ERROR and not isinstance(error, BronzeError):
            traceback.print_exception(error, file=err)       # an unexpected failure is a bug: show where
        return exit_code(error)

    if report.index_skipped:
        say(f"landing unchanged since run marker {report.marker.newest}: no object was read, the index was skipped")
    else:
        say(render(report.summary))
    say("")
    say(render_tables(report))
    say("")
    say(_marker_line(report))
    seconds = clock() - started
    if write:
        m = report.marker
        say(f"run marker written: {m.written}" if m.written else "run marker: unchanged, the tables are at it")
        say(f"catalog copy: replaced in gs://{settings.bucket}/{CATALOG_KEY}" if m.catalog_copied
            else "catalog copy: not needed")
        appended = sum(t.rows_appended for t in report.tables.values())
        if appended:
            verdict = (f"OK: appended {_count(appended, 'row')} in "
                       f"{_count(sum(t.batches for t in report.tables.values()), 'batch', 'batches')}")
        elif any(t.created for t in report.tables.values()):
            verdict = "OK: the tables were created, nothing to append"
        else:
            verdict = "OK: nothing to append, Bronze already holds every landed change once"
    else:
        say(_would_write(report))
        verdict = "OK: plan only, nothing was written. Run again with --write to do it."
    say("")
    say(f"time {int(seconds // 60)} min {int(seconds % 60)} s, peak memory {_megabytes(peak_memory_bytes())} "
        f"of {_megabytes(machine_memory_bytes())} on this machine")
    say(verdict)
    return EXIT_OK


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--write", is_flag=True, help="Do the work. Without it the command only plans: it reads and checks "
                                            "everything, prints what a write would do, and writes nothing to the bucket.")
@click.option("--rebuild-catalog", "rebuild", is_flag=True,
              help="Register the five tables in an empty local catalog from the newest run marker, after the catalog "
                   "file was lost. Writes nothing to the bucket; cannot be combined with --write.")
def cli(write: bool, rebuild: bool) -> None:
    """Bring the Bronze Iceberg tables up to date with the landed CDC objects. Needs GOOGLE_APPLICATION_CREDENTIALS
    set to the bronze-writer credentials file."""
    sys.exit(execute(BronzeSettings(), write=write, rebuild=rebuild))


def main(argv: Optional[list[str]] = None) -> int:
    """Run the command line; returns the exit code. A usage error is exit 1, not click's usual 2, because 2 means a
    contract stop here."""
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
