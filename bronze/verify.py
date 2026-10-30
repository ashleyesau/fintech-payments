"""
verify.py: python -m bronze.verify, is Bronze correct? (GCS Plan (V2) section 20, G4 decision 1)

    python -m bronze.verify                 Bronze against the landed objects AND against the source database
    python -m bronze.verify --landing-only  the first of those two checks only; it says it is not full proof

A thin command over bronze.replay.check, which asks three things: (1) does Bronze hold exactly the landed events, each
once; (2) does Bronze, replayed into a blank database built from the migrations, equal the source database table by table,
by digest; (3) do the updates found in Bronze touch exactly the loans the source shows as disbursed. The first compares
Bronze with the landing, which our own pipeline produced; the second and third compare it with the original, which it did
not. So a pass of the plain command is the full proof (GCS Plan acceptance criterion 6), and the flag that skips the source
comparison says so in its result and is never the evidence for G4.

If the source database cannot be used the command FAILS with a plain message (exit 3) and never skips the comparison. That
and every other cheap check (the credentials, the local catalog holding the five tables, the source database reachable,
allowed to create the scratch database and holding the five tables) is made before the first Google operation, so a
missing database never costs the reads of 908 landed objects.

The source's table digests are taken before the check starts and again when it ends. A table that changed in between is
reported as "the source changed during the check" and exits 2 as NOT PROVEN: it is not a Bronze mismatch, and it is not
a pass. Run it again when the source is quiet.

It reads, and writes nothing to either bucket. It creates one scratch database on the source's server
(fintech_replay_<pid>) and drops it at the end, also when interrupted. Cost on the real bucket: one listing of the
landing (Class A) and one read of each landed object (Class B), plus the metadata and data files of the five Bronze tables
(Class B): measure it with scripts/storage_request_counts.py before and after. Bronze is read through the local catalog,
BRONZE_CATALOG_PATH, so pointing that setting at a catalog rebuilt from the bucket (python -m bronze.main
--rebuild-catalog) verifies what the bucket holds without using the original catalog file.

Run it with GOOGLE_APPLICATION_CREDENTIALS pointing at the bronze-writer credentials file; it refuses to start without it.
The source is the database named by the CDC_ settings (host, port, CDC_DB, user, password). Exit codes:

    0    verified (or, with --landing-only, the landing check passed: NOT full proof)
    1    configuration or usage: missing credentials, a catalog without the Bronze tables, a bug (traceback printed)
    2    NOT VERIFIED: a mismatch, a contract stop, or the source changed during the check (NOT PROVEN)
    3    the bucket failed, or the source database could not be used
    130  interrupted with Ctrl-C

Every non-zero exit prints one line starting "error:" to stderr (and one PROBLEM: line for each mismatch); progress goes to
stderr, the figures to stdout.
"""
from __future__ import annotations

import sys
import time
import traceback
from typing import Callable, Mapping, Optional, TextIO

import click
import psycopg
from psycopg import conninfo
from pyiceberg.catalog import Catalog

from bronze.check import (EXIT_BUCKET, EXIT_CONTRACT, EXIT_ERROR, EXIT_INTERRUPTED, EXIT_OK, machine_memory_bytes,
                          peak_memory_bytes)
from bronze.config import BronzeSettings, require_credentials
from bronze.errors import BronzeError, ConfigError, ContractError, SourceError
from bronze.landing import LandingReader, open_landing
from bronze.main import open_catalog
from bronze.replay import Report, check
from bronze.run import NAMESPACE, TABLE_ORDER
from cdc.config import CdcSettings
from cdc.errors import SinkError

NOT_FULL_PROOF = "SOURCE COMPARISON NOT RUN, THIS IS NOT FULL PROOF"


def exit_code(error: BaseException) -> int:
    if isinstance(error, KeyboardInterrupt):
        return EXIT_INTERRUPTED
    if isinstance(error, (SinkError, SourceError, OSError, psycopg.Error)):
        return EXIT_BUCKET
    if isinstance(error, ContractError):
        return EXIT_CONTRACT
    return EXIT_ERROR


def reason(error: BaseException) -> str:
    """One plain line saying why the command stopped."""
    text = " ".join(str(error).split())
    if isinstance(error, KeyboardInterrupt):
        return "interrupted; nothing was written to either bucket"
    if isinstance(error, (BronzeError, SinkError)):
        return text
    if isinstance(error, psycopg.Error):
        return f"the source database failed during the check ({type(error).__name__}: {text})"
    if isinstance(error, OSError):
        return f"a bucket or file operation failed ({type(error).__name__}: {text})"
    return f"unexpected {type(error).__name__}: {text}"


def _where(source_dsn: str) -> str:
    """host:port/database of a connection string, never the user's password."""
    parts = conninfo.conninfo_to_dict(source_dsn)
    return f"{parts.get('host', 'localhost')}:{parts.get('port', '5432')}/{parts.get('dbname', '')}"


def check_catalog(catalog: Catalog, settings: BronzeSettings) -> None:
    """The local catalog must hold the five Bronze tables. Asked of the local file only, before any Google operation: a lost
    catalog would otherwise be found after every landed object had been read."""
    missing = [name for name in TABLE_ORDER if not catalog.table_exists(f"{NAMESPACE}.{name}")]
    if missing:
        raise ConfigError(f"the local catalog {settings.catalog_file} has no Bronze table for {', '.join(missing)}. If the "
                          "file was lost, run python -m bronze.main --rebuild-catalog; if BRONZE_CATALOG_PATH points at "
                          "the wrong file, correct it")


def check_source(source_dsn: str, settings: BronzeSettings) -> None:
    """The source database must be reachable, allowed to create the scratch database and hold the five tables. Raises
    SourceError with the way out in the message; never returns quietly when the comparison cannot be made."""
    where = _where(source_dsn)
    fail = f"cannot use the source database {where}: %s. The comparison with the source is never skipped; " \
           "fix the database, or run with --landing-only (which is NOT full proof)"
    try:
        with psycopg.connect(source_dsn, connect_timeout=10, autocommit=True) as conn:
            can_create = conn.execute("SELECT rolcreatedb OR rolsuper FROM pg_roles WHERE rolname = current_user").fetchone()
            if not can_create or not can_create[0]:
                raise SourceError(fail % "its user may not create a database, and the check replays Bronze into a scratch one")
            for table in TABLE_ORDER:
                try:
                    conn.execute(f"SELECT count(*) FROM {table}").fetchone()
                except psycopg.errors.UndefinedTable:
                    raise SourceError(fail % f"it has no table {table}; is {settings.source_db} the right database?") from None
    except psycopg.Error as error:
        raise SourceError(fail % " ".join(str(error).split())[:200]) from None


def _hex(value: int) -> str:
    return f"{value:x}"


def render(report: Report, *, landing_only: bool) -> str:
    """The counts and digests of a finished check, as a block of text."""
    width = max(len(t) for t in TABLE_ORDER)
    lines = [f"landed objects read    {report.objects:,}", "",
             "landed against Bronze (digests of the events, order independent; equal means the same events):",
             f"{'table':<{width}}  {'landed':>12}  {'Bronze':>12}  {'landed digest':>20}  {'Bronze digest':>20}  Bronze snapshot"]
    for table in TABLE_ORDER:
        landed, held = report.landed[table], report.bronze[table]
        snapshot = report.snapshots.get(table)
        lines.append(f"{table:<{width}}  {landed.rows:>12,}  {held.rows:>12,}  {_hex(landed.total):>20}  {_hex(held.total):>20}  "
                     f"{snapshot if snapshot is not None else 'none'}")
    lines.append(f"{'total':<{width}}  {sum(d.rows for d in report.landed.values()):>12,}  "
                 f"{sum(d.rows for d in report.bronze.values()):>12,}")
    if landing_only:
        lines += ["", NOT_FULL_PROOF]
        return "\n".join(lines)
    lines += ["", "the source against Bronze replayed into a blank database (digests of the rows, computed in SQL):",
              f"{'table':<{width}}  {'source rows':>12}  {'replayed':>12}  {'source digest':>20}  {'replay digest':>20}"]
    for table in TABLE_ORDER:
        source, replayed = report.source[table], report.replayed[table]
        lines.append(f"{table:<{width}}  {source[0]:>12,}  {replayed[0]:>12,}  {_hex(source[1]):>20}  {_hex(replayed[1]):>20}")
    lines += ["", f"updates held in Bronze {report.updates:,}, loans the source shows as disbursed "
                  f"{report.disbursed if report.disbursed is not None else 'unknown'}"]
    return "\n".join(lines)


def _megabytes(size: Optional[int]) -> str:
    return "unknown" if size is None else f"{size / 1_000_000:,.0f} MB"


def execute(settings: BronzeSettings, *, landing_only: bool = False, source_dsn: Optional[str] = None,
            landing_factory: Optional[Callable[[BronzeSettings, Optional[Mapping[str, str]]], LandingReader]] = None,
            catalog_factory: Optional[Callable[[BronzeSettings], Catalog]] = None,
            check_fn: Callable[..., Report] = check,
            environ: Optional[Mapping[str, str]] = None, out: Optional[TextIO] = None, err: Optional[TextIO] = None,
            clock: Callable[[], float] = time.monotonic) -> int:
    """Run the command and return the exit code. The factories and check_fn exist so tests can supply a fake landing, a
    local catalog and a stand-in check; the command line uses the real ones."""
    out, err = out or sys.stdout, err or sys.stderr

    def say(message: str) -> None:
        print(message, file=out, flush=True)

    def complain(message: str) -> None:
        print(message, file=err, flush=True)

    started = clock()
    source_dsn = source_dsn or CdcSettings().dsn
    try:
        credentials = require_credentials(environ)
        catalog = (catalog_factory or open_catalog)(settings)
        check_catalog(catalog, settings)                              # the cheap checks first: no Google operation yet
        if not landing_only:
            check_source(source_dsn, settings)
        reader = (landing_factory or open_landing)(settings, environ)
        say(f"verifying gs://{settings.bucket} against gs://{settings.landing_bucket}/{reader.prefix}"
            + ("" if landing_only else f" and the source database {_where(source_dsn)}"))
        say(f"  Bronze is read through the catalog {settings.catalog_file}; {settings.read_workers} reads at a time "
            f"(credentials file {credentials})")
        if landing_only:
            say(f"  {NOT_FULL_PROOF}")
        report = check_fn(catalog, reader, None if landing_only else source_dsn, source_db=settings.source_db,
                          say=complain, workers=settings.read_workers)
    except BaseException as error:               # every way out becomes an exit code and one line
        if isinstance(error, SystemExit):
            raise
        complain(f"error: {reason(error)}")
        if exit_code(error) == EXIT_ERROR and not isinstance(error, BronzeError):
            traceback.print_exception(error, file=err)       # an unexpected failure is a bug: show where
        return exit_code(error)

    say(render(report, landing_only=landing_only))
    seconds = clock() - started
    say("")
    say(f"time {int(seconds // 60)} min {int(seconds % 60)} s, peak memory {_megabytes(peak_memory_bytes())} "
        f"of {_megabytes(machine_memory_bytes())} on this machine")
    if report.ok:
        if landing_only:
            say(f"OK (landing only): every landed event is in Bronze exactly once. {NOT_FULL_PROOF}")
        else:
            say(f"VERIFIED: Bronze equals the source database {settings.source_db} in all five tables by digest, every landed "
                f"event is in Bronze exactly once, and the {report.updates:,} updates in Bronze are the "
                f"{report.disbursed:,} disbursed loans")
        return EXIT_OK
    for problem in report.problems:
        print(f"PROBLEM: {problem}", file=err, flush=True)
    if report.moved:
        complain(f"error: NOT PROVEN: the source changed during the check (tables: {', '.join(report.moved)}). This is not a "
                 "Bronze mismatch and not a pass: run it again when the source is quiet"
                 + (f"; {len(report.problems)} other problems were also found" if report.problems else ""))
    else:
        complain(f"error: NOT VERIFIED: {len(report.problems)} problems")
    return EXIT_CONTRACT


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--landing-only", is_flag=True,
              help="Check only that Bronze holds the landed events, not the source database. NOT full proof; the "
                   "result says so.")
def cli(landing_only: bool) -> None:
    """Check that Bronze is correct: it holds every landed event once, and replayed into a blank database it equals the
    source database. Reads only. Needs GOOGLE_APPLICATION_CREDENTIALS set to the bronze-writer credentials file."""
    sys.exit(execute(BronzeSettings(), landing_only=landing_only))


def main(argv: Optional[list[str]] = None) -> int:
    """Run the command line; returns the exit code. A usage error is exit 1, not click's usual 2, because 2 means
    "not verified" here."""
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
