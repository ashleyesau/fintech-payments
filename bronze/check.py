"""
check.py: python -m bronze.check, the read-only look at the landed objects (GCS Plan (V2) G1).

    python -m bronze.check

Runs pass 1 over gs://<landing bucket>/cdc/<slot>/ (bronze.index): lists the prefix once, reads every
object once, checks every event, and prints what is there. It writes nothing, anywhere. It is how
G1's done-when is shown on the real bucket: object and event counts, per table and per operation,
duplicates, incomplete transactions, how long it took and how much memory it needed.

Run it with GOOGLE_APPLICATION_CREDENTIALS pointing at the bronze-writer credentials file; it refuses
to start without it (bronze.config.require_credentials). Settings are the BRONZE_ ones and, for the
landing bucket and slot, the CDC_ ones (bronze/config.py, .env). Exit codes:

    0    every object is valid and every transaction is complete
    1    anything else that is not a contract stop: missing credentials, a usage error, a bug
    2    a contract stop: a name or an event the contract does not allow, two copies of one change that
         disagree, an incomplete transaction (the summary is still printed for the last one)
    3    the bucket failed: a list or read gave up after its retries, or Google rejected the account
    130  interrupted with Ctrl-C

Every non-zero exit prints one line to stderr saying why. Nothing in a credentials file is printed,
only its path.
"""
from __future__ import annotations

import os
import resource
import sys
import time
import traceback
from typing import Callable, Optional, TextIO

import click

from bronze.config import BronzeSettings, require_credentials
from bronze.errors import BronzeError, ConfigError, ContractError, IncompleteTransactions
from bronze.index import IndexSummary, build_index
from bronze.landing import LandingReader, open_landing
from cdc.errors import SinkError

EXIT_OK, EXIT_ERROR, EXIT_CONTRACT, EXIT_BUCKET, EXIT_INTERRUPTED = 0, 1, 2, 3, 130
PROGRESS_EVERY = 100


def exit_code(error: BaseException) -> int:
    if isinstance(error, KeyboardInterrupt):
        return EXIT_INTERRUPTED
    if isinstance(error, SinkError):
        return EXIT_BUCKET
    if isinstance(error, ContractError):
        return EXIT_CONTRACT
    return EXIT_ERROR


def reason(error: BaseException) -> str:
    """One plain line saying why the command stopped."""
    text = " ".join(str(error).split())
    if isinstance(error, KeyboardInterrupt):
        return "interrupted; nothing was written"
    if isinstance(error, (BronzeError, SinkError)):
        return text
    return f"unexpected {type(error).__name__}: {text}"


def peak_memory_bytes() -> int:
    """The most memory this process has held, from the operating system (bytes on macOS, kilobytes on Linux)."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


def machine_memory_bytes() -> Optional[int]:
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError, AttributeError):
        return None


def render(summary: IndexSummary) -> str:
    """The counts, as a block of text."""
    inserts = sum(c["c"] for c in summary.by_table.values())
    updates = sum(c["u"] for c in summary.by_table.values())
    width = max(len(t) for t in summary.by_table)
    lines = [
        f"objects        {summary.objects:,}",
        f"events         {summary.events_read:,}  ({summary.unique_events:,} unique, {summary.duplicates:,} duplicates)",
        f"transactions   {summary.transactions:,}  ({summary.incomplete:,} incomplete)",
        "",
        f"{'table':<{width}}  {'inserts (c)':>13}  {'updates (u)':>12}",
    ]
    for table, counts in summary.by_table.items():
        lines.append(f"{table:<{width}}  {counts['c']:>13,}  {counts['u']:>12,}")
    lines.append(f"{'total':<{width}}  {inserts:>13,}  {updates:>12,}")
    return "\n".join(lines)


def _megabytes(size: Optional[int]) -> str:
    return "unknown" if size is None else f"{size / 1_000_000:,.0f} MB"


def execute(settings: BronzeSettings, *,
            reader_factory: Optional[Callable[[BronzeSettings], LandingReader]] = None,
            environ: Optional[dict] = None,
            out: Optional[TextIO] = None, err: Optional[TextIO] = None,
            clock: Callable[[], float] = time.monotonic) -> int:
    """Run pass 1 and return the exit code. The factory exists so tests can supply a reader over a fake
    sink; the command line uses the real landing bucket."""
    out, err = out or sys.stdout, err or sys.stderr

    def say(message: str) -> None:
        print(message, file=out, flush=True)

    def complain(message: str) -> None:
        print(message, file=err, flush=True)

    started = clock()
    summary = None
    try:
        credentials = require_credentials(environ)
        reader = reader_factory(settings) if reader_factory else open_landing(settings, environ)
        say(f"reading gs://{settings.landing_bucket}/{reader.prefix} with {settings.read_workers} reads at a time "
            f"(credentials file {credentials})")

        def progress(done: int, total: int) -> None:
            if done % PROGRESS_EVERY == 0 or done == total:
                complain(f"  read {done:,} of {total:,} objects")

        index = build_index(reader, settings.read_workers, progress)
        summary = index.summary()
        say(render(summary))
        index.require_complete()
    except BaseException as error:           # every way out becomes an exit code and one line
        if isinstance(error, SystemExit):
            raise
        if summary is not None and isinstance(error, IncompleteTransactions):
            say("")
        complain(f"error: {reason(error)}")
        if exit_code(error) == EXIT_ERROR and not isinstance(error, (BronzeError, SinkError)):
            traceback.print_exception(error, file=err)       # an unexpected failure is a bug: show where
        return exit_code(error)
    seconds = clock() - started
    say("")
    say(f"time {int(seconds // 60)} min {int(seconds % 60)} s, peak memory {_megabytes(peak_memory_bytes())} "
        f"of {_megabytes(machine_memory_bytes())} on this machine")
    say("OK: every object is valid and every transaction is complete")
    return EXIT_OK


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
def cli() -> None:
    """Read every landed object once and print what is there. Writes nothing. Needs
    GOOGLE_APPLICATION_CREDENTIALS set to the bronze-writer credentials file."""
    sys.exit(execute(BronzeSettings()))


def main(argv: Optional[list[str]] = None) -> int:
    """Run the command line; returns the exit code. A usage error is exit 1, not click's usual 2,
    because 2 means a contract stop here."""
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
