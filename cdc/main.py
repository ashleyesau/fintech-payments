"""
main.py: the command line (M4-Q7).

    python -m cdc.main --once      drain the slot, then exit
    python -m cdc.main --follow    drain, then look again every CDC_POLL_INTERVAL_SECONDS until stopped

Everything else comes from the CDC_ settings (cdc/config.py, .env). Exit codes:

    0    drained (--once), or stopped cleanly with Ctrl-C
    1    anything else that is not a contract stop: a lost database connection, a missing slot, a
         usage error, a bug
    2    a contract stop: a delete or truncate, schema drift, an undecodable message, or an
         existing object whose events differ from this batch
    3    the sink failed: an upload or read gave up after its retries, or the credentials are missing
    4    the slot is lost (Postgres removed the WAL it held; it cannot be repaired)
    130  a second Ctrl-C: stopped at once, nothing more confirmed

Every non-zero exit prints one line to stderr saying why and never confirms anything it has not
landed, so the next run replays the batch in hand. The first Ctrl-C finishes the batch in hand
(landed and confirmed), then exits 0. A second one stops at once; uploads already running still
have to end before the process can exit. No credentials are ever printed.
"""
from __future__ import annotations

import signal
import sys
import threading
import traceback
from typing import Callable, Optional, TextIO

import click
import psycopg

from cdc.config import CdcSettings
from cdc.consumer import Consumer, RunSummary
from cdc.errors import CdcError, SinkError, SlotLost, SlotMissing
from cdc.gcs import GcsSink
from cdc.lsn import format_lsn
from cdc.reader import Reader
from cdc.sink import Sink

EXIT_OK, EXIT_ERROR, EXIT_CONTRACT, EXIT_SINK, EXIT_SLOT_LOST, EXIT_INTERRUPTED = 0, 1, 2, 3, 4, 130


def exit_code(error: BaseException) -> int:
    if isinstance(error, KeyboardInterrupt):
        return EXIT_INTERRUPTED
    if isinstance(error, SlotMissing):
        return EXIT_ERROR
    if isinstance(error, SlotLost):
        return EXIT_SLOT_LOST
    if isinstance(error, SinkError):
        return EXIT_SINK
    if isinstance(error, CdcError):          # every other stop-without-confirming error is a contract stop
        return EXIT_CONTRACT
    return EXIT_ERROR


def reason(error: BaseException) -> str:
    """One plain line saying why the consumer stopped."""
    text = " ".join(str(error).split())
    if isinstance(error, KeyboardInterrupt):
        return ("interrupted again, so it stopped at once without confirming; the slot keeps its position and "
                "the next run replays the batch in hand")
    if isinstance(error, psycopg.OperationalError):
        return f"lost or could not make the database connection: {text}. The slot keeps its position; restart it"
    if isinstance(error, CdcError):
        return text
    return f"unexpected {type(error).__name__}: {text}"


def _connect_reader(settings: CdcSettings) -> Reader:
    return Reader(settings.dsn, settings.slot_name, settings.publication)


def _connect_sink(settings: CdcSettings) -> Sink:
    return GcsSink.connect(settings.gcs_bucket)


def execute(settings: CdcSettings, follow: bool, *,
            reader_factory: Callable[[CdcSettings], object] = _connect_reader,
            sink_factory: Callable[[CdcSettings], Sink] = _connect_sink,
            out: Optional[TextIO] = None, err: Optional[TextIO] = None,
            handle_signals: bool = True) -> int:
    """Run the consumer and return the exit code. The factories exist so tests can supply a scripted
    reader and an in-memory sink; the command line uses the real ones."""
    out, err = out or sys.stdout, err or sys.stderr

    def say(message: str) -> None:
        print(message, file=out, flush=True)

    def complain(message: str) -> None:
        print(message, file=err, flush=True)

    previous = None
    try:
        reader = reader_factory(settings)
        try:
            sink = sink_factory(settings)
            consumer = Consumer(reader, sink, database=settings.db, prefix=settings.gcs_prefix,
                                batch_size=settings.batch_size, workers=settings.upload_workers,
                                poll_interval=settings.poll_interval_seconds, log=say, warn=complain)
            say(f"slot {settings.slot_name} in {settings.db} to gs://{settings.gcs_bucket}/{settings.gcs_prefix} "
                f"({'following' if follow else 'draining once'}; batches of {settings.batch_size:,} messages, "
                f"{settings.upload_workers} uploads at a time"
                + (f", looking again every {settings.poll_interval_seconds:g} s" if follow else "") + ")")
            if handle_signals and threading.current_thread() is threading.main_thread():
                previous = _stop_on_first_interrupt(consumer, complain)
            summary = consumer.run(follow=follow)
        finally:
            if hasattr(reader, "close"):
                reader.close()
    except BaseException as error:           # every way out becomes an exit code and one line
        if isinstance(error, SystemExit):
            raise
        complain(f"error: {reason(error)}")
        if exit_code(error) == EXIT_ERROR and not isinstance(error, (CdcError, psycopg.OperationalError)):
            traceback.print_exception(error, file=err)       # an unexpected failure is a bug: show where
        return exit_code(error)
    finally:
        if previous is not None:
            signal.signal(signal.SIGINT, previous)
    say(_summary(summary))
    return EXIT_OK


def _stop_on_first_interrupt(consumer: Consumer, complain: Callable[[str], None]):
    count = [0]

    def handler(signum, frame):
        count[0] += 1
        if count[0] == 1:
            consumer.request_stop()
            complain("stopping after the batch in hand is landed and confirmed (Ctrl-C again to stop at once, "
                     "without confirming)")
        else:
            raise KeyboardInterrupt

    return signal.signal(signal.SIGINT, handler)


def _summary(summary: RunSummary) -> str:
    where = f"; slot confirmed to {format_lsn(summary.confirmed_lsn)}" if summary.confirmed_lsn is not None else ""
    if summary.batches == 0:
        what = "nothing was waiting in the slot"
    else:
        what = (f"{summary.changes:,} changes from {summary.transactions:,} transactions in {summary.batches:,} "
                f"batches ({summary.objects_created:,} objects new, {summary.objects_replayed:,} already there)")
    prefix = "stopped on request" if summary.stopped_by_request else "done"
    return f"{prefix}: {what}{where}"


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--once", is_flag=True, help="Drain the slot, then exit.")
@click.option("--follow", is_flag=True, help="Drain, then look again every CDC_POLL_INTERVAL_SECONDS until Ctrl-C.")
def cli(once: bool, follow: bool) -> None:
    """Land the changes waiting in the replication slot as gzipped NDJSON objects in GCS, confirming the
    slot only after they have landed. Settings come from the CDC_ environment variables or .env."""
    if once == follow:
        raise click.UsageError("choose exactly one of --once or --follow")
    sys.exit(execute(CdcSettings(), follow=follow))


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
