"""
lag.py: how far behind the slot is, from Postgres alone (M6-Q1). No bucket, no Google, no cost.

    python -m cdc.lag               one report
    python -m cdc.lag --watch 10    one compact line every 10 seconds until Ctrl-C

It says where the slot is (its confirmed position), where the server's WAL is now, how far behind that
is, how much WAL the slot is holding, the cap and the headroom left, and the same warning level the
consumer uses. Exit codes: 0 for a slot reported at any level but lost (a warning level is information
here, not a failure), 4 for a lost slot, 1 for a missing slot, a lost connection or a usage error.
Settings are the CDC_ variables, as for the consumer. It is the pre-flight check before a big run.
"""
from __future__ import annotations

import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional, TextIO

import click
import psycopg

from cdc.config import CdcSettings
from cdc.consumer import LagWatch
from cdc.errors import SlotLost, SlotMissing
from cdc.lsn import format_lsn
from cdc.reader import Reader, SlotStatus

EXIT_OK, EXIT_ERROR, EXIT_SLOT_LOST = 0, 1, 4

LEVELS = {
    "ok": "healthy",
    "half": "under half the cap is left: the consumer is falling behind the writer",
    "tenth": "under a tenth of the cap is left: pause the generator or let the consumer catch up",
    "unreserved": "past the cap but not lost yet: Postgres can invalidate it at the next checkpoint",
    "lost": "LOST: Postgres removed the WAL the slot held and it cannot be repaired; regenerate the run with the "
            "same seed into a fresh database, a new slot name and so a new bucket prefix",
    "nocap": "no cap (max_slot_wal_keep_size is unlimited): an unread slot can fill the disk",
}


@dataclass(frozen=True, slots=True)
class LagReport:
    slot: str
    database: str
    status: SlotStatus
    current_lsn: int
    level: str

    @property
    def behind_bytes(self) -> int:
        """WAL written since the slot's confirmed position (not all of it is the slot's changes)."""
        return max(self.current_lsn - self.status.confirmed_flush_lsn, 0)

    @property
    def held_bytes(self) -> Optional[int]:
        """WAL the slot is keeping from its restart position, None once it is lost."""
        restart = self.status.restart_lsn
        return None if restart is None else max(self.current_lsn - restart, 0)


def take_report(reader: Reader, database: str) -> LagReport:
    status = reader.status()
    current = reader.wal_position()
    if status.lost:
        level = "lost"
    else:
        level = LagWatch.level(status)
        if level == "ok" and status.cap_bytes is None:
            level = "nocap"
    return LagReport(reader.slot_name, database, status, current, level)


def size(n: Optional[int]) -> str:
    if n is None:
        return "none"
    sign, n = ("-" if n < 0 else ""), abs(n)
    if n < 1024:
        return f"{sign}{n} bytes"
    if n < 1024 * 1024:
        return f"{sign}{n / 1024:,.0f} kB"
    return f"{sign}{n / 1024 / 1024:,.1f} MB"


def format_report(report: LagReport) -> str:
    s = report.status
    cap = "unlimited" if s.cap_bytes is None else size(s.cap_bytes)
    headroom = "none (the slot is lost)" if report.level == "lost" else size(s.safe_wal_size) if s.safe_wal_size is not None else "unlimited"
    restart = "none" if s.restart_lsn is None else format_lsn(s.restart_lsn)
    return "\n".join([
        f"slot {report.slot} in {report.database}: {s.wal_status}",
        f"  confirmed position  {format_lsn(s.confirmed_flush_lsn)}",
        f"  WAL position now    {format_lsn(report.current_lsn)}",
        f"  behind              {size(report.behind_bytes)} of WAL written since the confirmed position",
        f"  held by the slot    {size(report.held_bytes)} (from its restart position {restart})",
        f"  cap and headroom    cap {cap}, headroom {headroom}",
        f"  level               {LEVELS[report.level]}",
    ])


def format_line(report: LagReport, moment: datetime) -> str:
    s = report.status
    headroom = size(s.safe_wal_size) if s.safe_wal_size is not None else ("none" if report.level == "lost" else "unlimited")
    return (f"{moment:%H:%M:%S}  {s.wal_status:<10}  behind {size(report.behind_bytes):>10}  "
            f"held {size(report.held_bytes):>10}  headroom {headroom:>10}  {report.level}")


def _connect(settings: CdcSettings) -> Reader:
    return Reader(settings.dsn, settings.slot_name, settings.publication)


def execute(settings: CdcSettings, watch: Optional[float] = None, *,
            reader_factory: Callable[[CdcSettings], Reader] = _connect,
            out: Optional[TextIO] = None, err: Optional[TextIO] = None,
            sleep: Callable[[float], None] = time.sleep,
            clock: Callable[[], datetime] = datetime.now) -> int:
    """Report once, or every `watch` seconds until Ctrl-C. Returns the exit code."""
    out, err = out or sys.stdout, err or sys.stderr
    try:
        reader = reader_factory(settings)
        try:
            header_printed = False
            while True:
                report = take_report(reader, settings.db)
                if watch is None:
                    print(format_report(report), file=out, flush=True)
                else:
                    if not header_printed:
                        print(f"slot {report.slot} in {report.database}, every {watch:g} s (Ctrl-C to stop)", file=out)
                        header_printed = True
                    print(format_line(report, clock()), file=out, flush=True)
                if report.level == "lost":
                    return EXIT_SLOT_LOST
                if watch is None:
                    return EXIT_OK
                sleep(watch)
        finally:
            reader.close()
    except KeyboardInterrupt:
        return EXIT_OK
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        text = " ".join(str(error).split())
        if isinstance(error, SlotMissing):
            line = text
        elif isinstance(error, psycopg.OperationalError):
            line = f"lost or could not make the database connection: {text}"
        else:
            line = f"unexpected {type(error).__name__}: {text}"
        print(f"error: {line}", file=err, flush=True)
        if not isinstance(error, (SlotMissing, psycopg.OperationalError)):
            traceback.print_exception(error, file=err)
        return EXIT_ERROR


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--watch", type=float, default=None, metavar="SECONDS",
              help="Print one line every SECONDS until Ctrl-C, instead of one report.")
def cli(watch: Optional[float]) -> None:
    """How far behind the replication slot is: its position, the WAL it holds, the headroom left against
    max_slot_wal_keep_size and the warning level. Reads Postgres only. Settings come from the CDC_ variables."""
    if watch is not None and watch <= 0:
        raise click.UsageError("--watch needs a number of seconds above zero")
    sys.exit(execute(CdcSettings(), watch))


def main(argv: Optional[list[str]] = None) -> int:
    """Returns the exit code. A usage error is exit 1, as for the consumer (2 is not used here)."""
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
