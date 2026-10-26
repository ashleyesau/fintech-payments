"""
streaming.py: the streaming loop. It generates a day exactly as backfill does
(advance_day), then replays the day's rows in time order across all merchants, sleeping
the gap between rows divided by the speed factor, and writes each row on its own through
StreamingWriter.

The replay is paced; generating the next day is not, so each day starts when the
previous one has been written and checkpointed. The Poisson timestamps already give the
arrivals their exponential spacing, so no separate delay is drawn.

A transaction and the risk events that point at it commit together. If the writer cannot
keep up with the schedule it writes late and logs a warning; it never drops a row.

Stream mode continues from a checkpoint when one exists (a backfill's, for instance) and
otherwise starts fresh from the start date. A resumed stream writes with skip_duplicates,
so a day that crashed part-way replays safely.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Optional

import psycopg

from simulator.common import SAST, end_of_day
from simulator.db import StreamingWriter
from simulator.runner import DayResult, RunError, RunSettings, RunSummary, advance_day, start_run, summarise
from simulator.state import save_checkpoint

log = logging.getLogger("simulator")

# A write this far behind its slot is worth a warning.
LAG_WARNING_SECONDS = 1.0

# Tie order for rows with the same timestamp: an application exists before its
# disbursement update, which comes before any repayment on the loan.
_RANK = {"transaction": 0, "risk": 1, "application": 2, "update": 3, "repayment": 4}
_UNPAID_ATTEMPT_HOUR = 8  # repayment rows with no payment_ts: the collection window, SAST


class Pacer:
    """Sleeps so that simulated time passes `speed_factor` times faster than real time.

    Deadlines come from one anchor, not from the previous write, so slow writes do not
    push every later row back: a writer that has fallen behind catches up by not sleeping.
    The clock and sleep are injectable so tests run without waiting.
    """

    def __init__(
        self,
        speed_factor: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if speed_factor <= 0:
            raise ValueError("speed_factor must be positive")
        self.speed_factor = speed_factor
        self._clock = clock
        self._sleep = sleep
        self._anchor_wall = 0.0
        self._anchor_sim: Optional[datetime] = None
        self._cursor: Optional[datetime] = None

    def start(self, sim_time: datetime) -> None:
        self._anchor_wall = self._clock()
        self._anchor_sim = sim_time
        self._cursor = sim_time

    def wait_until(self, sim_time: datetime) -> float:
        """Sleep until `sim_time` is due and return how many seconds late the caller is
        (0 when on schedule). A time earlier than one already waited for is treated as
        that earlier time, so lateness is never inflated by out-of-order timestamps."""
        if self._anchor_sim is None:
            raise RuntimeError("Pacer.start() must be called first")
        target = max(sim_time, self._cursor)
        self._cursor = target
        deadline = self._anchor_wall + (target - self._anchor_sim).total_seconds() / self.speed_factor
        now = self._clock()
        if deadline > now:
            self._sleep(deadline - now)
            return 0.0
        return now - deadline


@dataclass
class TimedRow:
    when: datetime
    kind: str
    row: dict
    # For a transaction: the risk events that point at it, committed in the same transaction.
    attached: list = field(default_factory=list)


def build_timeline(result: DayResult) -> list[TimedRow]:
    """Every row of the day in the order it should be written."""
    day_fallback = datetime(result.day.year, result.day.month, result.day.day,
                            _UNPAID_ATTEMPT_HOUR, tzinfo=SAST)
    items: list[TimedRow] = []
    for merchant_day in result.merchant_days:
        by_transaction: dict[str, list[dict]] = {}
        known = {event["transaction_id"] for event in merchant_day.events}
        for risk_row in merchant_day.risk_rows:
            if risk_row["entity_id"] in known:
                by_transaction.setdefault(risk_row["entity_id"], []).append(risk_row)
            else:
                items.append(TimedRow(risk_row["action_ts"], "risk", risk_row))
        for event in merchant_day.events:
            items.append(TimedRow(event["created_at"], "transaction", event,
                                  by_transaction.get(event["transaction_id"], [])))
        lending = merchant_day.lending
        for row in lending.get("loan_applications", []):
            items.append(TimedRow(row["decision_ts"], "application", row))
        for row in lending.get("loan_application_updates", []):
            items.append(TimedRow(row["disbursed_at"], "update", row))
        for row in lending.get("loan_repayments", []):
            items.append(TimedRow(row["payment_ts"] or day_fallback, "repayment", row))
    items.sort(key=lambda item: (item.when, _RANK[item.kind]))
    return items


def write_row(writer: StreamingWriter, item: TimedRow) -> None:
    if item.kind == "transaction":
        writer.write_events([item.row], item.attached)
    elif item.kind == "risk":
        writer.write("risk_events", [item.row])
    elif item.kind == "application":
        writer.write("loan_applications", [item.row])
    elif item.kind == "update":
        writer.disburse(item.row)
    else:
        writer.write("loan_repayments", [item.row])


def replay_day(result: DayResult, writer: StreamingWriter, pacer: Pacer) -> float:
    """Write the day's rows on schedule, then its snapshots at 23:59. Returns the worst
    lateness in seconds."""
    day = result.day
    pacer.start(datetime(day.year, day.month, day.day, tzinfo=SAST))
    worst = 0.0
    timeline = build_timeline(result)
    for item in timeline:
        worst = max(worst, pacer.wait_until(item.when))
        write_row(writer, item)
    pacer.wait_until(end_of_day(day))
    writer.write_snapshots(result.snapshots)
    writer.end_of_day()
    if worst > LAG_WARNING_SECONDS:
        log.warning(
            "%s: the writer was up to %.1fs behind the schedule over %d rows; rows were written late, "
            "none dropped. Fewer merchants or a lower speed factor keeps it on schedule.",
            day, worst, len(timeline),
        )
    return worst


def run_stream(
    run: RunSettings,
    conn: psycopg.Connection,
    duration_days: int,
    speed_factor: float,
    pacer: Optional[Pacer] = None,
) -> RunSummary:
    """Stream `duration_days` simulated days, from the checkpoint if one exists."""
    if duration_days < 1:
        raise RunError("--duration-days must be at least 1.")
    resume = os.path.exists(run.checkpoint_path)
    merchants, first_day = start_run(run, conn, resume)
    writer = StreamingWriter(conn, skip_duplicates=resume)
    pacer = pacer or Pacer(speed_factor)

    started = time.monotonic()
    day = first_day
    stop = first_day + timedelta(days=duration_days)
    while day < stop:
        result = advance_day(merchants, day, run)
        replay_day(result, writer, pacer)
        save_checkpoint(merchants, day, run.flags, run.checkpoint_path, extra=run.extra())
        log.info("%s: streamed %d merchant-days, %d merchants onboarded", day, len(result.merchant_days), len(merchants))
        day += timedelta(days=1)
    return summarise(merchants, first_day, day, writer.written, started, resume, run.checkpoint_path)
