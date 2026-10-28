"""
runner.py: the daily loop. advance_day() produces one simulated day for the whole
population; run_backfill() writes it to Postgres a day at a time and checkpoints after
each commit. The streaming loop reuses advance_day().

Per merchant and day the order is: simulate_day, evaluate_risk, simulate_lending_day,
evaluate_state, decay_risk. Never-activators are only checked for abandonment, and
merchants that have not reached their activation date are skipped. After the merchants,
the day's onboarding runs (the ramp, plus one replacement per merchant that became
Churned today from month 7), and then the end-of-day snapshots are built.

The end date is exclusive: 2025-01-01 to 2026-01-01 is 365 simulated days.

Restart safety: a day's rows are committed before its checkpoint is written, so a crash
between the two replays one day. The checkpoint carries numpy's RNG state, so the replay
regenerates the same rows, and a resumed run writes with skip_duplicates=True so the rows
that already reached the database are skipped.
"""
from __future__ import annotations

import logging
import os
import time
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import numpy as np
import psycopg

from simulator.db import BackfillWriter, snapshot_rows
from simulator.generator import simulate_day
from simulator.lending import simulate_lending_day
from simulator.onboarding import onboard_day
from simulator.risk import evaluate_risk
from simulator.state import Merchant, load_checkpoint, save_checkpoint

log = logging.getLogger("simulator")


class RunError(Exception):
    """The run cannot start or resume as asked. The message says what to do."""


@dataclass(frozen=True)
class RunSettings:
    start_date: date
    end_date: Optional[date]       # exclusive; None for streaming, which is bounded by duration
    merchant_count: int            # merchants onboarded over the ramp, not merchants active
    seed: int = 42
    enable_risk_events: bool = True
    enable_loan_applications: bool = True
    checkpoint_path: str = "checkpoint.json"

    @property
    def flags(self) -> dict:
        """Fixed for a run: a resume under different values is refused."""
        return {
            "enable_risk_events": self.enable_risk_events,
            "enable_loan_applications": self.enable_loan_applications,
        }

    def extra(self) -> dict:
        """What a resumed run needs besides the merchants: onboarding is a function of these."""
        return {
            "start_date": self.start_date,
            "merchant_count": self.merchant_count,
            "seed": self.seed,
        }


@dataclass
class MerchantDay:
    merchant_id: str
    events: list
    risk_rows: list
    lending: dict


@dataclass
class DayResult:
    day: date
    merchant_days: list[MerchantDay]
    churned: int                   # merchants that became Churned today
    onboarded: int                 # merchants that joined today
    snapshots: list[dict]


@dataclass
class RunSummary:
    first_day: date
    last_day: date
    days: int
    merchants_onboarded: int
    merchants_ever_transacted: int
    churned_never_activated: int   # Churned without ever transacting: a funnel drop-off, not churn
    churned_after_trading: int     # Churned having traded: the figure comparable to real attrition
    states: Counter
    rows_written: Counter
    elapsed_seconds: float
    resumed: bool
    checkpoint_path: str = ""


def advance_day(merchants: list[Merchant], day: date, run: RunSettings) -> DayResult:
    """Simulate one day for every merchant onboarded before it, then onboard today's.

    `merchants` is updated in place. Merchants onboarded today cannot transact today
    (activation is at least a day after onboarding), so they only appear in the
    snapshots.
    """
    merchant_days: list[MerchantDay] = []
    churned = 0
    for m in merchants:
        if m.never_activates:
            if m.abandon_onboarding(day):
                churned += 1
            continue
        if not m.can_trade(day):
            continue
        events = simulate_day(m, day)
        risk_rows = evaluate_risk(m, events, day) if run.enable_risk_events else []
        lending = simulate_lending_day(m, day) if run.enable_loan_applications else {}
        if m.evaluate_state(day) == "churned":
            churned += 1
        m.decay_risk(day)
        if events or risk_rows or any(lending.values()):
            merchant_days.append(MerchantDay(m.merchant_id, events, risk_rows, lending))

    joined = onboard_day(day, run.start_date, run.merchant_count, replacements=churned)
    merchants.extend(joined)
    return DayResult(day, merchant_days, churned, len(joined), snapshot_rows(merchants, day))


def database_is_empty(conn: psycopg.Connection) -> bool:
    return not conn.execute("SELECT EXISTS (SELECT 1 FROM merchants)").fetchone()[0]


def start_run(run: RunSettings, conn: psycopg.Connection, resume: bool) -> tuple[list[Merchant], date]:
    """The merchants to begin with and the first day to simulate.

    A fresh run seeds numpy and needs an empty database and no existing checkpoint, so it
    never overwrites or mixes with an earlier run. A resume loads the checkpoint, checks it
    belongs to this run, and restores the RNG.
    """
    if not resume:
        if os.path.exists(run.checkpoint_path):
            raise RunError(
                f"{run.checkpoint_path} already exists. Use --resume to continue that run, "
                "or point --checkpoint at a new file."
            )
        if not database_is_empty(conn):
            raise RunError(
                "The merchants table already has rows and there is no checkpoint to continue from. "
                "A fresh run needs an empty database; to continue an earlier run, use its checkpoint file."
            )
        np.random.seed(run.seed)
        return [], run.start_date

    try:
        checkpoint = load_checkpoint(run.checkpoint_path, expected_flags=run.flags)
    except FileNotFoundError:
        raise RunError(f"No checkpoint at {run.checkpoint_path}, so there is nothing to resume.") from None
    except ValueError as error:
        raise RunError(str(error)) from error
    for key, value in (("start_date", run.start_date), ("merchant_count", run.merchant_count)):
        if checkpoint.extra.get(key) != value:
            raise RunError(
                f"The checkpoint was written with {key}={checkpoint.extra.get(key)}, "
                f"this run has {key}={value}."
            )
    checkpoint.restore_rng()
    return checkpoint.merchants, checkpoint.last_day + timedelta(days=1)


def run_backfill(run: RunSettings, conn: psycopg.Connection, resume: bool = False) -> RunSummary:
    """Simulate every day from the start (or the checkpoint) up to the end date, writing
    each day to Postgres and checkpointing after its commit."""
    if run.end_date is None:
        raise RunError("A backfill needs an end date.")
    merchants, first_day = start_run(run, conn, resume)
    if first_day >= run.end_date:
        raise RunError(
            f"Nothing to do: the next day is {first_day} and the end date {run.end_date} is exclusive."
        )

    writer = BackfillWriter(conn, skip_duplicates=resume)
    started = time.monotonic()
    day = first_day
    while day < run.end_date:
        result = advance_day(merchants, day, run)
        for item in result.merchant_days:
            writer.write_merchant_day(item.events, item.risk_rows, item.lending)
        writer.write_snapshots(result.snapshots)
        writer.end_of_day()
        save_checkpoint(merchants, day, run.flags, run.checkpoint_path, extra=run.extra())
        if (day + timedelta(days=1)).day == 1:
            log.info(
                "%s: %d merchants onboarded, %d rows written, %.0fs elapsed",
                day, len(merchants), sum(writer.written.values()), time.monotonic() - started,
            )
        day += timedelta(days=1)
    writer.close()
    return summarise(merchants, first_day, day, writer.written, started, resume, run.checkpoint_path)


def summarise(
    merchants: list[Merchant], first_day: date, next_day: date, written: Counter,
    started: float, resumed: bool, checkpoint_path: str,
) -> RunSummary:
    """The end-of-run summary. `next_day` is the first day not simulated."""
    return RunSummary(
        first_day=first_day,
        last_day=next_day - timedelta(days=1),
        days=(next_day - first_day).days,
        merchants_onboarded=len(merchants),
        merchants_ever_transacted=sum(1 for m in merchants if m.first_transaction_at is not None),
        churned_never_activated=sum(
            1 for m in merchants if m.merchant_state == "churned" and m.never_activates
        ),
        churned_after_trading=sum(
            1 for m in merchants if m.merchant_state == "churned" and not m.never_activates
        ),
        states=Counter(m.merchant_state for m in merchants),
        rows_written=written,
        elapsed_seconds=time.monotonic() - started,
        resumed=resumed,
        checkpoint_path=checkpoint_path,
    )


def _churn_lines(summary: RunSummary) -> list[str]:
    """Churn split in two. Never-activators are churned by rule 30 days after onboarding;
    the Churned count over everything ever onboarded is cumulative, not a churn rate."""
    never, after = summary.churned_never_activated, summary.churned_after_trading
    onboarded, traded = summary.merchants_onboarded, summary.merchants_ever_transacted
    share = lambda n, d: f"{100 * n / d:.1f}%" if d else "n/a"
    return [
        f"Merchants churned: {never + after} ({share(never + after, onboarded)} of onboarded, cumulative)",
        f"  never activated: {never} ({share(never, onboarded)} of onboarded; signed up, never transacted)",
        f"  churned after trading: {after} ({share(after, traded)} of the {traded} that transacted)",
    ]


def format_summary(summary: RunSummary) -> str:
    """The end-of-run report."""
    lines = [
        f"{'Resumed' if summary.resumed else 'Fresh'} run: {summary.first_day} to {summary.last_day} "
        f"({summary.days} days) in {summary.elapsed_seconds:.0f}s",
        f"Merchants onboarded: {summary.merchants_onboarded}",
        f"Merchants that ever transacted: {summary.merchants_ever_transacted}",
        "Merchant states: " + ", ".join(f"{s} {n}" for s, n in sorted(summary.states.items())),
        *_churn_lines(summary),
        "Rows written this run:",
    ]
    lines += [f"  {name}: {count}" for name, count in sorted(summary.rows_written.items())]
    lines.append(f"Checkpoint: {summary.checkpoint_path}")
    return "\n".join(lines)
