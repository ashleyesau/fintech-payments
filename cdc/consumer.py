"""
consumer.py: the loop. Fetch, assemble, land, then confirm (M4-Q6).

One batch is: fetch whole transactions from the reader, turn them into objects, upload every
object, and only then confirm the slot to the `end_lsn` of the last transaction. Nothing
confirms earlier, so a crash or a failed upload at any point before the confirm replays the
whole batch on the next run, and the replay is absorbed (landing.land_object, M4-Q5). A crash
after the confirm loses nothing, because everything had already landed.

A flush uploads its objects in parallel, `workers` at a time (M4-Q4a). When one fails the
objects already uploading finish, the ones not yet started are skipped, and the failure is
raised, so the batch is not confirmed. Retrying transient failures is the sink's job.

The loop checks the slot before every batch (M4-Q8): it stops if the slot is lost, and warns
once when the WAL headroom falls below half the cap, below a tenth, or the slot is unreserved.
A stop request (the first Ctrl-C) is honoured between batches and while waiting, never in the
middle of a flush, so the current batch is landed and confirmed first.
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional, Protocol

from cdc.errors import DecodeError
from cdc.landing import Object, build_batch, land_object
from cdc.lsn import format_lsn
from cdc.reader import SlotStatus
from cdc.sink import Sink
from cdc.transactions import TransactionAssembler


class SlotReader(Protocol):
    slot_name: str

    def fetch(self, limit: int) -> list[tuple[int, bytes]]: ...
    def confirm(self, end_lsn: int) -> None: ...
    def status(self) -> SlotStatus: ...
    def lost_error(self) -> Exception: ...


def _quiet(message: str) -> None:
    pass


@dataclass(frozen=True, slots=True)
class Flushed:
    created: int
    replayed: int


def flush(sink: Sink, objects: list[Object], workers: int) -> Flushed:
    """Land every object, `workers` at a time. All of them have landed, or this raises (the first
    failure in object order). After a failure, uploads already running finish and the rest are
    skipped; a second interrupt (KeyboardInterrupt) abandons what has not started."""
    if not objects:
        return Flushed(0, 0)
    failed = threading.Event()

    def one(obj: Object) -> Optional[str]:
        if failed.is_set():
            return None
        try:
            return land_object(sink, obj)
        except BaseException:
            failed.set()
            raise

    pool = ThreadPoolExecutor(max_workers=min(workers, len(objects)))
    futures = [pool.submit(one, obj) for obj in objects]
    try:
        wait(futures)
    except BaseException:
        failed.set()
        pool.shutdown(wait=False, cancel_futures=True)
        raise
    pool.shutdown()
    outcomes = [future.result() for future in futures]       # raises the first failure in object order
    return Flushed(outcomes.count("created"), outcomes.count("replayed"))


def _mb(size: int) -> str:
    return f"{size / 1024 / 1024:,.0f} MB"


class LagWatch:
    """Warns when the slot is falling behind (M4-Q8). Each level warns once; when the slot is back
    above half the cap the levels are re-armed, so they can warn again."""

    def __init__(self, slot_name: str) -> None:
        self.slot_name = slot_name
        self._warned: set[str] = set()

    @staticmethod
    def level(status: SlotStatus) -> str:
        if status.wal_status == "unreserved":
            return "unreserved"
        if status.cap_bytes is None or status.safe_wal_size is None:
            return "ok"
        if status.safe_wal_size < status.cap_bytes / 10:
            return "tenth"
        if status.safe_wal_size < status.cap_bytes / 2:
            return "half"
        return "ok"

    def check(self, status: SlotStatus) -> list[str]:
        level = self.level(status)
        if level == "ok":
            self._warned.clear()
            return []
        warnings = []
        # a slot that is already very low has passed the milder level too; say the worst once
        if level not in self._warned:
            warnings.append(self._message(level, status))
        self._warned.add(level)
        if level == "tenth":
            self._warned.add("half")
        return warnings

    def _message(self, level: str, status: SlotStatus) -> str:
        left = _mb(max(status.safe_wal_size or 0, 0))
        cap = _mb(status.cap_bytes) if status.cap_bytes else "unlimited"
        tail = ("If the headroom runs out Postgres drops the slot, and a lost slot cannot be repaired. "
                "Pause the generator or let the consumer catch up.")
        if level == "half":
            return (f"WARNING: slot {self.slot_name} has {left} of WAL headroom left, under half of the {cap} cap. "
                    f"The consumer is falling behind the writer. {tail}")
        if level == "tenth":
            return (f"WARNING: slot {self.slot_name} has {left} of WAL headroom left, under a tenth of the {cap} "
                    f"cap. {tail}")
        return (f"WARNING: slot {self.slot_name} is unreserved: it is past the {cap} cap but not lost yet, and "
                f"Postgres can invalidate it at the next checkpoint. {tail}")


@dataclass(frozen=True, slots=True)
class BatchResult:
    transactions: int
    changes: int
    objects: tuple[str, ...]   # the keys landed, in order
    created: int
    replayed: int
    confirmed_lsn: int


@dataclass(slots=True)
class RunSummary:
    batches: int = 0
    transactions: int = 0
    changes: int = 0
    objects_created: int = 0
    objects_replayed: int = 0
    idle_polls: int = 0
    confirmed_lsn: Optional[int] = None
    stopped_by_request: bool = False


class Consumer:
    def __init__(self, reader: SlotReader, sink: Sink, *, database: str, prefix: str,
                 batch_size: int = 10_000, workers: int = 8, poll_interval: float = 60.0,
                 log: Callable[[str], None] = _quiet, warn: Callable[[str], None] = _quiet,
                 clock: Optional[Callable[[], datetime]] = None) -> None:
        self.reader, self.sink = reader, sink
        self.database, self.prefix = database, prefix
        self.batch_size, self.workers, self.poll_interval = batch_size, workers, poll_interval
        self.log, self.warn, self.clock = log, warn, clock
        self.watch = LagWatch(reader.slot_name)
        self._stop = threading.Event()

    def request_stop(self) -> None:
        """Finish the batch in hand (landed and confirmed), then stop. Safe from a signal handler."""
        self._stop.set()

    @property
    def stop_requested(self) -> bool:
        return self._stop.is_set()

    def check_slot(self) -> None:
        status = self.reader.status()
        if status.lost:
            raise self.reader.lost_error()
        for message in self.watch.check(status):
            self.warn(message)

    def process_batch(self) -> Optional[BatchResult]:
        """One fetch, land, confirm. None when the slot had nothing waiting."""
        messages = self.reader.fetch(self.batch_size)
        if not messages:
            return None
        # a new assembler per fetch: Relation messages come again with each read, so no state is carried
        assembler = TransactionAssembler()
        transactions = [t for lsn, data in messages if (t := assembler.feed(lsn, data)) is not None]
        if assembler.in_transaction or not transactions:
            raise DecodeError("a fetch ended inside a transaction; the reader must return whole transactions")
        objects = build_batch(transactions, self.database, self.prefix, self.clock() if self.clock else None)
        flushed = flush(self.sink, objects, self.workers)
        end_lsn = transactions[-1].end_lsn        # the Commit's end_lsn, never its commit_lsn
        self.reader.confirm(end_lsn)
        changes = sum(len(t.changes) for t in transactions)
        self.log(f"landed {changes:,} changes from {len(transactions):,} transactions as {len(objects):,} objects "
                 f"({flushed.created} new, {flushed.replayed} already there); slot confirmed to {format_lsn(end_lsn)}")
        return BatchResult(len(transactions), changes, tuple(o.key for o in objects),
                           flushed.created, flushed.replayed, end_lsn)

    def run(self, follow: bool) -> RunSummary:
        """Drain the slot, then stop (`follow` False) or wait `poll_interval` and look again until
        a stop is requested. Raises on anything that must stop the consumer; the slot is never
        confirmed past what has landed."""
        summary = RunSummary()
        while not self._stop.is_set():
            self.check_slot()
            result = self.process_batch()
            if result is None:
                summary.idle_polls += 1
                if not follow:
                    break
                self._stop.wait(self.poll_interval)
                continue
            if summary.confirmed_lsn is not None and result.confirmed_lsn <= summary.confirmed_lsn:
                # A confirm that does not move the slot forward returns the same transaction on every
                # read, for ever (M2: this is what confirming a commit LSN does). Stop instead.
                raise RuntimeError(
                    f"the slot is not advancing: this batch ended at {format_lsn(result.confirmed_lsn)}, "
                    f"which is not past the last confirmed position {format_lsn(summary.confirmed_lsn)}")
            summary.batches += 1
            summary.transactions += result.transactions
            summary.changes += result.changes
            summary.objects_created += result.created
            summary.objects_replayed += result.replayed
            summary.confirmed_lsn = result.confirmed_lsn
        summary.stopped_by_request = self._stop.is_set()
        return summary
