"""
index.py: pass 1, the streaming index of the landed objects (GCS Plan (V2) section 4, G1 design decision 1).

build_index lists the landing prefix once, reads every object once (8 at a time), checks it
(bronze.events) and folds it into an Index. The Index never holds an event. Per transaction it keeps
the size and, for each position in it, an 8 byte content hash and the number of the object that holds
the first copy: about 12 bytes an event plus a small record per transaction, against about 270 bytes
an event for a dict of tuples (DECISION_LOG, "G1 design decision 1" and "G1 build notes").

Objects are read in parallel but folded in listing order, so the index does not depend on which read
finished first. "The first copy" of a duplicated change is therefore the one in the object that comes
first in the listing, always.

What stops the run, with nothing committed:
  - while building: an object that is not valid, an event that fails a structural check
    (bronze.events) or whose values do not map to a Bronze row (bronze.mapping, MappingError), two copies of one change that disagree (DisagreementError), a failed read
    (cdc.errors.SinkError, after the sink's own retries);
  - on the finished index: incomplete transactions (Index.require_complete, IncompleteTransactions).
Completeness cannot be decided until every object is read, so it is a question put to the finished
index, and the two methods pass 2 uses (keys, objects_missing) put it themselves: pass 2 cannot start
on an incomplete set.

How pass 2 finds its objects: every indexed key remembers the object that holds its first copy.
objects_missing(table, present) returns the objects of `table` that hold a key not in `present` (the
keys Bronze already has), so only those are read again, and object_of(key) says which copy of a
duplicated change to take.
"""
from __future__ import annotations

from array import array
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable, Container, Iterator, Optional

from bronze.errors import ConfigError, DisagreementError, IncompleteTransactions
from bronze.events import ObjectSummary, summarise_object
from bronze.landing import LandedObject, LandingReader
from cdc.columns import EXPECTED_COLUMNS
from cdc.dedup import Key

NO_OBJECT = -1
REPORTED = 10   # incomplete transactions named in the error message


class _Transaction:
    __slots__ = ("size", "hashes", "holders", "problem")

    def __init__(self, size: int) -> None:
        self.size = size
        self.hashes = array("Q", bytes(8 * size))
        self.holders = array("i", [NO_OBJECT]) * size   # object number of the first copy of each position
        self.problem = ""


@dataclass(frozen=True, slots=True)
class IndexSummary:
    objects: int
    events_read: int       # every event in every object, duplicates included
    unique_events: int
    duplicates: int        # copies beyond the first of a change that agree with it
    transactions: int
    incomplete: int
    by_table: dict         # table -> {"c": inserts, "u": updates}, unique events only


class Index:
    def __init__(self, objects: list[LandedObject]) -> None:
        self.objects = objects
        self._transactions: dict[int, _Transaction] = {}
        self._events_read = 0
        self._unique = 0
        self._duplicates = 0
        self._by_table = {table: {"c": 0, "u": 0} for table in EXPECTED_COLUMNS}

    # -- building ------------------------------------------------------------

    def add(self, number: int, summary: ObjectSummary) -> None:
        """Fold one object, which is objects[number], into the index. Call in listing order."""
        counts = self._by_table[summary.obj.table]
        for commit_lsn, position, size, op, digest in summary.rows:
            self._events_read += 1
            transaction = self._transactions.get(commit_lsn)
            if transaction is None:
                transaction = self._transactions[commit_lsn] = _Transaction(size)
            elif transaction.size != size:
                transaction.problem = transaction.problem or (
                    f"events disagree about the transaction size ({transaction.size} and {size})")
                continue
            holder = transaction.holders[position]
            if holder == NO_OBJECT:
                transaction.holders[position] = number
                transaction.hashes[position] = digest
                counts[op] += 1
                self._unique += 1
            elif transaction.hashes[position] != digest:
                raise DisagreementError(
                    f"commit_lsn {commit_lsn:016x} position {position} landed twice with different content, "
                    f"in {self.objects[holder].key} and {summary.obj.key}: copies may differ only in "
                    "ts_ms and ingested_at, so the landing contract was broken")
            else:
                self._duplicates += 1

    # -- completeness ---------------------------------------------------------

    def incomplete(self) -> dict[int, str]:
        """commit_lsn -> what is wrong, for every transaction that is not exactly positions 0 to size - 1."""
        problems = {}
        for commit_lsn, transaction in self._transactions.items():
            if transaction.problem:
                problems[commit_lsn] = transaction.problem
                continue
            absent = transaction.holders.count(NO_OBJECT)
            if absent:
                first = [str(p) for p, holder in enumerate(transaction.holders) if holder == NO_OBJECT][:5]
                more = f", first {', '.join(first)}" if first else ""
                problems[commit_lsn] = f"{absent} of {transaction.size} positions are missing{more}"
        return problems

    def require_complete(self) -> None:
        """Raise IncompleteTransactions unless every transaction has every position."""
        problems = self.incomplete()
        if not problems:
            return
        shown = [f"commit_lsn {lsn:016x}: {why}" for lsn, why in sorted(problems.items())[:REPORTED]]
        more = f" (and {len(problems) - REPORTED} more)" if len(problems) > REPORTED else ""
        raise IncompleteTransactions(
            f"{len(problems)} of {len(self._transactions)} transactions are incomplete{more}: " + "; ".join(shown) +
            ". The consumer may be mid-flush, or an object is missing; nothing was committed.")

    # -- what pass 2 asks -------------------------------------------------------

    def keys(self, table: str) -> Iterator[Key]:
        """The unique keys of `table`, in (commit_lsn, position) order. The index must be complete."""
        self.require_complete()
        yield from self._keys(table)

    def object_of(self, key: Key) -> LandedObject:
        """The object holding the first copy of this change. KeyError if the index has no such change."""
        commit_lsn, position = key
        transaction = self._transactions.get(commit_lsn)
        if transaction is None or not 0 <= position < transaction.size or transaction.holders[position] == NO_OBJECT:
            raise KeyError(key)
        return self.objects[transaction.holders[position]]

    def objects_missing(self, table: str, present: Container[Key]) -> list[LandedObject]:
        """The objects, in listing order, whose first copy of some key of `table` is not in `present`.
        Reading exactly these once more gives every key Bronze lacks. The index must be complete."""
        self.require_complete()
        needed = set()
        for lsn, position in self._keys(table):
            if (lsn, position) not in present:
                needed.add(self._transactions[lsn].holders[position])
        return [self.objects[number] for number in sorted(needed)]

    def _keys(self, table: str) -> Iterator[Key]:
        # Only called on a complete index, so every position has a holder.
        tables = [obj.table for obj in self.objects]
        for commit_lsn in sorted(self._transactions):
            for position, holder in enumerate(self._transactions[commit_lsn].holders):
                if tables[holder] == table:
                    yield commit_lsn, position

    # -- reporting ----------------------------------------------------------------

    def summary(self) -> IndexSummary:
        return IndexSummary(
            objects=len(self.objects), events_read=self._events_read, unique_events=self._unique,
            duplicates=self._duplicates, transactions=len(self._transactions),
            incomplete=len(self.incomplete()), by_table={t: dict(c) for t, c in self._by_table.items()})


def build_index(reader: LandingReader, workers: int = 8,
                progress: Optional[Callable[[int, int], None]] = None, *, source_db: str) -> Index:
    """List the landing prefix once and read every object once. Raises on the first object that fails;
    objects still being read are left to finish and their results dropped. `progress(done, total)` is
    called after each object is folded in. `source_db` is the database the events must come from; every
    event is mapped to a Bronze row on the way (bronze.mapping), so value drift stops here."""
    if workers < 1:
        raise ConfigError(f"workers must be at least 1, not {workers}")
    objects = reader.list_keys()
    index = Index(objects)

    def read_and_summarise(obj: LandedObject) -> ObjectSummary:
        return summarise_object(obj, reader.read(obj.key), source_db=source_db)

    window = 2 * workers
    pending: deque[Future] = deque()
    remaining = iter(objects)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        try:
            number = 0
            while True:
                while len(pending) < window:
                    try:
                        obj = next(remaining)
                    except StopIteration:
                        break
                    pending.append(pool.submit(read_and_summarise, obj))
                if not pending:
                    break
                index.add(number, pending.popleft().result())
                number += 1
                if progress is not None:
                    progress(number, len(objects))
        except BaseException:
            for future in pending:
                future.cancel()
            raise
    return index
