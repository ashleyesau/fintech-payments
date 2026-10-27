"""
reader.py: the one place that talks to the replication slot (decision D1a, M4-Q1).

Three calls. `fetch(limit)` reads the next changes without consuming them. `confirm(end_lsn)`
moves the slot forward once those changes are safely in the bucket. `status()` says how much
WAL the slot is holding back. Everything after the reader works on (lsn, bytes) pairs, so a
streaming reader could replace this module without touching the decoder or the sink.

Reading is a peek, not a get: nothing is consumed until `confirm`, so a crash at any point
before it replays the same changes. Postgres decodes from the confirmed position on every
call, so every fetch is bounded.

Facts about Postgres 16 this module relies on, each checked against a real server (DECISION_LOG,
"M4 build notes"): `upto_nchanges` is a floor, the call finishes the transaction it is in, so a
fetch ends on a Commit; 0 or NULL means no limit at all; advancing the slot backwards raises
55000, advancing to the same position does nothing, and advancing past the end of the WAL
silently moves the slot to the end, skipping everything unread; a lost slot raises 55000 on a
peek and on an advance; a missing slot raises 42704.

A lost slot is recognised by asking the slot (`wal_status` is "lost"), never by the wording of the
error: the wording changed between Postgres 16 ("can no longer get changes from replication slot"
on a peek, "cannot be advanced" on an advance) and later releases ("can no longer access
replication slot ... invalidated due to wal_removed"), which GitHub's runner showed when the first
version of this module matched the Postgres 16 text and reported a lost slot as an unknown error. The connection stays autocommit, with TimeZone UTC so timestamp text is
deterministic. A dropped connection raises psycopg's OperationalError and stops the consumer:
the slot keeps its position, so the operator just restarts it.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import psycopg
from psycopg import errors as pg_errors

from cdc.errors import SlotLost, SlotMissing
from cdc.lsn import format_lsn, parse_lsn

_PEEK = ("SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes("
         "%s, NULL, %s, 'proto_version', '1', 'publication_names', %s)")
_ADVANCE = "SELECT end_lsn::text FROM pg_replication_slot_advance(%s, %s::pg_lsn)"
_WAL_STATUS = "SELECT wal_status FROM pg_replication_slots WHERE slot_name = %s"
_STATUS = ("SELECT wal_status, safe_wal_size, confirmed_flush_lsn::text, restart_lsn::text, "
           "pg_size_bytes(current_setting('max_slot_wal_keep_size')) "
           "FROM pg_replication_slots WHERE slot_name = %s")


@dataclass(frozen=True, slots=True)
class SlotStatus:
    wal_status: Optional[str]      # reserved, extended, unreserved or lost
    safe_wal_size: Optional[int]   # bytes of WAL that can still be written before the slot is lost;
                                   # negative when unreserved, None when lost or when there is no cap
    cap_bytes: Optional[int]       # max_slot_wal_keep_size, None if unlimited
    confirmed_flush_lsn: int       # where the slot is: everything before it is confirmed
    restart_lsn: Optional[int]

    @property
    def lost(self) -> bool:
        return self.wal_status == "lost"


class Reader:
    def __init__(self, dsn: str, slot_name: str, publication: str) -> None:
        self.slot_name = slot_name
        self.publication = publication
        self._conn = psycopg.connect(dsn, autocommit=True)
        self._conn.execute("SET TimeZone = 'UTC'")
        self._read_to = 0   # the slot's LSN on the last row fetched: the furthest we may confirm

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "Reader":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def fetch(self, limit: int) -> list[tuple[int, bytes]]:
        """The next messages in slot order as (the slot's lsn for the message, pgoutput bytes).
        At least `limit` changes unless the slot has fewer, and always whole transactions, so a
        non-empty result ends with a Commit message. [] when nothing is waiting."""
        if limit < 1:
            raise ValueError(f"limit must be at least 1, got {limit}: Postgres reads 0 as no limit at all")
        try:
            rows = self._conn.execute(_PEEK, (self.slot_name, limit, self.publication)).fetchall()
        except pg_errors.UndefinedObject:
            raise self._missing() from None
        except pg_errors.ObjectNotInPrerequisiteState:
            if self._is_lost():
                raise self.lost_error() from None
            raise
        messages = [(parse_lsn(lsn), bytes(data)) for lsn, data in rows]
        if messages:
            self._read_to = max(self._read_to, messages[-1][0])
        return messages

    def confirm(self, end_lsn: int) -> None:
        """Move the slot to `end_lsn`, the `end_lsn` of the last Commit message of what has landed
        (never its commit_lsn: that leaves the transaction in the slot). Only a position this
        reader has fetched may be confirmed, because Postgres would silently accept a position
        past the end of the WAL and skip everything unread. Going backwards raises Postgres's own
        error; staying put is a no-op."""
        if end_lsn > self._read_to:
            raise RuntimeError(f"cannot confirm {format_lsn(end_lsn)}: this reader has only read up to "
                               f"{format_lsn(self._read_to)}, and confirming unread changes would lose them")
        try:
            row = self._conn.execute(_ADVANCE, (self.slot_name, format_lsn(end_lsn))).fetchone()
        except pg_errors.UndefinedObject:
            raise self._missing() from None
        except pg_errors.ObjectNotInPrerequisiteState:
            if self._is_lost():
                raise self.lost_error() from None   # lost while the batch was uploading
            raise                                   # anything else of this kind (going backwards) is Postgres's own error
        reached = parse_lsn(row[0])
        if reached != end_lsn:
            raise RuntimeError(f"asked Postgres to confirm {format_lsn(end_lsn)} but the slot is at "
                               f"{format_lsn(reached)}")

    def status(self) -> SlotStatus:
        row = self._conn.execute(_STATUS, (self.slot_name,)).fetchone()
        if row is None:
            raise self._missing()
        wal_status, safe_wal_size, confirmed, restart, cap = row
        return SlotStatus(wal_status, safe_wal_size, None if cap < 0 else cap,
                          parse_lsn(confirmed), None if restart is None else parse_lsn(restart))

    def _is_lost(self) -> bool:
        """Whether Postgres has invalidated the slot, which is what makes it unreadable and unrepairable."""
        row = self._conn.execute(_WAL_STATUS, (self.slot_name,)).fetchone()
        return row is not None and row[0] == "lost"

    def lost_error(self) -> SlotLost:
        return SlotLost(
            f"Slot {self.slot_name} is lost: Postgres removed the WAL it was holding because the slot fell "
            "further behind than max_slot_wal_keep_size allows, and a lost slot cannot be repaired. The only "
            "recovery is to regenerate the run with the same seed into a fresh database with a new slot name "
            "(for example cdc_r02), which also gives a new bucket prefix.")

    def _missing(self) -> SlotMissing:
        return SlotMissing(
            f"Slot {self.slot_name} does not exist in database {self._conn.info.dbname}. Create it before any "
            "data is written (README, CDC setup), or the first run would miss rows.")
