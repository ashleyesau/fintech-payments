"""
transactions.py: from a stream of messages to whole transactions.

The reader hands over messages in slot order. A transaction is complete only at its Commit
message, and the consumer only ever confirms whole transactions, so this is where the
stream is cut into the units everything after it works on.

Every message passes the contract's stop conditions on the way: a delete, a truncate, an
update without the full old row (require_supported) and a Relation message whose columns
differ from the model (check_relation). After any error discard the assembler: its state is
undefined, and the consumer stops anyway.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union

from cdc.columns import check_relation
from cdc.errors import DecodeError
from cdc.pgoutput import Begin, Commit, Decoder, Insert, Relation, Update, require_supported

Change = tuple[int, Union[Insert, Update]]  # (change_lsn, message)


@dataclass(frozen=True, slots=True)
class Transaction:
    xid: int
    commit_lsn: int      # the dedup key's LSN, from the Begin and Commit messages
    end_lsn: int         # what to confirm once everything in it has landed
    commit_ts: datetime  # aware, UTC
    changes: tuple[Change, ...]  # in commit order; empty if no published change was in it


class TransactionAssembler:
    def __init__(self, decoder: Optional[Decoder] = None) -> None:
        self._decoder = decoder or Decoder()
        self._begin: Optional[Begin] = None
        self._changes: list[Change] = []

    @property
    def in_transaction(self) -> bool:
        """True between a Begin and its Commit. A batch must end with this False."""
        return self._begin is not None

    def feed(self, lsn: int, data: bytes) -> Optional[Transaction]:
        """Decode one message. `lsn` is the slot's own lsn for it, which for an insert or update
        is the change's LSN. Returns the Transaction when this message is its Commit."""
        message = self._decoder.decode(data)
        require_supported(message)
        if isinstance(message, Relation):
            check_relation(message)
            return None
        if isinstance(message, Begin):
            if self._begin is not None:
                raise DecodeError("Begin inside an open transaction")
            self._begin = message
            return None
        if isinstance(message, Commit):
            begin = self._begin
            if begin is None:
                raise DecodeError("Commit without a Begin")
            if message.commit_lsn != begin.final_lsn:
                raise DecodeError(f"Commit LSN {message.commit_lsn} differs from the Begin's {begin.final_lsn}")
            transaction = Transaction(begin.xid, message.commit_lsn, message.end_lsn, message.commit_ts,
                                      tuple(self._changes))
            self._begin, self._changes = None, []
            return transaction
        if self._begin is None:
            raise DecodeError(f"{type(message).__name__} outside a transaction")
        self._changes.append((lsn, message))
        return None
