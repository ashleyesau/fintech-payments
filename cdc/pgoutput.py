"""
pgoutput.py: decoder for the pgoutput logical replication protocol, version 1, text format.

The decoder takes one binary message at a time and returns a typed message. It is stateful in
one way only: it remembers the latest Relation message for each table OID, because Insert,
Update and Delete messages refer to a table by OID. A Relation message can arrive at any point
in a decode call (not only once per table), so every one replaces what was known before.

It does not depend on how messages are fetched, so a streaming reader can replace the SQL
polling reader without touching it. Anything it cannot decode exactly raises DecodeError (or
UnknownTypeError), which stops the consumer without confirming the batch.

Message layouts follow the PostgreSQL logical replication message formats. Origin, Type and
Message ('O', 'Y', 'M') are not requested by the reader and are refused if they appear.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional, Union

from cdc.errors import DecodeError, UnsupportedChange
from cdc.types import converter_for

_POSTGRES_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)

# The only table that may be updated (contract section 8) and the only old-row kind accepted.
UPDATABLE_TABLE = "loan_applications"
FULL_OLD_ROW = "O"


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    type_oid: int
    type_modifier: int
    is_key: bool


@dataclass(frozen=True, slots=True)
class Relation:
    oid: int
    schema: str
    name: str
    replica_identity: str  # 'd' default, 'n' nothing, 'f' full, 'i' index
    columns: tuple[Column, ...]
    converters: tuple[tuple[str, Callable[[str], object]], ...]


@dataclass(frozen=True, slots=True)
class Begin:
    final_lsn: int          # the commit LSN of the transaction
    commit_ts: datetime     # aware, UTC
    xid: int


@dataclass(frozen=True, slots=True)
class Commit:
    flags: int
    commit_lsn: int
    end_lsn: int            # confirm this, not commit_lsn, to skip the transaction on the next read
    commit_ts: datetime


@dataclass(frozen=True, slots=True)
class Insert:
    relation: Relation
    new: dict


@dataclass(frozen=True, slots=True)
class Update:
    relation: Relation
    old_kind: Optional[str]     # 'O' full old row, 'K' key columns only (others NULL), None no old row
    old: Optional[dict]
    new: dict


@dataclass(frozen=True, slots=True)
class Delete:
    relation: Relation
    old_kind: str               # 'K' or 'O'
    old: dict


@dataclass(frozen=True, slots=True)
class Truncate:
    relations: tuple[Relation, ...]
    cascade: bool
    restart_identity: bool


Message = Union[Begin, Commit, Relation, Insert, Update, Delete, Truncate]


def _cstring(buf: bytes, pos: int) -> tuple[str, int]:
    end = buf.index(b"\x00", pos)
    return buf[pos:end].decode(), end + 1


class Decoder:
    """Decodes pgoutput messages one at a time. Keep one instance per reader session."""

    def __init__(self) -> None:
        self._relations: dict[int, Relation] = {}

    def decode(self, message: bytes) -> Message:
        if not message:
            raise DecodeError("empty message")
        kind = message[:1]
        try:
            handler = self._HANDLERS.get(kind)
            if handler is None:
                raise DecodeError(f"unknown message type {kind!r}")
            result, end = handler(self, message)
        except (struct.error, IndexError, ValueError, UnicodeDecodeError) as exc:
            # ValueError covers bytes.index failing on a missing terminator.
            raise DecodeError(f"malformed {kind!r} message: {exc}") from exc
        if end != len(message):
            raise DecodeError(f"{len(message) - end} unexpected trailing bytes in {kind!r} message")
        return result

    # -- transaction framing ------------------------------------------------

    def _begin(self, buf: bytes) -> tuple[Begin, int]:
        final_lsn, micros, xid = struct.unpack_from(">QqI", buf, 1)
        return Begin(final_lsn, _POSTGRES_EPOCH + timedelta(microseconds=micros), xid), 21

    def _commit(self, buf: bytes) -> tuple[Commit, int]:
        flags, commit_lsn, end_lsn, micros = struct.unpack_from(">BQQq", buf, 1)
        return Commit(flags, commit_lsn, end_lsn, _POSTGRES_EPOCH + timedelta(microseconds=micros)), 26

    # -- relation -----------------------------------------------------------

    def _relation(self, buf: bytes) -> tuple[Relation, int]:
        (oid,) = struct.unpack_from(">I", buf, 1)
        schema, pos = _cstring(buf, 5)
        name, pos = _cstring(buf, pos)
        identity = chr(buf[pos])
        (count,) = struct.unpack_from(">H", buf, pos + 1)
        pos += 3
        columns = []
        for _ in range(count):
            flags = buf[pos]
            column, pos = _cstring(buf, pos + 1)
            type_oid, modifier = struct.unpack_from(">Ii", buf, pos)
            pos += 8
            columns.append(Column(column, type_oid, modifier, bool(flags & 1)))
        # Resolving the converters here stops the consumer on an unknown type at the Relation
        # message, before any row of that table is read.
        converters = tuple((c.name, converter_for(c.type_oid, f"{name}.{c.name}")) for c in columns)
        relation = Relation(oid, schema, name, identity, tuple(columns), converters)
        self._relations[oid] = relation
        return relation, pos

    def _known(self, oid: int) -> Relation:
        try:
            return self._relations[oid]
        except KeyError:
            raise DecodeError(f"change for relation {oid} with no Relation message before it") from None

    # -- row data -----------------------------------------------------------

    @staticmethod
    def _tuple(buf: bytes, pos: int, relation: Relation) -> tuple[dict, int]:
        (count,) = struct.unpack_from(">H", buf, pos)
        pos += 2
        if count != len(relation.converters):
            raise DecodeError(f"{relation.name}: row has {count} columns, the Relation message listed {len(relation.converters)}")
        row = {}
        for name, convert in relation.converters:
            kind = buf[pos]
            pos += 1
            if kind == 0x74:  # 't' text
                (length,) = struct.unpack_from(">I", buf, pos)
                pos += 4
                raw = buf[pos:pos + length]
                if len(raw) != length:
                    raise DecodeError(f"{relation.name}.{name}: value runs past the end of the message")
                pos += length
                try:
                    row[name] = convert(raw.decode())
                except (ValueError, OverflowError) as exc:
                    raise DecodeError(f"{relation.name}.{name}: {exc}") from exc
            elif kind == 0x6E:  # 'n' null
                row[name] = None
            elif kind == 0x75:  # 'u' unchanged TOAST value
                raise DecodeError(f"{relation.name}.{name}: unchanged TOAST value, which the contract does not allow")
            elif kind == 0x62:  # 'b' binary
                raise DecodeError(f"{relation.name}.{name}: binary value, but the reader asks for text")
            else:
                raise DecodeError(f"{relation.name}.{name}: unknown tuple column kind {kind:#x}")
        return row, pos

    def _insert(self, buf: bytes) -> tuple[Insert, int]:
        relation = self._known(struct.unpack_from(">I", buf, 1)[0])
        if buf[5:6] != b"N":
            raise DecodeError(f"insert on {relation.name} without a new tuple")
        new, pos = self._tuple(buf, 6, relation)
        return Insert(relation, new), pos

    def _update(self, buf: bytes) -> tuple[Update, int]:
        relation = self._known(struct.unpack_from(">I", buf, 1)[0])
        pos = 5
        old_kind, old = None, None
        if buf[pos:pos + 1] in (b"K", b"O"):
            old_kind = chr(buf[pos])
            old, pos = self._tuple(buf, pos + 1, relation)
        if buf[pos:pos + 1] != b"N":
            raise DecodeError(f"update on {relation.name} without a new tuple")
        new, pos = self._tuple(buf, pos + 1, relation)
        return Update(relation, old_kind, old, new), pos

    def _delete(self, buf: bytes) -> tuple[Delete, int]:
        relation = self._known(struct.unpack_from(">I", buf, 1)[0])
        if buf[5:6] not in (b"K", b"O"):
            raise DecodeError(f"delete on {relation.name} without an old tuple")
        old, pos = self._tuple(buf, 6, relation)
        return Delete(relation, chr(buf[5]), old), pos

    def _truncate(self, buf: bytes) -> tuple[Truncate, int]:
        count, options = struct.unpack_from(">IB", buf, 1)
        oids = struct.unpack_from(f">{count}I", buf, 6)
        relations = tuple(self._known(oid) for oid in oids)
        return Truncate(relations, bool(options & 1), bool(options & 2)), 6 + 4 * count

    _HANDLERS = {
        b"B": _begin,
        b"C": _commit,
        b"R": _relation,
        b"I": _insert,
        b"U": _update,
        b"D": _delete,
        b"T": _truncate,
    }


def require_supported(message: Message) -> None:
    """Raise UnsupportedChange for a change the contract does not allow (contract section 8).

    Inserts are always allowed. An update is allowed only on loan_applications and only with
    the full old row (replica identity FULL, migration 003). Deletes and truncates are not
    allowed. Begin, Commit and Relation messages carry no change and always pass.
    """
    if isinstance(message, Delete):
        raise UnsupportedChange(f"delete on {message.relation.name}: V1 never deletes, stopping without confirming")
    if isinstance(message, Truncate):
        names = ", ".join(r.name for r in message.relations)
        raise UnsupportedChange(f"truncate of {names}: V1 never truncates, stopping without confirming")
    if isinstance(message, Update):
        if message.relation.name != UPDATABLE_TABLE:
            raise UnsupportedChange(f"update on {message.relation.name}: only {UPDATABLE_TABLE} is ever updated")
        if message.old_kind != FULL_OLD_ROW:
            raise UnsupportedChange(
                f"update on {message.relation.name} without the full old row (old kind {message.old_kind!r}); "
                "is REPLICA IDENTITY FULL set (migration 003)?"
            )
