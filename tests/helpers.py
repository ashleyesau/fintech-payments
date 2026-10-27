"""Helpers shared by the runner, streaming and CDC tests."""
from datetime import date, timedelta

import psycopg
import pytest

from simulator.db import STREAMS
from simulator.runner import RunSettings

START = date(2025, 1, 1)
LONG_RUN = {"days": 260, "count": 25}  # long enough to originate loans and collect instalments


def settings_for(tmp_path, days=60, count=60, **kwargs) -> RunSettings:
    return RunSettings(
        start_date=START,
        end_date=START + timedelta(days=days),
        merchant_count=count,
        checkpoint_path=str(tmp_path / "checkpoint.json"),
        **kwargs,
    )


def table_contents(conn) -> dict[str, list]:
    """Every row of every table, ordered by primary key, for comparing two runs."""
    out = {}
    for name, stream in STREAMS.items():
        order = ", ".join(stream.key)
        out[name] = [tuple(r) for r in conn.execute(f"SELECT * FROM {stream.table} ORDER BY {order}")]
    return out


def reset(conn) -> None:
    for stream in STREAMS.values():
        conn.execute(f"TRUNCATE {stream.table} CASCADE")
    conn.commit()


# ---------------------------------------------------------------------------
# CDC tests
# ---------------------------------------------------------------------------

def require_cdc_postgres(connect, ci: bool):
    """Return an admin connection, or skip or fail the CDC tests (decision D9).

    Skip when Postgres is unreachable, as the other DB tests do. Fail when it is unreachable
    under CI, and fail when it is reachable but wal_level is not logical: skipping there
    would keep CI green without running the tests.
    """
    try:
        admin = connect()
    except psycopg.OperationalError as exc:
        if ci:
            pytest.fail(f"CI is set and Postgres is not reachable, so the CDC tests cannot run: {exc}")
        pytest.skip(f"Postgres not reachable: {exc}")
    level = admin.execute("SHOW wal_level").fetchone()[0]
    if level != "logical":
        admin.close()
        pytest.fail(
            f"wal_level is {level!r}, not 'logical'. Start Postgres with the flags in "
            "docker-compose.yml (wal_level=logical, max_replication_slots, max_wal_senders)."
        )
    return admin


def peek_changes(conn, slot: str, publication: str = "fintech_cdc", limit: int | None = None):
    """Binary pgoutput messages waiting in a slot, without confirming them.

    Returns (lsn text, xid, message bytes) in slot order. TimeZone is UTC so timestamp text
    is deterministic. This is a test aid for looking at raw messages; the consumer's own
    reader and decoder are separate code.
    """
    conn.execute("SET TimeZone = 'UTC'")
    rows = conn.execute(
        "SELECT lsn::text, xid::text::bigint, data "
        "FROM pg_logical_slot_peek_binary_changes(%s, NULL, %s, 'proto_version', '1', 'publication_names', %s)",
        (slot, limit, publication),
    ).fetchall()
    return [(lsn, xid, bytes(data)) for lsn, xid, data in rows]


def tuple_end(buf: bytes, pos: int) -> int:
    """Index just past the pgoutput TupleData that starts at `pos`."""
    columns = int.from_bytes(buf[pos:pos + 2], "big")
    pos += 2
    for _ in range(columns):
        kind = buf[pos:pos + 1]
        pos += 1
        if kind in (b"t", b"b"):
            pos += 4 + int.from_bytes(buf[pos:pos + 4], "big")
        elif kind not in (b"n", b"u"):
            raise ValueError(f"unknown tuple column kind {kind!r}")
    return pos


def split_update(message: bytes):
    """(old key kind, old tuple bytes, new tuple bytes) of a pgoutput Update message.

    The kind is b"O" (full old row), b"K" (key columns only) or None (no old row, which is
    what default replica identity gives when the key does not change).
    """
    assert message[:1] == b"U", f"not an update message: {message[:1]!r}"
    pos = 5  # type byte and the 4-byte relation id
    kind, old = None, None
    if message[pos:pos + 1] in (b"K", b"O"):
        kind = message[pos:pos + 1]
        end = tuple_end(message, pos + 1)
        old = message[pos + 1:end]
        pos = end
    assert message[pos:pos + 1] == b"N", "an update message ends with the new tuple"
    return kind, old, message[pos + 1:tuple_end(message, pos + 1)]
