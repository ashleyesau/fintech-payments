"""Helpers shared by the runner and streaming tests."""
from datetime import date, timedelta

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
