"""
validation.py: run the named queries in validate.sql against a populated database.

    python -m simulator.validation                       # every check, against POSTGRES_DB
    POSTGRES_DB=val_s42 python -m simulator.validation --only par --only collections

The SQL lives in validate.sql so it can also be run through psql. This module splits it
into named blocks, runs the setup block (temp tables), then each check, and compares the
value in each check row to its band. A NULL bound is open ended; a row with no bounds is
reported as info.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import click
import psycopg

from simulator import db

SQL_PATH = Path(__file__).resolve().parent.parent / "validate.sql"
_NAME = re.compile(r"^-- name: (\w+)\s*$", re.MULTILINE)


@dataclass(frozen=True)
class Check:
    query: str
    name: str
    value: Optional[float]
    low: Optional[float]
    high: Optional[float]
    n: Optional[int]
    note: str

    @property
    def status(self) -> str:
        if self.value is None:
            return "no data"
        if self.low is None and self.high is None:
            return "info"
        if self.low is not None and self.value < self.low:
            return "LOW"
        if self.high is not None and self.value > self.high:
            return "HIGH"
        return "ok"


def load_queries(path: Path = SQL_PATH) -> dict[str, str]:
    """Named SQL blocks from validate.sql, in file order."""
    text = path.read_text()
    marks = list(_NAME.finditer(text))
    queries = {}
    for i, mark in enumerate(marks):
        end = marks[i + 1].start() if i + 1 < len(marks) else len(text)
        queries[mark.group(1)] = text[mark.end():end].strip()
    return queries


def _number(value: Any) -> Optional[float]:
    return None if value is None else float(value) if isinstance(value, (Decimal, int, float)) else None


def run_setup(conn: psycopg.Connection) -> None:
    conn.execute(load_queries()["setup"])
    conn.commit()  # a later rollback must not drop the temp tables


def run_check(conn: psycopg.Connection, name: str) -> list[Check]:
    """Rows of one check query as Check objects. Series queries are not checks."""
    cur = conn.execute(load_queries()[name])
    columns = [c.name for c in cur.description]
    if "check_name" not in columns:
        raise ValueError(f"{name} is a series query, use run_series")
    out = []
    for row in cur.fetchall():
        r = dict(zip(columns, row))
        out.append(Check(name, r["check_name"], _number(r["value"]), _number(r["low"]),
                         _number(r["high"]), r["n"], r.get("note") or ""))
    return out


def run_series(conn: psycopg.Connection, name: str) -> tuple[list[str], list[tuple]]:
    cur = conn.execute(load_queries()[name])
    return [c.name for c in cur.description], cur.fetchall()


def run_all(conn: psycopg.Connection, only: tuple[str, ...] = ()) -> tuple[list[Check], dict[str, tuple]]:
    run_setup(conn)
    checks: list[Check] = []
    series: dict[str, tuple] = {}
    for name in load_queries():
        if name == "setup" or (only and not any(o in name for o in only)):
            continue
        if name.startswith("series_"):
            series[name] = run_series(conn, name)
        else:
            checks.extend(run_check(conn, name))
    return checks, series


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, (Decimal, float)):
        return f"{float(value):.4g}" if abs(float(value)) < 100 else f"{float(value):,.0f}"
    return str(value)


def format_checks(checks: list[Check]) -> str:
    rows = [("check", "value", "band", "n", "status")]
    for c in checks:
        band = "-" if c.low is None and c.high is None else f"{_fmt(c.low)} to {_fmt(c.high)}"
        rows.append((c.name, _fmt(c.value), band, _fmt(c.n), c.status))
    widths = [max(len(r[i]) for r in rows) for i in range(5)]
    return "\n".join("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(r)).rstrip() for r in rows)


def format_series(name: str, columns: list[str], rows: list[tuple]) -> str:
    table = [tuple(columns)] + [tuple(_fmt(v) for v in r) for r in rows]
    widths = [max(len(str(r[i])) for r in table) for i in range(len(columns))]
    body = "\n".join("  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(r)).rstrip() for r in table)
    return f"{name}\n{body}"


@click.command()
@click.option("--only", multiple=True, help="Run only queries whose name contains this text.")
@click.option("--dsn", default=None, help="Connection string. Defaults to the POSTGRES_* settings.")
def main(only: tuple[str, ...], dsn: Optional[str]) -> None:
    """Run validate.sql and print each check against its Validation Targets band."""
    conn = db.connect(dsn) if dsn else db.connect()
    try:
        checks, series = run_all(conn, only)
    finally:
        conn.close()
    for name, (columns, rows) in series.items():
        click.echo(format_series(name, columns, rows) + "\n")
    click.echo(format_checks(checks))


if __name__ == "__main__":
    main()
