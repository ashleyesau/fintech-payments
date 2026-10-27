"""
measure_decode_speed.py: how fast the pgoutput decoder runs in Python.

Peeks up to --changes changes from a slot (without confirming anything), then decodes the
messages --repeat times and reports the best time. The fetch and the decode are timed
separately, because only the decode is the decoder's cost. Nothing is written or consumed.

    PYTHONPATH=. python scripts/measure_decode_speed.py --slot cdc_r01 --changes 200000

The database comes from the CDC_ settings (CDC_DB and so on), as the consumer would use it.
The reader needs the slot to hold at least that many changes; it reports how many it got.
"""
from __future__ import annotations

import time
from collections import Counter

import click
import psycopg

from cdc.config import settings
from cdc.pgoutput import Decoder, Insert, Update, require_supported


@click.command()
@click.option("--slot", default=settings.slot_name, show_default=True)
@click.option("--changes", default=200_000, show_default=True, help="Stop reading after about this many changes.")
@click.option("--repeat", default=3, show_default=True, help="Decode this many times and keep the best.")
def main(slot: str, changes: int, repeat: int) -> None:
    with psycopg.connect(settings.dsn, autocommit=True) as conn:
        conn.execute("SET TimeZone = 'UTC'")
        started = time.perf_counter()
        rows = conn.execute(
            "SELECT data FROM pg_logical_slot_peek_binary_changes(%s, NULL, %s, 'proto_version', '1', 'publication_names', %s)",
            (slot, changes, settings.publication)).fetchall()
        fetch_seconds = time.perf_counter() - started
    messages = [bytes(r[0]) for r in rows]
    megabytes = sum(map(len, messages)) / 1e6

    best = None
    for _ in range(repeat):
        decoder = Decoder()
        started = time.perf_counter()
        kinds = Counter()
        for message in messages:
            decoded = decoder.decode(message)
            require_supported(decoded)
            kinds[type(decoded).__name__] += 1
        elapsed = time.perf_counter() - started
        best = elapsed if best is None else min(best, elapsed)

    changes_read = kinds["Insert"] + kinds["Update"]
    click.echo(f"slot {slot} in {settings.db}: {len(messages):,} messages, {changes_read:,} row changes, {megabytes:.1f} MB")
    click.echo(f"  kinds: {dict(kinds)}")
    click.echo(f"  fetch (SQL peek, including the transfer): {fetch_seconds:.2f} s, {changes_read / fetch_seconds:,.0f} row changes/s")
    click.echo(f"  decode (best of {repeat}): {best:.2f} s, {changes_read / best:,.0f} row changes/s, {len(messages) / best:,.0f} messages/s, {megabytes / best:.1f} MB/s")


if __name__ == "__main__":
    main()
