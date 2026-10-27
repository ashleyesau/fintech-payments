"""
measure_decode_speed.py: how fast the consumer's CPU work runs in Python.

Peeks up to --changes changes from a slot (without confirming anything), then runs each stage
--repeat times and reports the best time: the decode alone, then the whole pipeline (decode,
transaction assembly with the drift and operation checks, envelope and NDJSON line), then
gzip of the lines. The SQL fetch is timed separately, because it is not the consumer's CPU
work. Nothing is written or consumed.

    PYTHONPATH=. python scripts/measure_decode_speed.py --slot cdc_r01 --changes 200000

The database comes from the CDC_ settings (CDC_DB and so on), as the consumer would use it.
The reader needs the slot to hold at least that many changes; it reports how many it got.
"""
from __future__ import annotations

import gzip
import time
from collections import Counter

import click
import psycopg

from cdc.config import settings
from cdc.envelope import build_events, to_line
from cdc.lsn import parse_lsn
from cdc.pgoutput import Decoder, require_supported
from cdc.transactions import TransactionAssembler


@click.command()
@click.option("--slot", default=settings.slot_name, show_default=True)
@click.option("--changes", default=200_000, show_default=True, help="Stop reading after about this many changes.")
@click.option("--repeat", default=3, show_default=True, help="Decode this many times and keep the best.")
def main(slot: str, changes: int, repeat: int) -> None:
    with psycopg.connect(settings.dsn, autocommit=True) as conn:
        conn.execute("SET TimeZone = 'UTC'")
        started = time.perf_counter()
        rows = conn.execute(
            "SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes(%s, NULL, %s, 'proto_version', '1', 'publication_names', %s)",
            (slot, changes, settings.publication)).fetchall()
        fetch_seconds = time.perf_counter() - started
    lsns = [parse_lsn(r[0]) for r in rows]
    messages = [bytes(r[1]) for r in rows]
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

    pipeline, lines, keys = None, [], []
    for _ in range(repeat):
        assembler = TransactionAssembler()
        lines, keys = [], []
        started = time.perf_counter()
        for lsn, message in zip(lsns, messages):
            transaction = assembler.feed(lsn, message)
            if transaction is not None:
                for event in build_events(transaction, settings.db):
                    lines.append(to_line(event))
                    keys.append((event.source.table, event.event_date))
        elapsed = time.perf_counter() - started
        pipeline = elapsed if pipeline is None else min(pipeline, elapsed)
    click.echo(f"  decode + assemble + envelope + line (best of {repeat}): {pipeline:.2f} s, {len(lines) / pipeline:,.0f} events/s")

    body = b"".join(lines)
    started = time.perf_counter()
    packed = gzip.compress(body)
    gzip_seconds = time.perf_counter() - started
    click.echo(f"  lines: {len(lines):,} events, {len(body) / 1e6:.1f} MB ({len(body) / max(len(lines), 1):,.0f} bytes an event)")
    click.echo(f"  gzip, everything in one object: {len(packed) / 1e6:.1f} MB, {len(body) / len(packed):.1f}x, "
               f"{gzip_seconds:.2f} s ({len(lines) / gzip_seconds:,.0f} events/s)")

    # What the sink will really write: one object per (table, event date), each compressed alone.
    groups: dict[tuple, list[bytes]] = {}
    for key, line in zip(keys, lines):
        groups.setdefault(key, []).append(line)
    started = time.perf_counter()
    sizes = [len(gzip.compress(b"".join(group))) for group in groups.values()]
    objects_seconds = time.perf_counter() - started
    click.echo(f"  gzip, one object per (table, event date): {len(sizes):,} objects, {sum(sizes) / 1e6:.1f} MB, "
               f"{len(body) / sum(sizes):.1f}x, mean {sum(sizes) / len(sizes) / 1e3:,.0f} KB, "
               f"largest {max(sizes) / 1e3:,.0f} KB, smallest {min(sizes) / 1e3:,.1f} KB, {objects_seconds:.2f} s")
    click.echo("  (a transaction cut off at the --changes limit is finished, so the counts can exceed it slightly)")


if __name__ == "__main__":
    main()
