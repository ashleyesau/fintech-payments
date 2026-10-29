"""tests/bronze_memory_probe.py: pass 1 over a generated landing, run in its own process to measure memory.

    python -m tests.bronze_memory_probe TRANSACTIONS CEILING_MB

Builds the index over LazyLanding(TRANSACTIONS) with the default 8 workers and prints one JSON line:
objects, events, the memory the process held before and at its peak, and the seconds taken. A watchdog
thread looks at the process's peak every 50 ms and, as soon as it is more than CEILING_MB above where it
started, writes "ceiling exceeded" to stderr and exits 7, so an index that holds the events fails in
seconds instead of using all the memory first. Peak memory comes from the operating system
(bronze.check.peak_memory_bytes), so the measurement is of the whole process, threads and buffers included.
"""
import json
import os
import sys
import threading
import time

from bronze.check import peak_memory_bytes
from bronze.index import build_index
from bronze.landing import LandingReader
from tests.bronze_synth import PREFIX, SOURCE_DB, LazyLanding


def main(transactions: int, ceiling_mb: float) -> None:
    lazy = LazyLanding(transactions)
    reader = LandingReader(lazy, PREFIX)
    base = peak_memory_bytes()
    done = threading.Event()

    def watch() -> None:
        while not done.wait(0.05):
            if peak_memory_bytes() - base > ceiling_mb * 1_000_000:
                print(f"ceiling exceeded: {(peak_memory_bytes() - base) / 1e6:.0f} MB above the start, "
                      f"ceiling {ceiling_mb:g} MB", file=sys.stderr, flush=True)
                os._exit(7)

    threading.Thread(target=watch, daemon=True).start()
    started = time.monotonic()
    index = build_index(reader, source_db=SOURCE_DB)
    done.set()
    summary = index.summary()
    print(json.dumps({"objects": summary.objects, "events": summary.events_read, "unique": summary.unique_events,
                      "incomplete": summary.incomplete, "base": base, "peak": peak_memory_bytes(),
                      "seconds": round(time.monotonic() - started, 1)}))


if __name__ == "__main__":
    main(int(sys.argv[1]), float(sys.argv[2]))
