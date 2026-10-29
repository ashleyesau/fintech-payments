"""tests/bronze_run_probe.py: a whole write run over a generated landing, in its own process, to measure memory.

    python -m tests.bronze_run_probe TRANSACTIONS CEILING_MB [BATCH_ROWS]

Runs bronze.run.run (write=True, the default 8 workers) over LazyLanding(TRANSACTIONS) into a temporary local
warehouse (a SQLite catalog and a directory that are removed afterwards) and prints one JSON line: the objects and
events read, per table the rows appended and the batches committed, the rows read back from each table after the
run, the memory the process held before and at its peak, and the seconds taken. A watchdog thread looks at the
process's peak every 50 ms and, as soon as it is more than CEILING_MB above where it started, writes
"ceiling exceeded" to stderr and exits 7, so a run that holds rows fails in seconds. The memory is the whole
process's peak from the operating system (bronze.check.peak_memory_bytes), threads and Parquet buffers included.
The peak is taken before the rows are counted, so the count does not enter the measurement.
"""
import json
import os
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

from pyiceberg.catalog.sql import SqlCatalog

from bronze.check import peak_memory_bytes
from bronze.landing import LandingReader
from bronze.run import NAMESPACE, TABLE_ORDER, run
from tests.bronze_synth import PREFIX, SOURCE_DB, LazyLanding
from tests.fakes import MemorySink


def main(transactions: int, ceiling_mb: float, batch_rows: int) -> None:
    lazy = LazyLanding(transactions)
    reader = LandingReader(lazy, PREFIX)
    directory = tempfile.mkdtemp(prefix="bronze_run_probe_")
    try:
        warehouse = f"file://{directory}/warehouse"
        catalog = SqlCatalog("bronze", uri=f"sqlite:///{directory}/catalog.db", warehouse=warehouse)
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
        report = run(reader, catalog, MemorySink(), catalog_file=Path(directory) / "catalog.db", warehouse=warehouse,
                     source_db=SOURCE_DB, write=True, batch_rows=batch_rows)
        seconds = round(time.monotonic() - started, 1)
        peak = peak_memory_bytes()
        done.set()
        read_back = {name: catalog.load_table(f"{NAMESPACE}.{name}").scan(selected_fields=("commit_lsn",))
                     .to_arrow().num_rows for name in TABLE_ORDER}
        print(json.dumps({
            "objects": report.summary.objects, "events": report.summary.events_read,
            "appended": {n: t.rows_appended for n, t in report.tables.items()},
            "batches": {n: t.batches for n, t in report.tables.items()},
            "read_back": read_back, "base": base, "peak": peak, "seconds": seconds}))
    finally:
        shutil.rmtree(directory, ignore_errors=True)


if __name__ == "__main__":
    main(int(sys.argv[1]), float(sys.argv[2]), int(sys.argv[3]) if len(sys.argv) > 3 else 100_000)
