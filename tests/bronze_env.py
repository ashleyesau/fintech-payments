"""tests/bronze_env.py: what the Bronze run tests share: a local warehouse bound to run(), synthetic landings, the
oracle that reads what was landed without using bronze, and a source that counts and interferes.

Nothing here is a test and nothing here touches a bucket."""
import hashlib
import threading

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table

from bronze.landing import LandingReader
from bronze.mapping import row_to_event
from bronze.run import NAMESPACE, TABLE_ORDER, run
from cdc.columns import EXPECTED_COLUMNS
from cdc.dedup import dedup, events_in
from cdc.landing import CLOCK_FIELDS
from tests.bronze_synth import PREFIX, SOURCE_DB, Landing
from tests.fakes import MemorySink

TABLES = tuple(EXPECTED_COLUMNS)
DATES = ["2025-02-27", "2025-03-01", "2025-04-02"]


class Env:
    """A local warehouse: a SQLite catalog, a directory, and a run() bound to them."""

    def __init__(self, path, catalog_name: str = "catalog.db", store=None) -> None:
        path.mkdir(parents=True, exist_ok=True)
        self.dir = path
        self.uri = f"file://{path / 'warehouse'}"
        self.catalog_file = path / catalog_name
        self.catalog = SqlCatalog("bronze", uri=f"sqlite:///{self.catalog_file}", warehouse=self.uri)
        self.store = MemorySink() if store is None else store   # the Bronze bucket's own objects: markers, catalog copy

    def lose_catalog(self) -> "Env":
        """The same warehouse and the same bucket objects with a brand new, empty catalog file: the old one is lost."""
        return Env(self.dir, catalog_name=f"lost-{len(list(self.dir.glob('lost-*')))}.db", store=self.store)

    def run(self, reader, **kwargs):
        kwargs.setdefault("write", True)
        kwargs.setdefault("warehouse", self.uri)
        kwargs.setdefault("source_db", SOURCE_DB)
        kwargs.setdefault("catalog_file", self.catalog_file)
        return run(reader, self.catalog, kwargs.pop("store", self.store), **kwargs)

    def table(self, name: str) -> Table:
        return self.catalog.load_table(f"{NAMESPACE}.{name}")

    def has(self, name: str) -> bool:
        return self.catalog.table_exists(f"{NAMESPACE}.{name}")

    def snapshots(self) -> dict:
        """table -> every snapshot id it has, or None where the table does not exist."""
        return {name: ([s.snapshot_id for s in self.table(name).snapshots()] if self.has(name) else None)
                for name in TABLE_ORDER}

    def rows(self, name: str) -> list[dict]:
        return self.table(name).scan().to_arrow().to_pylist()

    def keys(self, name: str) -> list:
        return sorted((r["commit_lsn"], r["transaction_position"]) for r in self.rows(name))

    def files(self) -> dict:
        warehouse = self.dir / "warehouse"
        if not warehouse.exists():
            return {}
        return {str(p): (p.stat().st_size, p.stat().st_mtime_ns) for p in sorted(warehouse.rglob("*")) if p.is_file()}

    def catalog_digest(self) -> str:
        return hashlib.sha256((self.dir / "catalog.db").read_bytes()).hexdigest()


def busy_landing() -> Landing:
    """Six transactions over three days and two months (each flushed alone, so every table has several objects),
    then a loan application that is disbursed (a c and a u on loan_applications) with a repayment."""
    land = Landing()
    for date in DATES * 2:
        land.transaction(("merchants", "c"), ("merchant_transactions", "c"), ("merchant_transactions", "c"),
                         ("risk_events", "c"), date=date)
        land.flush()
    land.transaction(("loan_applications", "c"), date="2025-03-01")
    land.flush()
    land.transaction(("loan_applications", "u"), ("loan_repayments", "c"), date="2025-03-01")
    land.flush()
    return land


def landed(land: Landing) -> dict:
    """table -> the unique landed events (first copy, in listing order): an oracle that does not use bronze."""
    by_table = {table: [] for table in TABLES}
    for key in land.sink.list_keys(land.prefix):
        for e in events_in(land.sink.objects[key]):
            by_table[e["source"]["table"]].append(e)
    return {table: dedup(events) for table, events in by_table.items()}


def without_clock(e: dict) -> dict:
    return {k: v for k, v in e.items() if k not in CLOCK_FIELDS}


def sort_events(events: list[dict]) -> list[dict]:
    return sorted(events, key=lambda e: (e["source"]["commit_lsn"], e["transaction"]["position"]))


def bronze_events(env: Env, name: str) -> list[dict]:
    return sort_events([row_to_event(name, row, source_db=SOURCE_DB) for row in env.rows(name)])


class Spy:
    """A source over a Landing's sink that counts and can interfere: on_read(key, n) is called before the n-th
    read (from 1) of every key, may raise, and may return replacement bytes."""

    def __init__(self, sink, on_read=None) -> None:
        self.sink, self.on_read = sink, on_read
        self.listings, self.reads, self._seen = 0, [], {}
        self._lock = threading.Lock()

    def list_keys(self, prefix):
        self.listings += 1
        return self.sink.list_keys(prefix)

    def read(self, key):
        with self._lock:
            self.reads.append(key)
            n = self._seen[key] = self._seen.get(key, 0) + 1
        data = self.sink.read(key)
        if self.on_read is not None:
            replaced = self.on_read(key, n)
            if replaced is not None:
                return replaced
        return data

    def reader(self, prefix=PREFIX) -> LandingReader:
        return LandingReader(self, prefix)


def spread_landing(objects=7) -> Landing:
    """One table, `objects` objects of two rows each, one transaction apiece."""
    land = Landing()
    for i in range(objects):
        land.transaction(("merchant_transactions", "c"), ("merchant_transactions", "c"), date=DATES[i % 3])
        land.flush()
    return land
