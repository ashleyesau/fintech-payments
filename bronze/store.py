"""
store.py: the Bronze bucket's own objects, apart from the tables (GCS Plan (V2) section 19, G3 question 4).

PyIceberg writes the table files itself. This module is the door for the two other kinds of object the Bronze job
keeps in gs://fintech_bronze, and nothing else:

    _runs/<run_id>.json        the run marker, created once and never replaced (bronze/marker.py)
    _catalog/catalog.db        the copy of the local SQLite catalog, replaced after a run (bronze/catalog_copy.py)

Store is what the run needs: list, read, create-if-absent and overwrite. BronzeStore is the real one, over
cdc.gcs.GcsSink (the only module that imports Google's libraries). It is a guard as much as an adapter: a create
must be a marker name and an overwrite must be the catalog copy, so a bug elsewhere cannot write into
bronze/<table>/ and damage a table. It never touches the landing bucket: it is connected to the Bronze bucket only.
Like the landing reader, it refuses to connect unless the Bronze credentials are selected
(bronze.config.require_credentials).
"""
from __future__ import annotations

import re
from typing import Callable, Mapping, Optional, Protocol

from bronze.config import BronzeSettings, require_credentials
from bronze.errors import ConfigError
from cdc.gcs import GcsSink

RUNS_PREFIX = "_runs/"
CATALOG_KEY = "_catalog/catalog.db"
# The marker's name is its run_id, a UTC timestamp with microseconds, fixed width so that name order is time order.
MARKER_KEY = re.compile(r"_runs/(?P<run_id>[0-9]{8}T[0-9]{12}Z)\.json")
MARKER_TYPE = "application/json"
CATALOG_TYPE = "application/vnd.sqlite3"


class Store(Protocol):
    """What the run needs from the Bronze bucket: BronzeStore, or a fake with the same four calls."""
    def list_keys(self, prefix: str) -> list[str]: ...
    def read(self, key: str) -> bytes: ...
    def put_if_absent(self, key: str, data: bytes) -> bool: ...
    def overwrite(self, key: str, data: bytes) -> None: ...


class BronzeStore:
    __slots__ = ("_sink",)

    def __init__(self, sink: GcsSink) -> None:
        self._sink = sink

    def list_keys(self, prefix: str) -> list[str]:
        """One listing: the names under `prefix`, sorted (a Class A operation for each 1,000 names)."""
        return self._sink.list_keys(prefix)

    def read(self, key: str) -> bytes:
        return self._sink.read(key)

    def put_if_absent(self, key: str, data: bytes) -> bool:
        """Create a run marker. False, and nothing changed, if the name is taken. Any other key is refused."""
        if MARKER_KEY.fullmatch(key) is None:
            raise ConfigError(f"the store only creates run markers ({RUNS_PREFIX}<run_id>.json), not {key!r}")
        return self._sink.put_if_absent(key, data, content_type=MARKER_TYPE)

    def overwrite(self, key: str, data: bytes) -> None:
        """Replace the catalog copy. Any other key is refused."""
        if key != CATALOG_KEY:
            raise ConfigError(f"the store only overwrites the catalog copy ({CATALOG_KEY}), not {key!r}")
        self._sink.overwrite(key, data, content_type=CATALOG_TYPE)


def open_store(settings: Optional[BronzeSettings] = None, environ: Optional[Mapping[str, str]] = None,
               client_factory: Optional[Callable] = None) -> BronzeStore:
    """The store over the real Bronze bucket. Refuses unless the Bronze credentials are selected, and costs no
    operation until the first call."""
    settings = settings or BronzeSettings()
    require_credentials(environ)
    return BronzeStore(GcsSink.connect(settings.bucket, client_factory, project=settings.project))
