"""
landing.py: the read-only door to the landed CDC objects.

Bronze reads gs://<landing bucket>/cdc/<slot>/ and nothing else, and never writes there: the 908
landed objects are the deliverable and, with soft delete off, a deleted or overwritten object is gone
(GCS Plan (V2) sections 6 and 13). LandingReader is the only thing the rest of the package is given.
It offers list_keys and read and has no way to reach a write or a delete: the sink it wraps is held
privately, and put_if_absent is not forwarded. This is defence in depth. The real control is the
bronze-writer account, which has no write permission on the landing bucket; open_landing refuses to
connect unless that account's credentials file is selected (bronze.config.require_credentials).

An object name is checked against contract section 2 before anything is listed to the caller or read:

    cdc/<slot>/<table>/event_date=YYYY-MM-DD/lsn_<first 16 hex>_<last 16 hex>.ndjson.gz

A listed key that does not follow it, or lies outside the prefix, stops the run (LandingError). So does
a read of such a key, which also closes off keys like <prefix>../elsewhere. Failures of the bucket itself
are cdc.errors.SinkError, raised by GcsSink after its own retries.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Callable, Mapping, Optional, Protocol

from bronze.config import BronzeSettings, require_credentials
from bronze.errors import ConfigError, LandingError
from cdc.gcs import GcsSink

PREFIX = re.compile(r"cdc/[a-z0-9_]+/")
_NAME = re.compile(r"(?P<table>[a-z_]+)/event_date=(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})/"
                   r"lsn_(?P<first>[0-9a-f]{16})_(?P<last>[0-9a-f]{16})\.ndjson\.gz")


class Source(Protocol):
    """What the reader needs from a sink: cdc.gcs.GcsSink, or a fake with the same two calls."""
    def list_keys(self, prefix: str) -> list[str]: ...
    def read(self, key: str) -> bytes: ...


@dataclass(frozen=True, slots=True)
class LandedObject:
    """One landed object, as its name describes it."""
    key: str
    table: str
    event_date: str
    first_lsn: int   # lowest commit_lsn in the object
    last_lsn: int    # highest commit_lsn in the object


class LandingReader:
    __slots__ = ("_source", "prefix")

    def __init__(self, source: Source, prefix: str) -> None:
        if not PREFIX.fullmatch(prefix):
            raise ConfigError(f"the landing prefix must look like cdc/<slot>/, not {prefix!r}")
        self._source = source
        self.prefix = prefix

    def list_keys(self) -> list[LandedObject]:
        """Every object under the prefix, parsed, in key order. One listing (a Class A operation per
        1,000 names); call it once per run."""
        return [self.parse(key) for key in self._source.list_keys(self.prefix)]

    def read(self, key: str) -> bytes:
        """The object's bytes. The key must be a well-formed object name under the prefix."""
        self.parse(key)
        return self._source.read(key)

    def parse(self, key: str) -> LandedObject:
        if not isinstance(key, str) or not key.startswith(self.prefix):
            raise LandingError(f"{key!r} is outside the landing prefix {self.prefix}")
        match = _NAME.fullmatch(key[len(self.prefix):])
        if match is None:
            raise LandingError(f"{key}: not an object name of the form "
                               f"{self.prefix}<table>/event_date=<date>/lsn_<first>_<last>.ndjson.gz")
        try:
            date.fromisoformat(match["date"])
        except ValueError:
            raise LandingError(f"{key}: {match['date']} is not a calendar date") from None
        first, last = int(match["first"], 16), int(match["last"], 16)
        if first > last:
            raise LandingError(f"{key}: the first commit LSN is above the last")
        return LandedObject(key, match["table"], match["date"], first, last)


def open_landing(settings: Optional[BronzeSettings] = None, environ: Optional[Mapping[str, str]] = None,
                 client_factory: Optional[Callable] = None) -> LandingReader:
    """The reader over the real landing bucket. Refuses unless the Bronze credentials are selected, so
    no real-bucket access starts without the guard. Costs no operation until the first list or read."""
    settings = settings or BronzeSettings()
    require_credentials(environ)
    sink = GcsSink.connect(settings.landing_bucket, client_factory)
    return LandingReader(sink, settings.landing_prefix)
