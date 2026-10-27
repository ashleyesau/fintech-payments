"""
sink.py: where finished objects go.

The consumer never talks to GCS directly. It hands each finished object to a Sink, which has
one job: create this object under this name if nothing is there yet. The GCS writer (cdc/gcs.py)
is the real one; tests use an in-memory fake (tests/fakes.py) that follows the same rules.

The rules are what the delivery logic relies on (DECISION_LOG, M4-Q5 and M4-Q4b):
  - put_if_absent is atomic: afterwards the object exists in full, or it was not created.
  - It never overwrites. If the name is taken it returns False and leaves the object as it was.
  - Retrying transient failures is the sink's business. It raises SinkError only after giving up.
  - Both calls must be safe to make from several threads at once (a flush uploads in parallel).
"""
from __future__ import annotations

from typing import Protocol


class Sink(Protocol):
    def put_if_absent(self, key: str, data: bytes) -> bool:
        """Create the object. True if it was created, False if the key already exists.
        Raises SinkError if it could not be written after the sink's own retries."""

    def read(self, key: str) -> bytes:
        """The object's bytes. Raises SinkError if they could not be read, including when the
        object does not exist."""
