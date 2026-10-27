"""
gcs.py: the real sink. Objects go to a GCS bucket; this is the only module that imports Google's
libraries, so everything else (decoder, envelope, reader, loop, fakes) runs without them.

It follows the Sink rules (cdc/sink.py) and the decisions M4-Q4b and M4-Q5:

  - An object is created with the precondition if_generation_match=0, so an existing object is
    never overwritten; "already exists" (HTTP 412) is not a failure, it returns False and the
    caller compares the two (landing.land_object).
  - Content type application/gzip, and no Content-Encoding (the contract: with it GCS would
    transcode on download and some clients would hand back decompressed bytes).
  - Retries are this module's own: up to 5 attempts, waiting 1, 2, 4 and 8 seconds between them,
    each attempt with a 60 second timeout. The library's own retry is switched off (retry=None) so
    two retry layers never stack and the schedule can be tested. Only transient failures are
    retried: timeouts, dropped connections, and HTTP 408, 429 and 5xx. A permission or
    authentication error, a missing bucket and the like fail at once. Anything else is a bug and
    propagates as it is.
  - After a lost response (the upload reached GCS but the reply did not come back) the retry finds
    the object already there, returns False, and the caller's compare recognises its own bytes.

The client is shared by the flush's worker threads (8 at a time worked with no errors in the
latency runs); a Blob is made fresh for each call, so no state is shared between them.
"""
from __future__ import annotations

import time
from typing import Callable, Optional, TypeVar

import requests
from google.api_core import exceptions as api_errors
from google.auth import exceptions as auth_errors
from google.cloud import storage

from cdc.errors import SinkError

CONTENT_TYPE = "application/gzip"
TIMEOUT_SECONDS = 60
RETRY_WAITS = (1, 2, 4, 8)          # seconds slept between attempts; so there are five attempts
ATTEMPTS = len(RETRY_WAITS) + 1
TRANSIENT_STATUS = frozenset({408, 429, 500, 502, 503, 504})

_TRANSIENT_TRANSPORT = (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                        requests.exceptions.ChunkedEncodingError, auth_errors.TransportError,
                        ConnectionError, TimeoutError)
# What this module judges (retry or fail at once); anything not listed is not a storage failure.
_JUDGED = (api_errors.GoogleAPICallError, auth_errors.GoogleAuthError) + _TRANSIENT_TRANSPORT

T = TypeVar("T")


def is_transient(error: BaseException) -> bool:
    if isinstance(error, api_errors.GoogleAPICallError):
        return error.code in TRANSIENT_STATUS
    return isinstance(error, _TRANSIENT_TRANSPORT)


def _describe(error: BaseException) -> str:
    return f"{type(error).__name__}: {error}"


class GcsSink:
    def __init__(self, bucket: "storage.Bucket", sleep: Callable[[float], None] = time.sleep) -> None:
        self._bucket = bucket
        self._sleep = sleep
        self.bucket_name = bucket.name

    @classmethod
    def connect(cls, bucket_name: str, client_factory: Optional[Callable[[], "storage.Client"]] = None,
                sleep: Callable[[float], None] = time.sleep) -> "GcsSink":
        """A sink for `bucket_name`, signed in with the application-default credentials (never a key
        file; nothing is read from the repo)."""
        try:
            client = (client_factory or storage.Client)()
        except auth_errors.DefaultCredentialsError as error:
            raise SinkError("No Google credentials found. Sign in with: gcloud auth application-default login "
                            "(then: gcloud auth application-default set-quota-project <project>)") from error
        return cls(client.bucket(bucket_name), sleep)

    def put_if_absent(self, key: str, data: bytes) -> bool:
        def attempt() -> bool:
            try:
                self._bucket.blob(key).upload_from_string(
                    data, content_type=CONTENT_TYPE, if_generation_match=0, timeout=TIMEOUT_SECONDS, retry=None)
            except api_errors.PreconditionFailed:
                return False
            return True
        return self._retrying(f"write {key}", attempt)

    def read(self, key: str) -> bytes:
        return self._retrying(
            f"read {key}", lambda: self._bucket.blob(key).download_as_bytes(timeout=TIMEOUT_SECONDS, retry=None))

    def list_keys(self, prefix: str) -> list[str]:
        """The names of the objects under `prefix`, sorted. One listing (a Class A operation for each page of 1,000
        names), made once by the reconciler and never in a loop, with the same retries as a write."""
        return self._retrying(
            f"list {prefix}",
            lambda: sorted(blob.name for blob in self._bucket.list_blobs(prefix=prefix, timeout=TIMEOUT_SECONDS, retry=None)))

    def _retrying(self, what: str, attempt: Callable[[], T]) -> T:
        for number in range(1, ATTEMPTS + 1):
            try:
                return attempt()
            except _JUDGED as error:
                if not is_transient(error):
                    raise SinkError(self._permanent(what, error)) from error
                if number == ATTEMPTS:
                    raise SinkError(f"could not {what} in bucket {self.bucket_name}: gave up after {ATTEMPTS} "
                                    f"attempts, the last failing with {_describe(error)}") from error
                self._sleep(RETRY_WAITS[number - 1])
        raise AssertionError("unreachable")

    def _permanent(self, what: str, error: BaseException) -> str:
        base = f"could not {what} in bucket {self.bucket_name}: {_describe(error)}"
        if isinstance(error, (api_errors.Forbidden, api_errors.Unauthorized, auth_errors.GoogleAuthError)):
            return (f"{base}. Retrying will not help: check the application-default login "
                    "(gcloud auth application-default login) and that the account may use the bucket.")
        if isinstance(error, api_errors.NotFound):
            return f"{base}. Not found: check the bucket name ({self.bucket_name}) or that the object exists."
        return f"{base}. Retrying will not help."
