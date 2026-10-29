"""tests/test_cdc_gcs.py: M4 step 5, the GCS writer, against a fake storage client.

No network and no credentials: the client is a stand-in (tests/fakes.py) that follows GCS's rules
and can be scripted to fail. The errors raised are Google's real exception classes, so what the
writer decides about them is what it will decide in production. The real bucket is covered by the
manual smoke test (step 7), never by these tests.
"""
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests
from google.api_core import exceptions as api_errors
from google.auth import exceptions as auth_errors

from cdc.consumer import flush
from cdc.errors import ObjectMismatch, SinkError
from cdc.gcs import ATTEMPTS, CONTENT_TYPE, RETRY_WAITS, TIMEOUT_SECONDS, GcsSink, is_transient
from cdc.landing import build_batch, land_object
from tests.fakes import FakeGcsBucket, FakeStorageClient, Lost
from tests.test_cdc_envelope import NOW, assemble

LATER = datetime(2026, 10, 27, 9, 52, 30, 123000, tzinfo=timezone.utc)
KEY = "cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_0000000002b32560_0000000002b32560.ndjson.gz"
DATA = b"\x1f\x8b gzipped bytes"
ROOT = Path(__file__).resolve().parent.parent


def make(script=None):
    bucket = FakeGcsBucket(script=script)
    sleeps = []
    return GcsSink(bucket, sleep=sleeps.append), bucket, sleeps


def http(status, message="boom"):
    return api_errors.from_http_status(status, message)


# ---------------------------------------------------------------------------
# what is sent
# ---------------------------------------------------------------------------

def test_an_object_is_created_with_the_precondition_the_gzip_type_and_the_libraries_retry_off():
    sink, bucket, sleeps = make()
    assert sink.put_if_absent(KEY, DATA) is True
    [call] = bucket.calls
    assert call["op"] == "upload" and call["name"] == KEY and call["data"] == DATA and call["args"] == ()
    assert call["kwargs"] == {"content_type": "application/gzip", "if_generation_match": 0,
                              "timeout": 60, "retry": None}
    assert CONTENT_TYPE == "application/gzip" and TIMEOUT_SECONDS == 60
    assert bucket.objects == {KEY: DATA} and bucket.content_types == {KEY: "application/gzip"}
    assert sleeps == []


def test_no_content_encoding_is_ever_set_because_gcs_would_transcode_on_download():
    sink, bucket, _ = make()
    sink.put_if_absent(KEY, DATA)
    assert all(blob.content_encoding is None for blob in bucket.blobs)
    assert "content_encoding" not in bucket.calls[0]["kwargs"]


def test_a_read_asks_for_the_bytes_with_the_timeout_and_the_libraries_retry_off():
    sink, bucket, _ = make()
    sink.put_if_absent(KEY, DATA)
    assert sink.read(KEY) == DATA
    assert bucket.calls[-1]["op"] == "download" and bucket.calls[-1]["kwargs"] == {"timeout": 60, "retry": None}


def test_a_new_blob_is_made_for_every_call_so_threads_share_nothing():
    sink, bucket, _ = make()
    sink.put_if_absent(KEY, DATA)
    sink.read(KEY)
    assert len(bucket.blobs) == 2 and bucket.blobs[0] is not bucket.blobs[1]


# ---------------------------------------------------------------------------
# an object that already exists
# ---------------------------------------------------------------------------

def test_an_existing_object_is_not_a_failure_it_returns_false_without_retrying_or_overwriting():
    sink, bucket, sleeps = make()
    sink.put_if_absent(KEY, DATA)
    assert sink.put_if_absent(KEY, b"other bytes") is False
    assert bucket.objects == {KEY: DATA}                    # never overwritten
    assert len(bucket.calls) == 2 and sleeps == []


# ---------------------------------------------------------------------------
# retries (M4-Q4b): 5 attempts, waits of 1, 2, 4 and 8 seconds
# ---------------------------------------------------------------------------

def test_the_schedule_is_five_attempts_with_waits_of_one_two_four_and_eight_seconds():
    assert RETRY_WAITS == (1, 2, 4, 8) and ATTEMPTS == 5


@pytest.mark.parametrize("failures", [1, 2, 3, 4])
def test_a_transient_failure_is_retried_after_the_scheduled_waits_and_then_succeeds(failures):
    sink, bucket, sleeps = make(script=[api_errors.ServiceUnavailable("try later")] * failures)
    assert sink.put_if_absent(KEY, DATA) is True
    assert sleeps == list(RETRY_WAITS[:failures])
    assert len(bucket.calls) == failures + 1
    assert all(c["kwargs"] == bucket.calls[0]["kwargs"] for c in bucket.calls)     # same arguments every attempt
    assert bucket.objects == {KEY: DATA}


def test_after_five_failed_attempts_it_gives_up_and_never_sleeps_after_the_last():
    sink, bucket, sleeps = make(script=[api_errors.ServiceUnavailable("try later")] * 6)
    with pytest.raises(SinkError) as raised:
        sink.put_if_absent(KEY, DATA)
    assert len(bucket.calls) == 5 and sleeps == [1, 2, 4, 8]
    message = str(raised.value)
    assert "gave up after 5 attempts" in message and KEY in message and "cdc_landing" in message
    assert "ServiceUnavailable" in message and "try later" in message
    assert bucket.objects == {}


TRANSIENT = [
    http(429, "slow down"), http(500), http(502), http(503), http(504), http(408, "request timeout"),
    requests.exceptions.ConnectionError("connection reset"), requests.exceptions.ReadTimeout("read timed out"),
    requests.exceptions.ConnectTimeout("connect timed out"), requests.exceptions.ChunkedEncodingError("cut off"),
    auth_errors.TransportError("token endpoint unreachable"), ConnectionResetError("reset by peer"), TimeoutError("timed out"),
]


@pytest.mark.parametrize("error", TRANSIENT, ids=lambda e: f"{type(e).__name__}-{getattr(e, 'code', '')}")
def test_every_transient_failure_is_retried_on_a_write_and_on_a_read(error):
    assert is_transient(error)
    sink, bucket, sleeps = make(script=[error])
    assert sink.put_if_absent(KEY, DATA) is True and sleeps == [1]
    sink2, _, sleeps2 = make()
    sink2._bucket.objects[KEY] = DATA
    sink2._bucket.script = [error]
    assert sink2.read(KEY) == DATA and sleeps2 == [1]


PERMANENT = [
    api_errors.Forbidden("no access"), api_errors.Unauthorized("bad token"), api_errors.NotFound("no such bucket"),
    api_errors.BadRequest("bad"), api_errors.Conflict("conflict"), http(400), http(401), http(403), http(404),
    auth_errors.RefreshError("token refresh refused"), auth_errors.DefaultCredentialsError("no credentials"),
]


@pytest.mark.parametrize("error", PERMANENT, ids=lambda e: f"{type(e).__name__}-{getattr(e, 'code', '')}")
def test_a_permanent_failure_fails_at_once_without_retrying(error):
    assert not is_transient(error)
    sink, bucket, sleeps = make(script=[error])
    with pytest.raises(SinkError) as raised:
        sink.put_if_absent(KEY, DATA)
    assert len(bucket.calls) == 1 and sleeps == []
    assert raised.value.__cause__ is error
    assert "Retrying will not help" in str(raised.value) or "Not found" in str(raised.value)


def test_a_permission_failure_says_to_check_the_login_and_a_missing_bucket_names_it():
    sink, _, _ = make(script=[api_errors.Forbidden("no access")])
    with pytest.raises(SinkError, match="gcloud auth application-default login"):
        sink.put_if_absent(KEY, DATA)
    sink, _, _ = make(script=[api_errors.NotFound("no such bucket")])
    with pytest.raises(SinkError, match=r"check the bucket name \(cdc_landing\)"):
        sink.put_if_absent(KEY, DATA)


def test_something_that_is_not_a_storage_failure_is_a_bug_and_propagates_untouched():
    sink, bucket, sleeps = make(script=[ValueError("a bug of ours")])
    with pytest.raises(ValueError, match="a bug of ours"):
        sink.put_if_absent(KEY, DATA)
    assert len(bucket.calls) == 1 and sleeps == []


def test_a_precondition_failure_is_never_treated_as_transient():
    assert not is_transient(http(412))


# ---------------------------------------------------------------------------
# reads
# ---------------------------------------------------------------------------

def test_a_missing_object_on_a_read_is_a_sink_error_that_is_not_retried():
    sink, bucket, sleeps = make()
    with pytest.raises(SinkError, match="Not found"):
        sink.read(KEY)
    assert len(bucket.calls) == 1 and sleeps == []


def test_a_read_gives_up_after_five_attempts_like_a_write():
    sink, bucket, sleeps = make(script=[requests.exceptions.ReadTimeout("slow")] * 6)
    bucket.objects[KEY] = DATA
    with pytest.raises(SinkError, match="could not read .* gave up after 5 attempts"):
        sink.read(KEY)
    assert sleeps == [1, 2, 4, 8] and len(bucket.calls) == 5


# ---------------------------------------------------------------------------
# listing (the reconciler's one listing)
# ---------------------------------------------------------------------------

def test_list_keys_makes_one_listing_under_the_prefix_and_returns_sorted_names():
    sink, bucket, sleeps = make()
    for name in ("cdc/cdc_r01/b", "cdc/cdc_r01/a", "cdc/cdc_r02/z", "other/q"):
        bucket.objects[name] = b"x"
    assert sink.list_keys("cdc/cdc_r01/") == ["cdc/cdc_r01/a", "cdc/cdc_r01/b"]
    [call] = bucket.calls
    assert call["op"] == "list" and call["kwargs"] == {"prefix": "cdc/cdc_r01/", "timeout": 60, "retry": None}
    assert sleeps == []


def test_a_listing_is_retried_like_a_write_and_gives_up_after_five_attempts():
    sink, bucket, sleeps = make(script=[api_errors.ServiceUnavailable("try later")] * 2)
    bucket.objects["p/a"] = b"x"
    assert sink.list_keys("p/") == ["p/a"] and sleeps == [1, 2] and len(bucket.calls) == 3
    sink, bucket, sleeps = make(script=[api_errors.ServiceUnavailable("try later")] * 6)
    with pytest.raises(SinkError, match="could not list p/ .* gave up after 5 attempts"):
        sink.list_keys("p/")
    assert sleeps == [1, 2, 4, 8]


def test_a_listing_that_is_refused_fails_at_once():
    sink, bucket, sleeps = make(script=[api_errors.Forbidden("no access")])
    with pytest.raises(SinkError, match="application-default login"):
        sink.list_keys("p/")
    assert len(bucket.calls) == 1 and sleeps == []


# ---------------------------------------------------------------------------
# with the landing logic (M4-Q5, option B) and the flush
# ---------------------------------------------------------------------------

def objects_for():
    return build_batch(assemble("inserts_utc") + assemble("disbursement"), "fintech_cdc", "cdc/cdc_r01/", NOW)


def test_a_lost_reply_is_absorbed_the_retry_finds_the_object_there_and_the_compare_accepts_it():
    obj = objects_for()[0]
    # the upload reaches GCS but the reply is lost; the retry then meets the object it just wrote
    sink, bucket, sleeps = make(script=[Lost(requests.exceptions.ReadTimeout("reply lost"))])
    assert land_object(sink, obj) == "replayed"
    assert bucket.objects == {obj.key: obj.data} and sleeps == [1]
    assert [c["op"] for c in bucket.calls] == ["upload", "upload", "download"]


def test_a_replay_with_a_later_clock_is_recognised_and_the_original_object_is_kept():
    first, replay = objects_for()[0], build_batch(assemble("inserts_utc") + assemble("disbursement"),
                                                  "fintech_cdc", "cdc/cdc_r01/", LATER)[0]
    sink, bucket, _ = make()
    assert land_object(sink, first) == "created"
    assert land_object(sink, replay) == "replayed"
    assert bucket.objects == {first.key: first.data}


def test_a_different_object_under_the_same_name_stops_the_consumer():
    obj = objects_for()[0]
    sink, bucket, _ = make()
    bucket.objects[obj.key] = b"someone else's bytes"
    with pytest.raises(ObjectMismatch):
        land_object(sink, obj)
    assert bucket.objects[obj.key] == b"someone else's bytes"


def test_a_flush_of_many_objects_through_eight_threads_lands_each_exactly_once():
    transactions = assemble("inserts_utc") + assemble("disbursement")
    objects = [o for t in transactions for o in build_batch([t], "fintech_cdc", "cdc/cdc_r01/", NOW)]
    assert len(objects) == 8
    sink, bucket, _ = make()
    result = flush(sink, objects, workers=8)
    assert (result.created, result.replayed) == (8, 0)
    assert bucket.objects == {o.key: o.data for o in objects}
    assert [c["op"] for c in bucket.calls] == ["upload"] * 8


def test_a_permanent_failure_in_a_flush_surfaces_as_a_sink_error_and_uploads_nothing_more_to_retry():
    objects = objects_for()
    sink, bucket, sleeps = make(script=[api_errors.Forbidden("no access")] * 100)
    with pytest.raises(SinkError, match="application-default login"):
        flush(sink, objects, workers=2)
    assert sleeps == []


# ---------------------------------------------------------------------------
# connecting, and keeping Google's libraries out of everything else
# ---------------------------------------------------------------------------

def test_connect_uses_the_bucket_name_and_the_application_default_credentials():
    client = FakeStorageClient()
    sink = GcsSink.connect("cdc_landing", client_factory=lambda: client)
    assert client.requested == ["cdc_landing"] and sink.bucket_name == "cdc_landing"


def test_missing_credentials_are_a_sink_error_that_says_how_to_sign_in():
    def no_credentials():
        raise auth_errors.DefaultCredentialsError("Your default credentials were not found")

    with pytest.raises(SinkError) as raised:
        GcsSink.connect("cdc_landing", client_factory=no_credentials)
    assert "gcloud auth application-default login" in str(raised.value)


def recording_client(monkeypatch):
    """Replace the real storage.Client with one that records how it was called."""
    import cdc.gcs
    calls = []

    def make(*args, **kwargs):
        calls.append((args, kwargs))
        return FakeStorageClient()

    monkeypatch.setattr(cdc.gcs.storage, "Client", make)
    return calls


def test_connect_without_a_project_makes_the_client_exactly_as_before(monkeypatch):
    # storage.Client(project=None) is "no project" to Google, not "work it out": the consumer must not pass it.
    calls = recording_client(monkeypatch)
    GcsSink.connect("cdc_landing")
    assert calls == [((), {})]


def test_connect_with_a_project_makes_the_client_for_that_project(monkeypatch):
    calls = recording_client(monkeypatch)
    GcsSink.connect("cdc_landing", project="payments-512910")
    assert calls == [((), {"project": "payments-512910"})]


def test_an_injected_client_factory_is_used_as_it_is_whatever_the_project(monkeypatch):
    calls = recording_client(monkeypatch)
    made = []
    GcsSink.connect("cdc_landing", client_factory=lambda: made.append(1) or FakeStorageClient(), project="p")
    assert made == [1] and calls == []


def test_nothing_but_the_gcs_module_pulls_in_googles_libraries():
    code = ("import sys, cdc.consumer, cdc.landing, cdc.reader, cdc.dedup, cdc.envelope, cdc.pgoutput, "
            "cdc.transactions, cdc.columns, cdc.sink, cdc.lag, cdc.reconcile; "
            "bad = sorted(m for m in sys.modules if m.split('.')[0] in ('google', 'googleapiclient', 'requests')); "
            "print(bad); sys.exit(1 if bad else 0)")
    result = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True,
                            env={"PYTHONPATH": str(ROOT), "PATH": "/usr/bin:/usr/local/bin"})
    assert result.returncode == 0, result.stdout + result.stderr


def test_the_gcs_module_never_reads_a_key_file_or_a_path_from_the_environment():
    source = (ROOT / "cdc" / "gcs.py").read_text()
    for forbidden in ("service_account", "from_service_account", "GOOGLE_APPLICATION_CREDENTIALS", "credentials_file", "open("):
        assert forbidden not in source, forbidden


# ---------------------------------------------------------------------------
# overwrite and the content type (the Bronze job's catalog copy and run markers)
# ---------------------------------------------------------------------------

CATALOG = "_catalog/catalog.db"
SQLITE = "application/vnd.sqlite3"


def test_an_overwrite_sends_no_precondition_the_given_type_the_timeout_and_the_libraries_retry_off():
    sink, bucket, sleeps = make()
    assert sink.overwrite(CATALOG, DATA, SQLITE) is None
    [call] = bucket.calls
    assert call["op"] == "upload" and call["name"] == CATALOG and call["data"] == DATA and call["args"] == ()
    assert call["kwargs"] == {"content_type": SQLITE, "timeout": 60, "retry": None}
    assert "if_generation_match" not in call["kwargs"]
    assert bucket.content_types == {CATALOG: SQLITE} and sleeps == []
    assert all(blob.content_encoding is None for blob in bucket.blobs)


def test_an_overwrite_replaces_the_object_and_a_create_still_refuses_to():
    sink, bucket, _ = make()
    sink.overwrite(CATALOG, b"first", SQLITE)
    sink.overwrite(CATALOG, b"second", SQLITE)
    assert bucket.objects == {CATALOG: b"second"}
    assert sink.put_if_absent(CATALOG, b"third") is False and bucket.objects == {CATALOG: b"second"}


def test_a_create_can_name_its_content_type_and_the_default_is_unchanged():
    sink, bucket, _ = make()
    sink.put_if_absent("_runs/a.json", b"{}", content_type="application/json")
    sink.put_if_absent(KEY, DATA)
    assert bucket.calls[0]["kwargs"]["content_type"] == "application/json"
    assert bucket.calls[0]["kwargs"]["if_generation_match"] == 0
    assert bucket.calls[1]["kwargs"]["content_type"] == "application/gzip"


@pytest.mark.parametrize("failures", [1, 2, 3, 4])
def test_an_overwrite_is_retried_on_the_same_schedule_as_a_write(failures):
    sink, bucket, sleeps = make(script=[api_errors.ServiceUnavailable("try later")] * failures)
    sink.overwrite(CATALOG, DATA, SQLITE)
    assert sleeps == list(RETRY_WAITS[:failures]) and len(bucket.calls) == failures + 1
    assert bucket.objects == {CATALOG: DATA}


def test_an_overwrite_gives_up_after_five_attempts_and_says_what_it_was_doing():
    sink, bucket, sleeps = make(script=[api_errors.ServiceUnavailable("try later")] * 6)
    with pytest.raises(SinkError) as raised:
        sink.overwrite(CATALOG, DATA, SQLITE)
    assert len(bucket.calls) == 5 and sleeps == [1, 2, 4, 8]
    assert "could not overwrite _catalog/catalog.db" in str(raised.value) and "gave up after 5 attempts" in str(raised.value)
    assert bucket.objects == {}


def test_an_overwrite_that_is_refused_fails_at_once_and_says_to_check_the_login():
    sink, bucket, sleeps = make(script=[api_errors.Forbidden("no")])
    with pytest.raises(SinkError) as raised:
        sink.overwrite(CATALOG, DATA, SQLITE)
    assert len(bucket.calls) == 1 and sleeps == []
    assert "Retrying will not help" in str(raised.value) and "application-default login" in str(raised.value)


def test_after_a_lost_reply_an_overwrite_writes_the_same_bytes_again():
    sink, bucket, sleeps = make(script=[Lost(requests.exceptions.ConnectionError("reply lost"))])
    sink.overwrite(CATALOG, DATA, SQLITE)
    assert len(bucket.calls) == 2 and sleeps == [1] and bucket.objects == {CATALOG: DATA}
