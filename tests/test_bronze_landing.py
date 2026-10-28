"""tests/test_bronze_landing.py: G1 step 2, the read-only landing reader.

No cloud: the sink is the in-memory fake, and the real GcsSink is exercised over the fake bucket
(tests/fakes.py), so every call the reader makes is visible. The properties that matter: it can list
and read and nothing else, it never leaves the run's prefix, and a name the contract does not allow
stops the run.
"""
import ast
import inspect
from pathlib import Path

import pytest

import bronze.landing as landing_module
from bronze.config import CREDENTIALS_VARIABLE, BronzeSettings
from bronze.errors import BronzeError, ConfigError, LandingError
from bronze.landing import LandedObject, LandingReader, open_landing
from cdc.errors import SinkError
from cdc.gcs import GcsSink
from cdc.landing import object_key
from tests.fakes import FakeGcsBucket, FakeStorageClient, MemorySink

PREFIX = "cdc/cdc_r01/"
TABLES = ("merchants", "merchant_transactions", "loan_applications", "loan_repayments", "risk_events")


def key(table="merchants", date="2025-03-01", first=0x2B32560, last=0x2B32560, prefix=PREFIX):
    return object_key(prefix, table, date, first, last)


def reader_over(*keys, prefix=PREFIX):
    sink = MemorySink()
    for k in keys:
        sink.put_if_absent(k, b"data of " + k.encode())
    return LandingReader(sink, prefix), sink


# ---------------------------------------------------------------------------
# the prefix
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("prefix", ["cdc/cdc_r01/", "cdc/cdc_r02/", "cdc/a1_b/"])
def test_a_run_prefix_is_accepted(prefix):
    assert LandingReader(MemorySink(), prefix).prefix == prefix


@pytest.mark.parametrize("prefix", ["", "cdc/", "cdc//", "cdc/cdc_r01", "cdc/Cdc_R01/", "cdc/a/b/", "/cdc/cdc_r01/",
                                    "cdc/cdc_r01/\n", "other/cdc_r01/", "cdc/../", "cdc/cdc r01/"])
def test_anything_else_is_a_config_error(prefix):
    with pytest.raises(ConfigError):
        LandingReader(MemorySink(), prefix)


# ---------------------------------------------------------------------------
# object names (contract section 2)
# ---------------------------------------------------------------------------

def test_a_name_built_by_the_consumer_parses_back_to_its_parts():
    reader, _ = reader_over()
    parsed = reader.parse(object_key(PREFIX, "merchant_transactions", "2025-03-01", 0x2B32560, 0x2B9FFFF))
    assert parsed == LandedObject(
        key=f"{PREFIX}merchant_transactions/event_date=2025-03-01/lsn_0000000002b32560_0000000002b9ffff.ndjson.gz",
        table="merchant_transactions", event_date="2025-03-01", first_lsn=0x2B32560, last_lsn=0x2B9FFFF)


@pytest.mark.parametrize("table", TABLES)
def test_every_table_of_the_contract_parses(table):
    reader, _ = reader_over()
    assert reader.parse(key(table)).table == table


BAD_NAMES = {
    "another slot": "cdc/cdc_r02/merchants/event_date=2025-03-01/lsn_0000000002b32560_0000000002b32560.ndjson.gz",
    "a prefix that only starts the same": "cdc/cdc_r011/merchants/event_date=2025-03-01/lsn_0000000002b32560_0000000002b32560.ndjson.gz",
    "no table folder": "cdc/cdc_r01/event_date=2025-03-01/lsn_0000000002b32560_0000000002b32560.ndjson.gz",
    "an extra folder": "cdc/cdc_r01/x/merchants/event_date=2025-03-01/lsn_0000000002b32560_0000000002b32560.ndjson.gz",
    "a month as the date": "cdc/cdc_r01/merchants/event_date=2025-03/lsn_0000000002b32560_0000000002b32560.ndjson.gz",
    "month 13": "cdc/cdc_r01/merchants/event_date=2025-13-01/lsn_0000000002b32560_0000000002b32560.ndjson.gz",
    "30 February": "cdc/cdc_r01/merchants/event_date=2025-02-30/lsn_0000000002b32560_0000000002b32560.ndjson.gz",
    "upper case hex": "cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_0000000002B32560_0000000002b32560.ndjson.gz",
    "short hex": "cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_000000002b32560_0000000002b32560.ndjson.gz",
    "short hex in the last LSN": "cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_0000000002b32560_000000002b32560.ndjson.gz",
    "long hex": "cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_00000000002b32560_0000000002b32560.ndjson.gz",
    "no gzip": "cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_0000000002b32560_0000000002b32560.ndjson",
    "a suffix after gzip": "cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_0000000002b32560_0000000002b32560.ndjson.gz.tmp",
    "a trailing line feed": "cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_0000000002b32560_0000000002b32560.ndjson.gz\n",
    "non-ASCII digits in the date": "cdc/cdc_r01/merchants/event_date=\u0662\u0660\u0662\u0665-03-01/lsn_0000000002b32560_0000000002b32560.ndjson.gz",
    "first above last": "cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_0000000002b32561_0000000002b32560.ndjson.gz",
    "a traversal": "cdc/cdc_r01/../cdc_r02/merchants/event_date=2025-03-01/lsn_0000000002b32560_0000000002b32560.ndjson.gz",
    "a folder placeholder": "cdc/cdc_r01/merchants/",
    "a stray file": "cdc/cdc_r01/notes.txt",
    "upper case table": "cdc/cdc_r01/Merchants/event_date=2025-03-01/lsn_0000000002b32560_0000000002b32560.ndjson.gz",
}


@pytest.mark.parametrize("name", BAD_NAMES.values(), ids=BAD_NAMES.keys())
def test_a_name_the_contract_does_not_allow_stops_the_run(name):
    reader, _ = reader_over()
    with pytest.raises(LandingError):
        reader.parse(name)


def test_a_date_that_is_not_plain_ascii_is_not_an_object_name_at_all():
    reader, _ = reader_over()
    with pytest.raises(LandingError, match="not an object name"):
        reader.parse(BAD_NAMES["non-ASCII digits in the date"])


def test_a_non_string_key_is_refused():
    reader, _ = reader_over()
    with pytest.raises(LandingError):
        reader.parse(None)


def test_a_landing_error_is_a_bronze_error():
    assert issubclass(LandingError, BronzeError)


# ---------------------------------------------------------------------------
# listing
# ---------------------------------------------------------------------------

def test_the_listing_is_one_call_under_the_prefix_and_only_this_runs_objects():
    mine = [key("merchants", "2025-03-01", 1, 5), key("risk_events", "2025-03-02", 6, 9)]
    other = key(prefix="cdc/cdc_r02/")
    reader, sink = reader_over(*mine, other)
    listed = reader.list_keys()
    assert [o.key for o in listed] == sorted(mine)
    assert all(isinstance(o, LandedObject) for o in listed)
    assert sink.listings == 1 and sink.reads == 0


def test_an_empty_prefix_lists_nothing():
    reader, _ = reader_over()
    assert reader.list_keys() == []


def test_a_stray_object_under_the_prefix_stops_the_run_and_is_named():
    reader, _ = reader_over(key(), f"{PREFIX}notes.txt")
    with pytest.raises(LandingError, match="notes.txt"):
        reader.list_keys()


def test_a_source_that_lists_outside_the_prefix_stops_the_run():
    class Leaky:
        def list_keys(self, prefix):
            return [key(prefix="cdc/cdc_r02/")]

        def read(self, key):
            raise AssertionError("not read")

    with pytest.raises(LandingError, match="outside"):
        LandingReader(Leaky(), PREFIX).list_keys()


# ---------------------------------------------------------------------------
# reading
# ---------------------------------------------------------------------------

def test_a_read_returns_the_objects_bytes():
    k = key()
    reader, sink = reader_over(k)
    assert reader.read(k) == b"data of " + k.encode()
    assert sink.reads == 1


@pytest.mark.parametrize("name", BAD_NAMES.values(), ids=BAD_NAMES.keys())
def test_a_read_of_a_key_the_run_may_not_read_never_reaches_the_sink(name):
    reader, sink = reader_over(key(), name)
    with pytest.raises(LandingError):
        reader.read(name)
    assert sink.reads == 0


def test_a_missing_object_is_the_sinks_error_not_a_landing_error():
    reader, _ = reader_over()
    with pytest.raises(SinkError):
        reader.read(key())


# ---------------------------------------------------------------------------
# read only
# ---------------------------------------------------------------------------

def test_the_reader_offers_list_and_read_and_no_way_to_write():
    reader, _ = reader_over()
    public = {name for name in dir(reader) if not name.startswith("_")}
    assert public == {"prefix", "list_keys", "read", "parse"}


def test_the_reader_has_no_dict_to_stash_a_sink_in():
    reader, _ = reader_over()
    assert not hasattr(reader, "__dict__")


def test_the_only_calls_the_module_makes_on_its_sink_are_list_and_read():
    tree = ast.parse(inspect.getsource(landing_module))
    used = {node.attr for node in ast.walk(tree)
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Attribute)
            and node.value.attr == "_source"}
    assert used == {"list_keys", "read"}


def test_a_full_listing_and_read_never_touches_a_write_method():
    class Trap(MemorySink):
        def put_if_absent(self, key, data):
            raise AssertionError("the reader wrote")

    sink = Trap()
    sink.objects[key()] = b"x"
    reader = LandingReader(sink, PREFIX)
    for landed in reader.list_keys():
        reader.read(landed.key)
    assert sink.attempts == 0


def test_over_the_real_sink_only_a_list_and_a_read_reach_the_bucket():
    bucket = FakeGcsBucket()
    bucket.objects[key()] = b"x"
    reader = LandingReader(GcsSink(bucket, sleep=lambda s: None), PREFIX)
    for landed in reader.list_keys():
        reader.read(landed.key)
    assert [call["op"] for call in bucket.calls] == ["list", "download"]
    assert bucket.calls[0]["kwargs"]["prefix"] == PREFIX


# ---------------------------------------------------------------------------
# open_landing: the guard comes first
# ---------------------------------------------------------------------------

def clear(monkeypatch):
    for variable in ("CDC_GCS_BUCKET", "CDC_SLOT_NAME", "BRONZE_BUCKET", "BRONZE_CATALOG_PATH"):
        monkeypatch.delenv(variable, raising=False)


def test_no_connection_is_made_without_the_credentials_guard_passing(monkeypatch):
    clear(monkeypatch)
    client = FakeStorageClient()
    made = []
    with pytest.raises(ConfigError):
        open_landing(BronzeSettings(_env_file=None), environ={}, client_factory=lambda: made.append(1) or client)
    assert made == [] and client.requested == []


def test_the_reader_opens_on_the_landing_bucket_and_slot_of_the_cdc_settings(monkeypatch, tmp_path):
    clear(monkeypatch)
    creds = tmp_path / "creds.json"
    creds.write_text("{}")
    client = FakeStorageClient()
    client._bucket.objects[key()] = b"x"
    reader = open_landing(BronzeSettings(_env_file=None), environ={CREDENTIALS_VARIABLE: str(creds)},
                          client_factory=lambda: client)
    assert client.requested == ["cdc_landing"] and reader.prefix == PREFIX
    assert [o.key for o in reader.list_keys()] == [key()]


def test_a_new_slot_name_moves_the_reader_to_its_prefix(monkeypatch, tmp_path):
    clear(monkeypatch)
    monkeypatch.setenv("CDC_SLOT_NAME", "cdc_r02")
    monkeypatch.setenv("CDC_GCS_BUCKET", "elsewhere")
    creds = tmp_path / "creds.json"
    creds.write_text("{}")
    client = FakeStorageClient()
    reader = open_landing(BronzeSettings(_env_file=None), environ={CREDENTIALS_VARIABLE: str(creds)},
                          client_factory=lambda: client)
    assert client.requested == ["elsewhere"] and reader.prefix == "cdc/cdc_r02/"
