"""tests/test_bronze_store.py: G3 step 4, the Bronze bucket's own objects (bronze/store.py).

What is held here, each with a mutation that fails it (see the commit message):
  - BronzeStore creates only run markers and overwrites only the catalog copy, so a bug elsewhere cannot write into
    bronze/<table>/; a refused key never reaches the bucket;
  - what is sent: the precondition on a marker, the type of each object, nothing else;
  - open_store refuses without the Bronze credentials before any client is made, connects to the Bronze bucket (not the
    landing bucket), passes the project, and works with an impersonated credentials file that has no project;
  - the store offers exactly six calls (G4 added the listing with sizes and times and the delete), and a delete is
    refused for anything but a file under bronze/<table>/data/ or bronze/<table>/metadata/ of one of the five tables.
No network: the sink is the real GcsSink over a fake bucket (tests/fakes.py).
"""
import pytest

from bronze.config import CREDENTIALS_VARIABLE, BronzeSettings
from bronze.errors import ConfigError
from bronze.store import CATALOG_KEY, MARKER_KEY, RUNS_PREFIX, BronzeStore, is_table_file, open_store
from cdc.gcs import GcsSink
from tests.fakes import FakeGcsBucket, FakeStorageClient
from tests.test_bronze_landing import IMPERSONATED_FILE

RUN_ID = "20261029T153000123456Z"
MARKER = f"_runs/{RUN_ID}.json"


def make():
    bucket = FakeGcsBucket(name="fintech_bronze")
    return BronzeStore(GcsSink(bucket, sleep=lambda seconds: None)), bucket


def clear(monkeypatch):
    for variable in ("BRONZE_BUCKET", "BRONZE_PROJECT", "CDC_GCS_BUCKET", "CDC_SLOT_NAME", "CDC_DB"):
        monkeypatch.delenv(variable, raising=False)


# ---------------------------------------------------------------------------
# what may be written
# ---------------------------------------------------------------------------

def test_a_marker_is_created_with_the_precondition_and_the_json_type():
    store, bucket = make()
    assert store.put_if_absent(MARKER, b"{}") is True
    [call] = bucket.calls
    assert call["name"] == MARKER and call["data"] == b"{}"
    assert call["kwargs"] == {"content_type": "application/json", "if_generation_match": 0, "timeout": 60, "retry": None}


def test_a_marker_name_that_is_taken_is_not_replaced():
    store, bucket = make()
    store.put_if_absent(MARKER, b"first")
    assert store.put_if_absent(MARKER, b"second") is False
    assert bucket.objects == {MARKER: b"first"}


@pytest.mark.parametrize("key", [
    "bronze/merchants/data/event_month=2025-03/x.parquet", "bronze/merchants/metadata/00001-x.metadata.json",
    "_catalog/catalog.db", "_runs/notes.json", "_runs/20261029T153000123456Z.json.bak", "_runs/20261029T1530001234Z.json",
    "_runs/20261029t153000123456z.json", "_runs/x/20261029T153000123456Z.json", "x/_runs/20261029T153000123456Z.json",
    "_runs/20261029T153000123456Z.json\n", "_runs/", "", "cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_00_00.ndjson.gz",
])
def test_a_create_that_is_not_a_run_marker_is_refused_before_it_reaches_the_bucket(key):
    store, bucket = make()
    with pytest.raises(ConfigError) as refused:
        store.put_if_absent(key, b"x")
    assert "run markers" in str(refused.value) and bucket.calls == [] and bucket.objects == {}


def test_the_catalog_copy_is_replaced_with_no_precondition_and_the_sqlite_type():
    store, bucket = make()
    assert store.overwrite(CATALOG_KEY, b"one") is None
    store.overwrite(CATALOG_KEY, b"two")
    assert bucket.objects == {CATALOG_KEY: b"two"}
    assert all(c["kwargs"] == {"content_type": "application/vnd.sqlite3", "timeout": 60, "retry": None}
               for c in bucket.calls)


@pytest.mark.parametrize("key", [MARKER, "_catalog/catalog.db2", "_catalog/", "_catalog/other.db", "catalog.db",
                                 "_catalog/catalog.db/", "bronze/merchants/metadata/00001-x.metadata.json", ""])
def test_an_overwrite_of_anything_but_the_catalog_copy_is_refused_before_it_reaches_the_bucket(key):
    store, bucket = make()
    with pytest.raises(ConfigError) as refused:
        store.overwrite(key, b"x")
    assert "catalog copy" in str(refused.value) and bucket.calls == [] and bucket.objects == {}


def test_a_listing_and_a_read_pass_through_one_listing_and_one_read():
    store, bucket = make()
    for key in ("_runs/20261029T153000123456Z.json", "_runs/20261028T153000123456Z.json", CATALOG_KEY):
        bucket.objects[key] = key.encode()
    assert store.list_keys(RUNS_PREFIX) == ["_runs/20261028T153000123456Z.json", "_runs/20261029T153000123456Z.json"]
    assert store.read(CATALOG_KEY) == CATALOG_KEY.encode()
    assert [c["op"] for c in bucket.calls] == ["list", "download"]
    assert bucket.calls[0]["kwargs"]["prefix"] == "_runs/"


def test_the_store_offers_six_calls_and_holds_no_other_reach_into_the_bucket():
    store, _ = make()
    assert {name for name in dir(store) if not name.startswith("_")} == {
        "list_keys", "read", "put_if_absent", "overwrite", "list_objects", "delete"}
    assert not hasattr(store, "__dict__")


# ---------------------------------------------------------------------------
# G4: the listing with sizes and the guarded delete (python -m bronze.orphans)
# ---------------------------------------------------------------------------

TABLE_FILES = ["bronze/merchants/data/event_month=2025-03/00000-0-x.parquet", "bronze/merchants/data/x.parquet",
               "bronze/merchants/data/a b.parquet", "bronze/merchants/data/~x!#$%&'()+,;=@[]{}.parquet",
               "bronze/merchant_transactions/metadata/00001-x.metadata.json", "bronze/loan_applications/metadata/snap-1-2.avro",
               "bronze/loan_repayments/data/a/b/c/d.parquet", "bronze/risk_events/metadata/x-m0.avro"]


@pytest.mark.parametrize("key", TABLE_FILES)
def test_a_file_of_a_table_may_be_deleted_and_the_delete_reaches_the_bucket(key):
    store, bucket = make()
    bucket.objects[key] = b"x"
    bucket.objects["bronze/merchants/data/keep.parquet"] = b"y"
    assert is_table_file(key) is True and store.delete(key) is True
    assert key not in bucket.objects and "bronze/merchants/data/keep.parquet" in bucket.objects
    [call] = bucket.calls
    assert call["op"] == "delete" and call["name"] == key and call["kwargs"] == {"timeout": 60, "retry": None}


@pytest.mark.parametrize("key", [
    MARKER, "_runs/", CATALOG_KEY, "_catalog/", "", "/", "bronze/", "bronze/merchants/", "bronze/merchants/data", "bronze/merchants/data/",
    "bronze/merchants/metadata/", "bronze/merchants/notes.txt", "bronze/merchants/other/x.parquet", "bronze/other_table/data/x.parquet",
    "bronze/Merchants/data/x.parquet", "Bronze/merchants/data/x.parquet", "bronze//merchants/data/x.parquet",
    "bronze/merchants//data/x.parquet", "bronze/merchants/data//x.parquet", "bronze/merchants/data/../../_runs/" + RUN_ID + ".json",
    "bronze/merchants/data/../metadata/x.json", "bronze/merchants/data/./x.parquet", "bronze/merchants/data/..", "bronze/merchants/..",
    "/bronze/merchants/data/x.parquet", " bronze/merchants/data/x.parquet", "bronze/merchants/data/x.parquet ",
    "bronze/merchants/data/x.parquet\n", "bronze/merchants/data/x\x00.parquet", "bronze/merchants/data/x\t.parquet",
    "bronze/merchants/data/x\x1f.parquet", "bronze/merchants/data/x\x7f.parquet", "bronze/merchants/data/x\r.parquet", "bronze\\merchants\\data\\x.parquet",
    "bronze/merchants/data/x\\..\\y", "cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_00_00.ndjson.gz", "x/bronze/merchants/data/y",
    "bronze/merchants", "bronze/merchants/data/x/",
])
def test_a_delete_of_anything_but_a_table_file_is_refused_before_it_reaches_the_bucket(key):
    store, bucket = make()
    bucket.objects.update({MARKER: b"m", CATALOG_KEY: b"c", "bronze/merchants/data/x.parquet": b"d"})
    with pytest.raises(ConfigError) as refused:
        store.delete(key)
    assert "bronze/<table>/data/" in str(refused.value) and bucket.calls == [] and len(bucket.objects) == 3


def test_every_one_of_the_five_tables_is_deletable_and_no_other_name_is():
    from cdc.columns import EXPECTED_COLUMNS
    assert sorted(EXPECTED_COLUMNS) == ["loan_applications", "loan_repayments", "merchant_transactions", "merchants", "risk_events"]
    assert all(is_table_file(f"bronze/{t}/{folder}/x") for t in EXPECTED_COLUMNS for folder in ("data", "metadata"))
    assert not any(is_table_file(f"bronze/{t}/{folder}/x") for t in ("merchant", "risk_event", "") for folder in ("data", "metadata"))
    assert not any(is_table_file(f"bronze/merchants/{folder}/x") for folder in ("Data", "metadata2", "_data", "", "logs"))


def test_deleting_a_file_that_is_not_there_is_false_and_the_other_files_stay():
    store, bucket = make()
    bucket.objects["bronze/merchants/data/keep.parquet"] = b"y"
    assert store.delete("bronze/merchants/data/gone.parquet") is False and "bronze/merchants/data/keep.parquet" in bucket.objects


def test_list_objects_lists_only_under_the_prefix_it_was_given():
    store, bucket = make()
    bucket.objects.update({"bronze/merchants/data/b": b"12", "_runs/x": b"1", CATALOG_KEY: b"123"})
    assert [i.key for i in store.list_objects("bronze/")] == ["bronze/merchants/data/b"]
    assert [i.key for i in store.list_objects("_runs/")] == ["_runs/x"] and bucket.calls[-1]["kwargs"]["prefix"] == "_runs/"


def test_list_objects_passes_through_one_listing_with_sizes_and_times():
    store, bucket = make()
    bucket.objects.update({"bronze/merchants/data/b": b"12", "_runs/x": b"1", CATALOG_KEY: b"123"})
    found = store.list_objects("")
    assert [(i.key, i.size) for i in found] == [("_catalog/catalog.db", 3), ("_runs/x", 1), ("bronze/merchants/data/b", 2)]
    assert [c["op"] for c in bucket.calls] == ["list"] and bucket.calls[0]["kwargs"]["prefix"] == ""


def test_the_marker_pattern_is_a_fixed_width_timestamp_so_name_order_is_time_order():
    assert MARKER_KEY.fullmatch(MARKER)
    names = [f"_runs/{stamp}.json" for stamp in ("20260101T000000000000Z", "20260101T000000000001Z",
                                                 "20260101T000001000000Z", "20261231T235959999999Z")]
    assert names == sorted(names) and all(MARKER_KEY.fullmatch(n) for n in names)


# ---------------------------------------------------------------------------
# opening the real store
# ---------------------------------------------------------------------------

def test_opening_the_store_without_the_bronze_credentials_is_refused_before_any_client_is_made(monkeypatch):
    clear(monkeypatch)
    made = []
    with pytest.raises(ConfigError) as refused:
        open_store(BronzeSettings(_env_file=None), environ={}, client_factory=lambda: made.append(1))
    assert CREDENTIALS_VARIABLE in str(refused.value) and made == []


def test_the_store_connects_to_the_bronze_bucket_not_the_landing_bucket(monkeypatch, tmp_path):
    clear(monkeypatch)
    creds = tmp_path / "creds.json"
    creds.write_text("{}")
    client = FakeStorageClient()
    store = open_store(BronzeSettings(_env_file=None), environ={CREDENTIALS_VARIABLE: str(creds)},
                       client_factory=lambda: client)
    assert isinstance(store, BronzeStore) and client.requested == ["fintech_bronze"]
    monkeypatch.setenv("BRONZE_BUCKET", "elsewhere")
    client = FakeStorageClient()
    open_store(BronzeSettings(_env_file=None), environ={CREDENTIALS_VARIABLE: str(creds)}, client_factory=lambda: client)
    assert client.requested == ["elsewhere"]


def test_the_settings_given_are_the_settings_used(monkeypatch, tmp_path):
    clear(monkeypatch)
    creds = tmp_path / "creds.json"
    creds.write_text("{}")
    client = FakeStorageClient()
    open_store(BronzeSettings(_env_file=None, bucket="given_bucket"), environ={CREDENTIALS_VARIABLE: str(creds)},
               client_factory=lambda: client)
    assert client.requested == ["given_bucket"]


def test_the_client_is_made_for_the_bronze_project(monkeypatch, tmp_path):
    import cdc.gcs
    clear(monkeypatch)
    calls = []

    def make_client(*args, **kwargs):
        calls.append((args, kwargs))
        return FakeStorageClient()

    monkeypatch.setattr(cdc.gcs.storage, "Client", make_client)
    creds = tmp_path / "creds.json"
    creds.write_text("{}")
    open_store(BronzeSettings(_env_file=None), environ={CREDENTIALS_VARIABLE: str(creds)})
    assert calls == [((), {"project": "payments-512910"})]


def test_the_real_client_can_be_made_from_an_impersonated_credentials_file_with_no_project_anywhere(monkeypatch, tmp_path):
    # The regression test on the real client path (G1's lesson: a fake hides the first real call). No network:
    # nothing is sent until the first list, read or write.
    clear(monkeypatch)
    for variable in ("GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT"):
        monkeypatch.delenv(variable, raising=False)
    creds = tmp_path / "application_default_credentials.json"
    creds.write_text(IMPERSONATED_FILE)
    monkeypatch.setenv(CREDENTIALS_VARIABLE, str(creds))
    store = open_store(BronzeSettings(_env_file=None))
    assert store._sink._bucket.client.project == "payments-512910" and store._sink.bucket_name == "fintech_bronze"


def test_the_two_names_are_the_ones_the_contract_gives():
    assert RUNS_PREFIX == "_runs/" and CATALOG_KEY == "_catalog/catalog.db"
