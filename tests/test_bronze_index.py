"""tests/test_bronze_index.py: G1 step 3, pass 1: the streaming index and what stops it.

Synthetic landings (tests/bronze_synth.py) over the in-memory sink. The cases the real run cannot
show (it has 0 duplicates, no overlap, no disagreement, no incomplete transaction) are made here:
duplicates in other objects, overlapping objects, copies that disagree, a missing object, objects
read out of order. Nothing here touches a bucket.
"""
import threading
from copy import deepcopy

import pytest

from bronze.config import BronzeSettings
from bronze.errors import ConfigError, ContractError, DisagreementError, EventError, IncompleteTransactions
from bronze.index import REPORTED, Index, build_index
from cdc.errors import SinkError
from tests.bronze_synth import DAY, SOURCE_DB, Landing, encode, event, name
from tests.fakes import MemorySink

M, T, A, R, K = "merchants", "merchant_transactions", "loan_applications", "loan_repayments", "risk_events"


def build(land, workers=8):
    return build_index(land.reader(), workers, source_db=SOURCE_DB)


def real_shape():
    """A small run shaped like the real one: a mix of tables, two days, a disbursement update, a
    transaction that spans three tables."""
    land = Landing()
    land.transaction((M, "c"), (M, "c"), (M, "c"))
    land.transaction((T, "c"), (T, "c"), (K, "c"))
    land.transaction((A, "c"), (T, "c"), date="2025-03-02")
    land.transaction((A, "c"))
    land.transaction((T, "c"), (T, "c"), (T, "c"), (R, "c"), date="2025-03-02")
    land.transaction((A, "u"))
    land.flush()
    return land


# ---------------------------------------------------------------------------
# counts
# ---------------------------------------------------------------------------

def test_the_summary_counts_every_event_by_table_and_operation():
    s = build(real_shape()).summary()
    assert s.events_read == s.unique_events == 14 and s.duplicates == 0
    assert s.transactions == 6 and s.incomplete == 0
    assert s.by_table == {M: {"c": 3, "u": 0}, T: {"c": 6, "u": 0}, A: {"c": 2, "u": 1},
                          R: {"c": 1, "u": 0}, K: {"c": 1, "u": 0}}
    assert sum(sum(c.values()) for c in s.by_table.values()) == 14


def test_the_summary_counts_the_objects_and_an_empty_landing_is_empty():
    land = real_shape()
    assert build(land).summary().objects == len(land.sink.objects)
    empty = build(Landing()).summary()
    assert (empty.objects, empty.events_read, empty.unique_events, empty.transactions) == (0, 0, 0, 0)
    assert all(counts == {"c": 0, "u": 0} for counts in empty.by_table.values())


def test_every_table_is_listed_even_with_no_events():
    land = Landing()
    land.transaction((M, "c"))
    land.flush()
    assert set(build(land).summary().by_table) == {M, T, A, R, K}


# ---------------------------------------------------------------------------
# reading
# ---------------------------------------------------------------------------

def test_one_listing_and_one_read_of_every_object():
    land = real_shape()
    build(land)
    assert land.sink.listings == 1 and land.sink.reads == len(land.sink.objects)


def test_a_failed_read_stops_the_build_with_the_sinks_error():
    land = real_shape()
    land.sink.fail_read = lambda key: True
    with pytest.raises(SinkError):
        build(land)


def test_one_worker_and_eight_give_the_same_index():
    land = real_shape()
    one, eight = build(land, 1), build(land, 8)
    assert one.summary() == eight.summary()
    for table in (M, T, A, R, K):
        assert list(one.keys(table)) == list(eight.keys(table))
        assert one.objects_missing(table, set()) == eight.objects_missing(table, set())


def test_a_worker_count_below_one_is_refused():
    with pytest.raises(ConfigError):
        build(real_shape(), 0)


def test_the_read_workers_setting_defaults_to_eight_and_cannot_be_zero(monkeypatch):
    monkeypatch.delenv("BRONZE_READ_WORKERS", raising=False)
    assert BronzeSettings(_env_file=None).read_workers == 8
    monkeypatch.setenv("BRONZE_READ_WORKERS", "0")
    with pytest.raises(Exception):
        BronzeSettings(_env_file=None)


def test_a_bad_event_stops_the_build_and_no_index_is_returned():
    land = real_shape()
    bad = event(M, "c", 0x3000000, 0, 1)
    bad["envelope_version"] = 2
    land.put(M, DAY, [bad])
    with pytest.raises(EventError, match="envelope_version"):
        build(land)


@pytest.mark.parametrize("drift", ["an extra column", "a missing column", "a renamed column", "a wrong version"])
def test_schema_drift_in_any_object_stops_the_pass_before_an_index_exists(drift):
    land = real_shape()
    e = event(T, "c", 0x3000000, 0, 1)
    if drift == "an extra column":
        e["after"]["surprise"] = 1
    elif drift == "a missing column":
        e["after"].pop("auth_code")
    elif drift == "a renamed column":
        e["after"] = {("auth" if k == "auth_code" else k): v for k, v in e["after"].items()}
    else:
        e["envelope_version"] = 2
    land.put(T, DAY, [e])
    with pytest.raises(EventError):
        build(land)


def test_every_stop_is_a_contract_error():
    for error in (EventError, DisagreementError, IncompleteTransactions):
        assert issubclass(error, ContractError)


# ---------------------------------------------------------------------------
# duplicates, overlap and disagreement
# ---------------------------------------------------------------------------

def test_a_replay_under_the_same_names_adds_nothing():
    land = real_shape()
    land.flush(land.txs, clock=5000)
    s = build(land).summary()
    assert s.events_read == s.unique_events == 14 and s.duplicates == 0


def test_a_replay_that_covered_more_lands_overlapping_objects_and_counts_the_copies():
    land = Landing()
    t1, t2 = land.transaction((M, "c")), land.transaction((M, "c"))
    land.flush([t1])
    land.flush([t2])
    t3 = land.transaction((M, "c"))
    land.flush([t1, t2, t3], clock=7000)
    s = build(land).summary()
    assert s.events_read == 5 and s.unique_events == 3 and s.duplicates == 2
    assert s.by_table[M]["c"] == 3 and s.transactions == 3 and s.incomplete == 0


def test_a_change_twice_in_one_object_is_a_duplicate_that_agrees():
    land = Landing()
    tx = land.transaction((M, "c"))
    events = tx.events() + tx.events(clock=900)
    land.put(M, DAY, events)
    s = build(land).summary()
    assert s.events_read == 2 and s.unique_events == 1 and s.duplicates == 1


def disagreeing(mutate):
    """A set whose loan_applications change at position 0 also lands in a second object, edited."""
    land = Landing()
    tx = land.transaction((A, "c"), (M, "c"))
    land.flush()
    other = deepcopy(tx.events(clock=500))[0]
    mutate(other)
    land.put(A, DAY, [other], lsns=(tx.commit_lsn - 1, tx.commit_lsn))
    return land, tx


FIELDS = {
    "op": lambda e: e.__setitem__("op", "u") or e.__setitem__("before", dict(e["after"])),
    "an after value": lambda e: e["after"].__setitem__("merchant_id", "other"),
    "a before value": lambda e: (e.__setitem__("op", "u"), e.__setitem__("before", dict(e["after"])),
                                 e["before"].__setitem__("merchant_id", "other")),
    "change_lsn": lambda e: e["source"].__setitem__("change_lsn", 1),
    "commit_ts": lambda e: e["source"].__setitem__("commit_ts", "2025-03-01T11:00:00.000000Z"),
    "transaction id": lambda e: e["transaction"].__setitem__("id", 5),
    "event_ts": lambda e: e.__setitem__("event_ts", "2025-03-01T09:00:00.000000Z"),
    "flags": lambda e: e.__setitem__("flags", ["noop_update"]),
}


@pytest.mark.parametrize("field", FIELDS)
def test_two_copies_that_differ_in_anything_but_the_clock_stop_the_run(field):
    land, tx = disagreeing(FIELDS[field])
    with pytest.raises(DisagreementError) as caught:
        build(land)
    message = str(caught.value)
    assert f"{tx.commit_lsn:016x} position 0" in message
    assert message.count("loan_applications/event_date=") == 2, message
    assert "ts_ms and ingested_at" in message


def test_copies_that_differ_only_in_the_clock_agree():
    land, tx = disagreeing(lambda e: None)
    assert build(land).summary().duplicates == 1


def test_a_disagreeing_copy_in_the_same_object_stops_the_run():
    land = Landing()
    tx = land.transaction((M, "c"))
    a, b = tx.events(), tx.events()
    b[0]["after"]["merchant_id"] = "other"
    land.put(M, DAY, a + b)
    with pytest.raises(DisagreementError):
        build(land)


def test_a_transaction_whose_events_disagree_about_its_size_is_incomplete():
    land = Landing()
    tx = land.transaction((M, "c"), (M, "c"))
    land.flush()
    odd = event(M, "c", tx.commit_lsn, 0, 3, serial=99)
    land.put(M, DAY, [odd], lsns=(tx.commit_lsn - 1, tx.commit_lsn))
    index = build(land)
    assert list(index.incomplete().values()) == ["events disagree about the transaction size (3 and 2)"]
    with pytest.raises(IncompleteTransactions, match="disagree about the transaction size"):
        index.require_complete()


# ---------------------------------------------------------------------------
# completeness
# ---------------------------------------------------------------------------

def test_a_whole_run_is_complete():
    index = build(real_shape())
    assert index.incomplete() == {}
    index.require_complete()


def test_a_transaction_spread_over_objects_is_complete_once_every_object_is_read():
    land = Landing()
    land.transaction((M, "c"), (T, "c"), (K, "c"))
    land.flush()
    assert len(land.sink.objects) == 3
    assert build(land).incomplete() == {}


@pytest.mark.parametrize("missing", [M, T, K])
def test_a_missing_object_leaves_its_transactions_incomplete_and_names_them(missing):
    land = Landing()
    tx = land.transaction((M, "c"), (T, "c"), (K, "c"))
    land.flush()
    del land.sink.objects[next(k for k in land.sink.objects if f"/{missing}/" in k)]
    index = build(land)
    assert index.incomplete() == {tx.commit_lsn: "1 of 3 positions are missing, first " + str(["merchants", "merchant_transactions", "risk_events"].index(missing))}
    with pytest.raises(IncompleteTransactions, match=f"1 of 1 transactions are incomplete.*{tx.commit_lsn:016x}"):
        index.require_complete()
    assert index.summary().incomplete == 1


def test_a_duplicate_does_not_fill_a_gap():
    land = Landing()
    tx = land.transaction((M, "c"), (M, "c"))
    land.put(M, DAY, [tx.events()[0], tx.events(clock=9)[0]])
    index = build(land)
    assert index.summary().duplicates == 1
    assert list(index.incomplete().values()) == ["1 of 2 positions are missing, first 1"]


def test_the_error_names_at_most_ten_and_counts_the_rest():
    land = Landing()
    for _ in range(REPORTED + 4):
        land.transaction((M, "c"), (T, "c"))
    land.flush()
    for key in [k for k in land.sink.objects if f"/{T}/" in k]:
        del land.sink.objects[key]
    with pytest.raises(IncompleteTransactions) as caught:
        build(land).require_complete()
    message = str(caught.value)
    assert f"{REPORTED + 4} of {REPORTED + 4} transactions are incomplete (and 4 more)" in message
    assert message.count("commit_lsn ") == REPORTED


def test_a_large_gap_is_summarised_not_listed():
    land = Landing()
    tx = land.transaction(*[(M, "c")] * 40)
    events = tx.events()
    land.put(M, DAY, events[:3])
    (reason,) = build(land).incomplete().values()
    assert reason == "37 of 40 positions are missing, first 3, 4, 5, 6, 7"


# ---------------------------------------------------------------------------
# what pass 2 asks
# ---------------------------------------------------------------------------

def test_keys_are_the_unique_keys_of_the_table_in_order():
    land = real_shape()
    index = build(land)
    keys = list(index.keys(T))
    assert len(keys) == 6 == len(set(keys)) and keys == sorted(keys)
    assert sum(len(list(index.keys(t))) for t in (M, T, A, R, K)) == 14


def two_objects_per_table():
    land = Landing()
    t1, t2 = land.transaction((M, "c")), land.transaction((M, "c"))
    t3 = land.transaction((K, "c"))
    land.flush([t1])
    land.flush([t2])
    land.flush([t3])
    return land, t1, t2, t3


def test_only_the_objects_that_hold_a_missing_key_are_asked_for():
    land, t1, t2, t3 = two_objects_per_table()
    index = build(land)
    first, second = sorted(k for k in land.sink.objects if f"/{M}/" in k)
    assert [o.key for o in index.objects_missing(M, set())] == [first, second]
    assert [o.key for o in index.objects_missing(M, {(t1.commit_lsn, 0)})] == [second]
    assert [o.key for o in index.objects_missing(M, {(t2.commit_lsn, 0)})] == [first]
    assert index.objects_missing(M, {(t1.commit_lsn, 0), (t2.commit_lsn, 0)}) == []


def test_another_tables_objects_are_never_asked_for():
    land, t1, t2, t3 = two_objects_per_table()
    index = build(land)
    assert [o.table for o in index.objects_missing(M, set())] == [M, M]
    assert [o.table for o in index.objects_missing(K, set())] == [K]
    assert index.objects_missing(K, {(t3.commit_lsn, 0)}) == []
    assert index.objects_missing(T, set()) == []


def test_an_object_holding_only_copies_is_not_asked_for():
    land, t1, t2, _ = two_objects_per_table()
    land.flush([t1, t2], clock=1)
    index = build(land)
    wide = [k for k in land.sink.objects if f"/{M}/" in k and k.endswith(f"{t1.commit_lsn:016x}_{t2.commit_lsn:016x}.ndjson.gz")]
    assert len(wide) == 1
    # t1 and t2 first appear in the objects that sort first: the narrow t1 object and the wide one.
    asked = [o.key for o in index.objects_missing(M, set())]
    narrow_t2 = next(k for k in land.sink.objects if k.endswith(f"{t2.commit_lsn:016x}_{t2.commit_lsn:016x}.ndjson.gz") and f"/{M}/" in k)
    assert narrow_t2 not in asked and wide[0] in asked and len(asked) == 2
    assert index.object_of((t2.commit_lsn, 0)).key == wide[0]


def test_object_of_says_where_the_first_copy_is_and_refuses_what_it_does_not_have():
    land, t1, t2, t3 = two_objects_per_table()
    land.flush([t1, t2], clock=1)
    index = build(land)
    assert index.object_of((t1.commit_lsn, 0)).key.endswith(f"lsn_{t1.commit_lsn:016x}_{t1.commit_lsn:016x}.ndjson.gz")
    for missing in [(t1.commit_lsn, 5), (1, 0), (t1.commit_lsn, -1)]:
        with pytest.raises(KeyError):
            index.object_of(missing)


def test_the_first_copy_is_the_one_listed_first_even_when_it_is_read_last():
    land = Landing()
    t1, t2 = land.transaction((M, "c")), land.transaction((M, "c"))
    narrow = land.flush([t1])[0]
    land.flush([t1, t2], clock=3)
    wide = next(k for k in land.sink.objects if k != narrow)
    assert narrow < wide

    class Gated(MemorySink):
        gate = threading.Event()

        def read(self, key):
            if key == narrow:
                self.gate.wait(10)
            data = super().read(key)
            if key == wide:
                self.gate.set()
            return data

    gated = Gated()
    gated.objects = land.sink.objects
    from bronze.landing import LandingReader
    index = build_index(LandingReader(gated, land.prefix), workers=2, source_db=SOURCE_DB)
    assert index.object_of((t1.commit_lsn, 0)).key == narrow


def test_object_of_refuses_a_position_that_is_missing_from_an_incomplete_index():
    land = Landing()
    tx = land.transaction((M, "c"), (M, "c"), (M, "c"))
    land.put(M, DAY, [tx.events()[0], tx.events()[2]])
    index = build(land)
    assert index.object_of((tx.commit_lsn, 2)).table == M
    with pytest.raises(KeyError):
        index.object_of((tx.commit_lsn, 1))


def test_the_objects_come_back_in_listing_order_whatever_order_the_numbers_would_iterate_in():
    land = Landing()
    txs = []
    for _ in range(10):
        txs.append(land.transaction((M, "c")))
        land.flush()
    index = build(land)
    present = {(tx.commit_lsn, 0) for n, tx in enumerate(txs) if n not in (1, 8)}
    asked = index.objects_missing(M, present)
    assert [o.key for o in asked] == sorted(o.key for o in asked) and len(asked) == 2
    assert [index.objects.index(o) for o in asked] == [1, 8]


def test_at_most_twice_the_workers_objects_are_in_flight_while_the_oldest_is_slow():
    land = Landing()
    for _ in range(12):
        land.transaction((M, "c"))
        land.flush()
    first = sorted(land.sink.objects)[0]
    started = []
    release = threading.Event()
    seen = threading.Condition()

    class Slow(MemorySink):
        def read(self, key):
            with seen:
                started.append(key)
                seen.notify_all()
            if key == first:
                release.wait(10)
            return super().read(key)

    slow = Slow()
    slow.objects = land.sink.objects
    from bronze.landing import LandingReader
    runner = threading.Thread(target=lambda: build_index(LandingReader(slow, land.prefix), workers=2, source_db=SOURCE_DB))
    runner.start()
    try:
        with seen:
            assert seen.wait_for(lambda: len(started) >= 4, timeout=5), started
        threading.Event().wait(0.3)
        assert len(started) == 4
    finally:
        release.set()
        runner.join(10)
    assert len(started) == 12


@pytest.mark.parametrize("call", ["keys", "objects_missing"])
def test_pass_2_cannot_start_on_an_incomplete_index(call):
    land = Landing()
    land.transaction((M, "c"), (T, "c"))
    land.flush()
    del land.sink.objects[next(k for k in land.sink.objects if f"/{T}/" in k)]
    index = build(land)
    with pytest.raises(IncompleteTransactions):
        if call == "keys":
            list(index.keys(M))
        else:
            index.objects_missing(M, set())


def test_the_index_is_small_per_event():
    # A guard on the layout, not a benchmark: about 12 bytes an event plus the transaction record. The
    # real memory ceiling test runs on a set larger than the real run (step 4).
    land = Landing()
    tx = land.transaction(*[(M, "c")] * 1000)
    land.flush()
    index = build(land)
    (transaction,) = index._transactions.values()
    assert transaction.hashes.itemsize == 8 and transaction.holders.itemsize == 4
    assert len(transaction.hashes) == len(transaction.holders) == 1000
