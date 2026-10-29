"""tests/test_bronze_replay.py: bronze/replay.py, the half that needs no database (landed events against Bronze rows).

The half that replays into a blank database and compares with the source is tested in tests/test_cdc_roundtrip.py,
on the M5 run, because it needs a source database that the events really came from.
"""
import copy
import gzip
import json

import pyarrow as pa
import pytest
from pyiceberg.exceptions import NoSuchTableError

import bronze.replay as replay
from bronze.replay import Digest, check, event_hash, landed_digests
from tests.bronze_env import Env, busy_landing, landed
from tests.bronze_synth import PREFIX, SOURCE_DB, Landing
from tests.fakes import MemorySink
from bronze.landing import LandingReader


def written(tmp_path, land=None):
    land = land or busy_landing()
    env = Env(tmp_path)
    env.run(land.reader())
    return env, land


def reread(data, change):
    events = [json.loads(line) for line in gzip.decompress(data).decode().splitlines()]
    change(events)
    return gzip.compress(b"".join(json.dumps(e, separators=(",", ":")).encode() + b"\n" for e in events), mtime=0)


def with_object_changed(land, picker, change):
    """A reader over a copy of the landing in which one object's events were changed."""
    sink = MemorySink()
    sink.objects = dict(land.sink.objects)
    name = next(n for n in sorted(sink.objects) if picker(n))
    sink.objects[name] = reread(sink.objects[name], change)
    return LandingReader(sink, PREFIX)


def without_object(land, picker):
    sink = MemorySink()
    sink.objects = {n: d for n, d in land.sink.objects.items() if not picker(n)}
    assert len(sink.objects) < len(land.sink.objects)
    return LandingReader(sink, PREFIX)


# ---------------------------------------------------------------------------
# the hash and the digest
# ---------------------------------------------------------------------------

EVENT = {"op": "c", "ts_ms": 5, "ingested_at": "2026-10-28T10:00:00.000000Z", "flags": [], "before": None,
         "after": {"merchant_id": "m1", "risk_score": 7}, "source": {"table": "merchants", "commit_lsn": 9}}


def test_the_hash_ignores_the_consumer_clock_and_the_order_of_keys():
    other = dict(reversed(list(EVENT.items())))
    other["ts_ms"], other["ingested_at"] = 99, "2027-01-01T00:00:00.000000Z"
    assert event_hash(other) == event_hash(EVENT)


@pytest.mark.parametrize("change", [
    lambda e: e["after"].update(merchant_id="m2"),
    lambda e: e["after"].update(risk_score=8),
    lambda e: e.update(op="u"),
    lambda e: e.update(flags=["noop_update"]),
    lambda e: e.update(before={"merchant_id": "m1"}),
    lambda e: e["source"].update(commit_lsn=10),
    lambda e: e.update(extra=1),
])
def test_the_hash_sees_every_other_difference(change):
    other = copy.deepcopy(EVENT)
    change(other)
    assert event_hash(other) != event_hash(EVENT)


def test_the_hash_is_sixty_bits_and_stable():
    # a fixed value: the digest algorithm is part of what a real run's figures mean, so it must not drift unseen
    assert event_hash(EVENT) == 305283164582125950 and 0 <= event_hash(EVENT) < 2 ** 60
    assert event_hash(EVENT) == event_hash(copy.deepcopy(EVENT))


def test_a_digest_does_not_depend_on_the_order_events_are_added():
    a, b = EVENT, {**EVENT, "after": {"merchant_id": "m2", "risk_score": 7}}
    assert Digest().add(a).add(b) == Digest().add(b).add(a)
    assert Digest().add(a).add(b).rows == 2 and Digest().add(a).add(b) != Digest().add(a).add(a)
    assert Digest().add(a) != Digest().add(b)


# ---------------------------------------------------------------------------
# the landed side
# ---------------------------------------------------------------------------

def test_the_landed_digest_counts_every_change_once_per_key_and_equals_the_oracle():
    land = busy_landing()
    digests = landed_digests(land.reader())
    oracle = landed(land)
    for table in digests:
        assert digests[table].rows == len(oracle[table])
        assert digests[table].total == sum(event_hash(e) for e in oracle[table])
    assert digests["loan_applications"].rows == 2 and digests["merchants"].rows == 6


def test_a_replay_that_landed_again_changes_nothing_about_the_landed_digest():
    land = busy_landing()
    before = landed_digests(land.reader())
    land.flush(land.txs, clock=999)                    # the same changes under the same names with another clock: all taken
    assert landed_digests(land.reader()) == before
    land.flush(land.txs[:2] + land.txs[3:], clock=5)   # a replay of most of them under new overlapping names
    assert landed_digests(land.reader()) == before


def test_the_landed_digest_reports_progress_every_hundred_objects():
    land = Landing()
    for _ in range(120):
        land.transaction(("merchants", "c"))
        land.flush()
    said = []
    landed_digests(land.reader(), said.append)
    assert said == ["landed side: 100 of 120 objects"]


# ---------------------------------------------------------------------------
# Bronze against the landing
# ---------------------------------------------------------------------------

def test_bronze_written_from_a_landing_holds_exactly_what_landed(tmp_path):
    env, land = written(tmp_path)
    report = check(env.catalog, land.reader(), None, source_db=SOURCE_DB)
    assert report.ok, report.problems
    assert report.landed == report.bronze and report.landed["merchant_transactions"].rows == 12
    assert report.updates == 1 and report.source == {} and report.replayed == {}
    assert sum(d.rows for d in report.bronze.values()) == sum(len(v) for v in landed(land).values())


def test_a_changed_value_in_one_landed_object_is_found(tmp_path):
    env, land = written(tmp_path)
    reader = with_object_changed(land, lambda n: "/merchants/" in n,
                                 lambda events: events[0]["after"].update(merchant_id="somebody else"))
    problems = check(env.catalog, reader, None, source_db=SOURCE_DB).problems
    assert problems == ["merchants: Bronze holds the same number of changes as landed (6) but not the same events"]


def test_a_changed_value_in_an_update_is_found_in_its_table(tmp_path):
    env, land = written(tmp_path)
    has_update = lambda name: any(e["op"] == "u" for e in (json.loads(line) for line in
                                                           gzip.decompress(land.sink.objects[name]).decode().splitlines()))
    reader = with_object_changed(
        land, lambda n: "/loan_applications/" in n and has_update(n),
        lambda events: [e["before"].update(decision="changed") for e in events if e["op"] == "u"])
    problems = check(env.catalog, reader, None, source_db=SOURCE_DB).problems
    assert problems == ["loan_applications: Bronze holds the same number of changes as landed (2) but not the same events"]


def test_an_object_that_landed_but_is_not_in_bronze_is_found(tmp_path):
    env, land = written(tmp_path)
    late = Landing()                                   # a landing with one more transaction than Bronze was written from
    late.sink.objects = dict(land.sink.objects)
    late._next_lsn, late._serial = land._next_lsn, land._serial
    late.transaction(("risk_events", "c"), date="2025-04-02")
    late.flush()
    problems = check(env.catalog, late.reader(), None, source_db=SOURCE_DB).problems
    assert problems == ["risk_events: 7 changes landed, Bronze holds 6"]


def test_a_landed_change_missing_from_bronze_the_other_way_round_is_found(tmp_path):
    env, land = written(tmp_path)
    reader = without_object(land, lambda n: "/risk_events/event_date=2025-04-02/" in n)
    problems = check(env.catalog, reader, None, source_db=SOURCE_DB).problems
    assert problems == ["risk_events: 4 changes landed, Bronze holds 6"]


def test_a_key_held_twice_in_bronze_is_counted(tmp_path):
    env, land = written(tmp_path)
    table = env.table("risk_events")
    table.append(table.scan(limit=1).to_arrow())
    problems = check(env.catalog, land.reader(), None, source_db=SOURCE_DB).problems
    assert "risk_events: Bronze holds 1 keys more than once" in problems
    assert "risk_events: 6 changes landed, Bronze holds 7" in problems
    assert len(problems) == 2


def test_a_missing_table_is_an_error_not_a_pass(tmp_path):
    env = Env(tmp_path)
    with pytest.raises(NoSuchTableError):
        check(env.catalog, busy_landing().reader(), None, source_db=SOURCE_DB)


def test_events_read_back_under_another_source_database_do_not_equal_the_landed_ones(tmp_path):
    env, land = written(tmp_path)
    problems = check(env.catalog, land.reader(), None, source_db="another_database").problems
    assert len(problems) == 5 and all("but not the same events" in p for p in problems)


def test_reading_in_slices_gives_the_same_result_as_reading_whole(tmp_path, monkeypatch):
    env, land = written(tmp_path)
    whole = check(env.catalog, land.reader(), None, source_db=SOURCE_DB)
    monkeypatch.setattr(replay, "SLICE", 1)
    sliced = check(env.catalog, land.reader(), None, source_db=SOURCE_DB)
    assert sliced.ok and sliced.bronze == whole.bronze


def test_bronze_progress_is_reported_per_table(tmp_path):
    env, land = written(tmp_path)
    said = []
    check(env.catalog, land.reader(), None, source_db=SOURCE_DB, say=said.append)
    assert [m for m in said if m.startswith("Bronze side")] == [
        "Bronze side: merchants 6 rows", "Bronze side: merchant_transactions 12 rows", "Bronze side: risk_events 6 rows",
        "Bronze side: loan_applications 2 rows", "Bronze side: loan_repayments 1 rows"]


def test_the_check_reads_bronze_and_writes_nothing(tmp_path):
    env, land = written(tmp_path)
    files, snapshots, catalog = env.files(), env.snapshots(), env.catalog_digest()
    check(env.catalog, land.reader(), None, source_db=SOURCE_DB)
    assert (env.files(), env.snapshots(), env.catalog_digest()) == (files, snapshots, catalog)


def test_the_check_passes_its_progress_callback_to_the_landed_side_too(tmp_path):
    land = Landing()
    for _ in range(100):
        land.transaction(("merchants", "c"))
        land.flush()
    env = Env(tmp_path)
    env.run(land.reader())
    said = []
    check(env.catalog, land.reader(), None, source_db=SOURCE_DB, say=said.append)
    assert "landed side: 100 of 100 objects" in said
