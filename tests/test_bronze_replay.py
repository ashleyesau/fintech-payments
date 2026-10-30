"""tests/test_bronze_replay.py: bronze/replay.py, the half that needs no database (landed events against Bronze rows).

The half that replays into a blank database and compares with the source is tested in tests/test_cdc_roundtrip.py,
on the M5 run, because it needs a source database that the events really came from.
"""
import copy
import gzip
import json
import time

import pyarrow as pa
import pytest
from pyiceberg.exceptions import NoSuchTableError

import bronze.replay as replay
from bronze.replay import Digest, check, event_hash, landed_digests
from tests.bronze_env import Concurrency, Env, Spy, busy_landing, landed, spread_landing
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


# ---------------------------------------------------------------------------
# G4 step 2: the read-ahead on the landed side
# ---------------------------------------------------------------------------

def test_the_landed_side_reads_several_objects_at_a_time_and_gives_the_same_digest(tmp_path):
    land = spread_landing(10)
    sequential = landed_digests(land.reader(), workers=1)
    spy = Concurrency(land.sink)
    parallel = landed_digests(spy.reader(), workers=4)
    assert parallel == sequential and parallel["merchant_transactions"].rows == 20
    assert len(spy.reads) == 10 and sorted(spy.reads) == sorted(set(spy.reads))      # every object once
    assert max(spy.levels) > 1 and max(spy.levels) <= 4                              # in parallel, never over the workers


def test_one_worker_reads_one_object_at_a_time():
    land = spread_landing(6)
    spy = Concurrency(land.sink)
    landed_digests(spy.reader(), workers=1)
    assert max(spy.levels) == 1


def test_objects_are_folded_in_listing_order_whatever_order_the_reads_finish_in(monkeypatch):
    land = spread_landing(8)
    listing = [o.key for o in land.reader().list_keys()]
    delays = {key: (0.12 if number < 3 else 0.0) for number, key in enumerate(listing)}   # the first three finish last

    def slow_first(key, n):
        time.sleep(delays[key])

    spy = Spy(land.sink, slow_first)
    folded = []
    real = replay.events_in
    monkeypatch.setattr(replay, "events_in", lambda data: (folded.append(data), real(data))[1])
    landed_digests(spy.reader(), workers=8)
    assert folded == [land.sink.objects[key] for key in listing]


def test_the_check_passes_its_workers_to_the_landed_side(tmp_path):
    env, land = written(tmp_path)
    spy = Concurrency(land.sink)
    assert check(env.catalog, spy.reader(), None, source_db=SOURCE_DB, workers=3).ok
    assert 1 < max(spy.levels) <= 3
    spy = Concurrency(land.sink)
    assert check(env.catalog, spy.reader(), None, source_db=SOURCE_DB, workers=1).ok
    assert max(spy.levels) == 1


def test_landed_digests_reports_how_many_objects_it_read():
    land = spread_landing(5)
    stats = {}
    landed_digests(land.reader(), stats=stats)
    assert stats == {"objects": 5}


# ---------------------------------------------------------------------------
# G4 step 2: what the report says about the version of Bronze read
# ---------------------------------------------------------------------------

def test_the_report_names_the_snapshot_each_table_was_read_at_and_the_objects_read(tmp_path):
    env, land = written(tmp_path)
    report = check(env.catalog, land.reader(), None, source_db=SOURCE_DB)
    snapshots = {name: ids[-1] for name, ids in env.snapshots().items()}
    assert report.snapshots == snapshots and all(isinstance(s, int) for s in snapshots.values())
    assert report.objects == len(land.reader().list_keys())
    assert report.disbursed is None and report.moved == [] and report.source_before == {}


def test_the_snapshot_is_the_one_current_when_the_table_is_read_not_an_older_one(tmp_path):
    env, land = written(tmp_path)
    old = {name: ids[-1] for name, ids in env.snapshots().items()}
    table = env.table("risk_events")
    table.append(table.scan(limit=1).to_arrow())                    # a later commit
    report = check(env.catalog, land.reader(), None, source_db=SOURCE_DB)
    assert report.snapshots["risk_events"] == env.snapshots()["risk_events"][-1] != old["risk_events"]
    assert report.snapshots["merchants"] == old["merchants"]


def test_a_table_with_no_snapshot_is_reported_as_none(tmp_path):
    env = Env(tmp_path)
    env.run(Landing().reader())                                    # an empty landing: the tables exist, with no snapshot
    land = Landing()
    report = check(env.catalog, land.reader(), None, source_db=SOURCE_DB)
    assert report.snapshots == {name: None for name in report.snapshots} and len(report.snapshots) == 5


# ---------------------------------------------------------------------------
# G4 step 2: the source that moves while the check runs (stand-in server; the real database is tested in
# tests/test_cdc_roundtrip.py and tests/test_bronze_verify.py)
# ---------------------------------------------------------------------------

TABLES_REPLAYED = ("merchants", "merchant_transactions", "risk_events", "loan_applications", "loan_repayments")


class FakeServer:
    """Stands in for the source's Postgres server: table_digest answers from a script, one answer per call, the replay
    database is a name, and no statement does anything. `events` records the order things happen in."""

    def __init__(self, monkeypatch, source, replayed, disbursed=()):
        self.source = {table: list(answers) for table, answers in source.items()}      # table -> [before, after]
        self.replayed, self.disbursed, self.events = replayed, list(disbursed), []
        from contextlib import contextmanager

        @contextmanager
        def blank(dsn):
            self.events.append("replay database created")
            yield "replay-dsn"
            self.events.append("replay database dropped")

        class Conn:
            def __enter__(inner):
                return inner

            def __exit__(inner, *exc):
                return False

            def transaction(inner):
                return inner

            def execute(inner, sql, *args):
                return [(application_id,) for application_id in self.disbursed]

        class Psycopg:
            Error = replay.psycopg.Error
            connect = staticmethod(lambda *args, **kwargs: Conn())

        monkeypatch.setattr(replay, "blank_database", blank)
        monkeypatch.setattr(replay, "psycopg", Psycopg)
        monkeypatch.setattr(replay, "insert_rows", lambda conn, table, rows: None)
        monkeypatch.setattr(replay, "update_row", lambda conn, event: None)
        monkeypatch.setattr(replay, "table_digest", self.digest)
        monkeypatch.setattr(replay, "explain_difference", lambda source, replay_dsn, table: f"EXPLAINED {table}")

    def digest(self, dsn, table):
        if dsn == "replay-dsn":
            return self.replayed[table]
        self.events.append(f"source digest {table}")
        return self.source[table].pop(0)


def updated_loans(env):
    """The loans Bronze holds an update for, read straight from the table."""
    return [row["application_id"] for row in env.rows("loan_applications") if row["op"] == "u"]


def same(rows=3, digest=111):
    return {table: (rows, digest) for table in TABLES_REPLAYED}


def stable_source(**moved):
    """Source answers that agree with the replay: [before, after] per table, with `table=(after_rows, after_digest)` moved."""
    return {table: [(3, 111), moved.get(table, (3, 111))] for table in TABLES_REPLAYED}


def test_a_source_that_held_still_is_compared_and_the_check_passes(tmp_path, monkeypatch):
    env, land = written(tmp_path)
    FakeServer(monkeypatch, stable_source(), same(), disbursed=updated_loans(env))
    report = check(env.catalog, land.reader(), "source-dsn", source_db=SOURCE_DB)
    assert report.problems == [] and report.moved == [] and report.ok is True and report.disbursed == 1
    assert report.snapshots == {name: ids[-1] for name, ids in env.snapshots().items()}
    assert report.objects == len(land.reader().list_keys())
    assert report.source == report.source_before == same() and report.replayed == same()


def test_a_table_that_changed_while_the_check_ran_is_moved_not_a_mismatch(tmp_path, monkeypatch):
    env, land = written(tmp_path)
    FakeServer(monkeypatch, stable_source(merchants=(4, 222)), {**same(), "merchants": (3, 111)}, disbursed=updated_loans(env))
    said = []
    report = check(env.catalog, land.reader(), "source-dsn", source_db=SOURCE_DB, say=said.append)
    assert report.moved == ["merchants"] and report.ok is False
    assert [p for p in report.problems if "merchants" in p and "differs from the source" in p] == []   # not compared
    assert report.source_before["merchants"] == (3, 111) and report.source["merchants"] == (4, 222)
    assert "merchants: the source changed during the check (3 rows before, 4 after); not compared" in said


@pytest.mark.parametrize("after", [(4, 111), (3, 222)], ids=["rows only", "digest only"])
def test_a_move_that_changes_only_the_row_count_or_only_the_digest_is_still_a_move(tmp_path, monkeypatch, after):
    env, land = written(tmp_path)
    FakeServer(monkeypatch, stable_source(merchants=after), same(), disbursed=updated_loans(env))
    assert check(env.catalog, land.reader(), "source-dsn", source_db=SOURCE_DB).moved == ["merchants"]


def test_only_the_tables_that_moved_are_skipped_the_others_are_still_compared(tmp_path, monkeypatch):
    env, land = written(tmp_path)
    replayed = {**same(), "risk_events": (3, 999)}                                # a real mismatch in a table that held still
    FakeServer(monkeypatch, stable_source(merchants=(4, 222)), replayed, disbursed=updated_loans(env))
    report = check(env.catalog, land.reader(), "source-dsn", source_db=SOURCE_DB)
    assert report.moved == ["merchants"]
    assert report.problems == ["risk_events: the table replayed from Bronze differs from the source (rows 3 against 3): "
                               "EXPLAINED risk_events"]


def test_a_moved_source_is_never_called_a_bronze_mismatch_even_when_the_digests_differ(tmp_path, monkeypatch):
    env, land = written(tmp_path)
    FakeServer(monkeypatch, stable_source(merchants=(4, 222)), {**same(), "merchants": (3, 111)})
    report = check(env.catalog, land.reader(), "source-dsn", source_db=SOURCE_DB)
    assert not any("differs from the source" in p for p in report.problems)
    assert report.moved == ["merchants"]


def test_a_source_that_changed_and_changed_back_is_not_moved(tmp_path, monkeypatch):
    env, land = written(tmp_path)
    FakeServer(monkeypatch, {t: [(3, 111), (3, 111)] for t in TABLES_REPLAYED}, same())
    assert check(env.catalog, land.reader(), "source-dsn", source_db=SOURCE_DB).moved == []


def test_every_table_is_checked_for_a_move_not_just_the_first(tmp_path, monkeypatch):
    env, land = written(tmp_path)
    after = {table: (3, 222) for table in TABLES_REPLAYED}
    FakeServer(monkeypatch, stable_source(**after), same())
    assert check(env.catalog, land.reader(), "source-dsn", source_db=SOURCE_DB).moved == list(TABLES_REPLAYED)


def test_the_source_is_read_before_the_landing_and_again_after_the_replay(tmp_path, monkeypatch):
    env, land = written(tmp_path)
    server = FakeServer(monkeypatch, stable_source(), same())

    def note(key, n):
        server.events.append("landing read")

    check(env.catalog, Spy(land.sink, note).reader(), "source-dsn", source_db=SOURCE_DB)
    kinds = [e if not e.startswith("source digest") else "source digest" for e in server.events]
    first_read = kinds.index("landing read")
    assert kinds[:first_read] == ["source digest"] * 5                             # all five before any landing read
    last_read = len(kinds) - 1 - kinds[::-1].index("landing read")
    assert kinds[last_read + 1:] == ["replay database created"] + ["source digest"] * 5 + ["replay database dropped"]


def test_the_loans_the_source_shows_as_disbursed_are_counted_and_compared(tmp_path, monkeypatch):
    env, land = written(tmp_path)
    FakeServer(monkeypatch, stable_source(), same(), disbursed=["app-1", "app-2", "app-3"])
    report = check(env.catalog, land.reader(), "source-dsn", source_db=SOURCE_DB)
    assert report.disbursed == 3 and report.updates == 1
    assert report.problems == ["Bronze holds updates for 1 loans, the source shows 3 disbursed (4 differ)"]


def test_the_disbursed_comparison_is_skipped_when_loan_applications_moved(tmp_path, monkeypatch):
    env, land = written(tmp_path)
    FakeServer(monkeypatch, stable_source(loan_applications=(4, 222)), same(), disbursed=["app-1", "app-2", "app-3"])
    report = check(env.catalog, land.reader(), "source-dsn", source_db=SOURCE_DB)
    assert report.moved == ["loan_applications"] and report.disbursed == 3
    assert not any(p.startswith("Bronze holds updates") for p in report.problems)


def test_a_table_that_moved_elsewhere_does_not_hide_a_wrong_update_count(tmp_path, monkeypatch):
    env, land = written(tmp_path)
    FakeServer(monkeypatch, stable_source(merchants=(4, 222)), same(), disbursed=["app-1", "app-2", "app-3"])
    report = check(env.catalog, land.reader(), "source-dsn", source_db=SOURCE_DB)
    assert report.moved == ["merchants"]
    assert report.problems == ["Bronze holds updates for 1 loans, the source shows 3 disbursed (4 differ)"]


def test_the_replay_database_is_dropped_even_when_a_table_moved(tmp_path, monkeypatch):
    env, land = written(tmp_path)
    server = FakeServer(monkeypatch, stable_source(merchants=(4, 222)), same())
    check(env.catalog, land.reader(), "source-dsn", source_db=SOURCE_DB)
    assert server.events.count("replay database created") == server.events.count("replay database dropped") == 1


def test_the_report_without_a_source_has_nothing_to_move(tmp_path):
    env, land = written(tmp_path)
    report = check(env.catalog, land.reader(), None, source_db=SOURCE_DB)
    assert report.source_before == {} and report.moved == [] and report.ok
