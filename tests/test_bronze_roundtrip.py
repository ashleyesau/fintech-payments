"""tests/test_bronze_roundtrip.py: P5, the landed events through the pass 2 conversion and back (bronze/replay.py
roundtrip and python -m bronze.check --roundtrip). Synthetic landings over the in-memory sink: nothing here touches GCS."""
import copy
import io
from types import SimpleNamespace

import pytest

import bronze.check as check
import bronze.replay as replay
from bronze.config import CREDENTIALS_VARIABLE, BronzeSettings
from bronze.errors import ContractError, MappingError, RoundTripError
from bronze.replay import Digest, first_difference, payload_hash, roundtrip
from cdc.dedup import events_in
from tests.bronze_env import Concurrency, Spy, busy_landing, landed, spread_landing
from tests.bronze_synth import SOURCE_DB, Landing, event

M, T, A, K = "merchants", "merchant_transactions", "loan_applications", "risk_events"


# ---------------------------------------------------------------------------
# first_difference
# ---------------------------------------------------------------------------

def test_equal_values_have_no_difference():
    value = {"a": 1, "b": [1, {"c": None}], "d": "x", "e": 0.5, "f": True}
    assert first_difference(value, copy.deepcopy(value)) is None
    assert first_difference({}, {}) is None and first_difference([], []) is None


@pytest.mark.parametrize("expected, found", [(1, True), (True, 1), (1, 1.0), (0, False), ([1], (1,)), ("1", 1), (None, 0), ({}, [])])
def test_types_count(expected, found):
    assert first_difference({"v": expected}, {"v": found}) == f"v: expected {expected!r}, got {found!r}"


def test_the_first_difference_is_named_by_its_path():
    assert first_difference({"after": {"a": 1, "b": 2}}, {"after": {"a": 1, "b": 3}}) == "after.b: expected 2, got 3"
    assert first_difference({"after": {"a": [1, 2]}}, {"after": {"a": [1, 5]}}) == "after.a[1]: expected 2, got 5"
    assert first_difference({"a": [1, 2]}, {"a": [1]}) == "a: expected 2 elements, got 1"
    assert first_difference({"a": 1}, {"a": 1, "b": 2}) == "b: unexpected"
    assert first_difference({"a": 1, "b": 2}, {"a": 1}) == "b: missing"
    assert first_difference({"x": {"a": 1}}, {"x": {}}) == "x.a: missing"
    assert first_difference(1, 2) == "event: expected 1, got 2"
    assert first_difference("a", None) == "event: expected 'a', got None"


def test_the_earliest_field_in_the_expected_order_is_reported():
    assert first_difference({"a": 1, "b": 1}, {"a": 2, "b": 2}) == "a: expected 1, got 2"


# ---------------------------------------------------------------------------
# the payload hash and digest
# ---------------------------------------------------------------------------

def sample():
    return event(M, "c", 0x1000, 0, 1, serial=3)


@pytest.mark.parametrize("change", [
    lambda e: e["source"].update(commit_lsn=0x2000),
    lambda e: e["source"].update(change_lsn=1),
    lambda e: e["source"].update(commit_ts="2031-01-01T00:00:00.000000Z"),
    lambda e: e["source"].update(db="another_database"),
    lambda e: e["transaction"].update(id=99),
    lambda e: e["transaction"].update(position=4, size=9),
    lambda e: e.update(ts_ms=5, ingested_at="2031-01-01T00:00:00.000000Z"),
    lambda e: e.update(envelope_version=2),
])
def test_the_payload_hash_ignores_where_and_when_an_event_was_read(change):
    other = copy.deepcopy(sample())
    change(other)
    assert payload_hash(other) == payload_hash(sample())


@pytest.mark.parametrize("change", [
    lambda e: e["source"].update(table=T),
    lambda e: e.update(op="u"),
    lambda e: e.update(event_date="2025-03-02"),
    lambda e: e.update(event_ts="2025-03-01T08:00:01.000000Z"),
    lambda e: e.update(flags=["noop_update"]),
    lambda e: e.update(before={"merchant_id": "x"}),
    lambda e: e["after"].update(risk_score=8),
    lambda e: e["after"].update(merchant_id="somebody else"),
])
def test_the_payload_hash_sees_what_the_event_says_about_the_row(change):
    other = copy.deepcopy(sample())
    change(other)
    assert payload_hash(other) != payload_hash(sample())


def test_the_payload_hash_is_sixty_bits_and_fixed():
    assert 0 <= payload_hash(sample()) < 2 ** 60 and payload_hash(sample()) == payload_hash(copy.deepcopy(sample()))
    assert payload_hash(sample()) == 589030388814748489      # a fixed value: the algorithm is what a real run's digests mean


def test_a_digest_can_be_built_with_either_hash():
    assert Digest().add(sample()) == Digest(1, replay.event_hash(sample()))
    assert Digest().add(sample(), payload_hash) == Digest(1, payload_hash(sample()))


# ---------------------------------------------------------------------------
# roundtrip
# ---------------------------------------------------------------------------

def test_every_landed_event_comes_back_equal_and_the_counts_and_digests_are_reported():
    land = busy_landing()
    result = roundtrip(land.reader(), source_db=SOURCE_DB)
    oracle = landed(land)
    assert result.events == sum(len(v) for v in oracle.values()) == 27
    assert result.objects == len(land.sink.objects)
    for table, events in oracle.items():
        assert result.digests[table] == Digest(len(events), sum(payload_hash(e) for e in events)), table
    assert result.digests[A].rows == 2 and result.digests[T].rows == 12


def test_the_digests_do_not_depend_on_the_lsns_the_clock_or_the_transaction_ids():
    one, other = busy_landing(), Landing()
    other._next_lsn += 0x555500                      # the same rows, committed at other positions
    for date in ["2025-02-27", "2025-03-01", "2025-04-02"] * 2:
        other.transaction((M, "c"), (T, "c"), (T, "c"), (K, "c"), date=date)
        other.flush(clock=77)
    other.transaction((A, "c"), date="2025-03-01")
    other.flush(clock=88)
    other.transaction((A, "u"), ("loan_repayments", "c"), date="2025-03-01")
    other.flush(clock=99)
    assert roundtrip(one.reader(), source_db=SOURCE_DB).digests == roundtrip(other.reader(), source_db=SOURCE_DB).digests


def test_a_changed_value_changes_the_digest_of_its_table_only():
    land = busy_landing()
    before = roundtrip(land.reader(), source_db=SOURCE_DB).digests
    name = next(n for n in sorted(land.sink.objects) if "/risk_events/" in n)
    events = events_in(land.sink.objects[name])
    events[0]["after"]["rule_id"] = "another rule"
    from tests.bronze_synth import encode
    land.sink.objects[name] = encode(events)
    after = roundtrip(land.reader(), source_db=SOURCE_DB).digests
    assert [t for t in before if before[t] != after[t]] == [K] and after[K].rows == before[K].rows


def test_a_loss_in_the_inverse_stops_the_run_naming_the_object_event_and_field(monkeypatch):
    land = busy_landing()
    keys = [o.key for o in land.reader().list_keys()]
    counts = [len(events_in(land.sink.objects[k])) for k in keys]
    third_overall = 3                                         # the third event read, counting across objects in order
    owner, position = next((k, third_overall - sum(counts[:i])) for i, k in enumerate(keys) if sum(counts[:i + 1]) >= third_overall)
    real, calls = replay.row_to_event, []

    def lossy(table, row, *, source_db):
        calls.append(table)
        out = real(table, row, source_db=source_db)
        if len(calls) == third_overall:
            out["after"][next(iter(out["after"]))] = "lost"
        return out

    monkeypatch.setattr(replay, "row_to_event", lossy)
    with pytest.raises(RoundTripError) as stop:
        roundtrip(land.reader(), source_db=SOURCE_DB, workers=1)
    text = str(stop.value)
    assert text.startswith(f"{owner}, event {position}: ") and "did not come back equal from the pass 2 conversion: after." in text
    assert ": expected " in text and ", got 'lost'" in text
    assert isinstance(stop.value, ContractError) and len(calls) == third_overall


def test_a_mapping_error_in_the_inverse_names_the_object_and_event(monkeypatch):
    land = spread_landing(3)
    first = land.reader().list_keys()[0].key

    def refuse(table, row, *, source_db):
        raise MappingError(table, "merchant_id", "refused on purpose")

    monkeypatch.setattr(replay, "row_to_event", refuse)
    with pytest.raises(MappingError) as stop:
        roundtrip(land.reader(), source_db=SOURCE_DB, workers=1)
    assert str(stop.value).startswith(f"{first}, event 1: ")


def test_a_value_the_conversion_refuses_is_a_mapping_error_naming_the_object():
    land = Landing()
    bad = event(T, "c", 0x3000, 0, 1)
    bad["after"]["risk_score"] = 2 ** 40                    # an int column
    name = land.put(T, "2025-03-01", [bad])
    with pytest.raises(MappingError) as stop:
        roundtrip(land.reader(), source_db=SOURCE_DB)
    assert str(stop.value).startswith(f"{name}: ") and "risk_score" in str(stop.value)


def test_the_conversion_uses_each_tables_own_schema_and_source_database():
    with pytest.raises(MappingError):
        roundtrip(busy_landing().reader(), source_db="another_database")


def test_progress_is_reported_every_hundred_objects():
    land = Landing()
    for _ in range(120):
        land.transaction((M, "c"))
        land.flush()
    said = []
    roundtrip(land.reader(), source_db=SOURCE_DB, say=said.append)
    assert said == ["round trip: 100 of 120 objects"]


def test_each_object_is_read_once_and_nothing_is_written():
    land = busy_landing()
    before = dict(land.sink.objects)
    spy = Spy(land.sink)
    roundtrip(spy.reader(), source_db=SOURCE_DB)
    assert sorted(spy.reads) == sorted(before) and spy.listings == 1 and land.sink.objects == before


def test_the_workers_argument_sets_how_many_reads_run_at_once():
    land = spread_landing(7)
    one, many = Concurrency(land.sink), Concurrency(land.sink)
    roundtrip(one.reader(), source_db=SOURCE_DB, workers=1)
    roundtrip(many.reader(), source_db=SOURCE_DB, workers=4)
    assert max(one.levels) == 1 and max(many.levels) > 1


def test_an_empty_landing_gives_an_empty_result():
    result = roundtrip(Landing().reader(), source_db=SOURCE_DB)
    assert (result.events, result.objects) == (0, 0) and all(d == Digest() for d in result.digests.values())


# ---------------------------------------------------------------------------
# the command
# ---------------------------------------------------------------------------

@pytest.fixture
def environ(tmp_path):
    file = tmp_path / "credentials.json"
    file.write_text('{"refresh_token": "SECRET"}')
    return {CREDENTIALS_VARIABLE: str(file)}


def command(land, environ, *, do_roundtrip, spy=None):
    out, err = io.StringIO(), io.StringIO()
    reader = (spy or land).reader()
    code = check.execute(BronzeSettings(_env_file=None), do_roundtrip=do_roundtrip, reader_factory=lambda s: reader,
                         environ=environ, out=out, err=err)
    return code, out.getvalue(), err.getvalue()


def test_without_the_flag_the_command_does_not_round_trip_and_reads_each_object_once(environ):
    land = busy_landing()
    spy = Spy(land.sink)
    code, out, err = command(land, environ, do_roundtrip=False, spy=spy)
    assert code == 0 and "round trip" not in out and "round trip" not in err
    assert out.splitlines()[-1] == "OK: every object is valid, every event maps to a Bronze row and every transaction is complete"
    assert len(spy.reads) == len(land.sink.objects)


def test_with_the_flag_it_prints_the_events_and_payload_digests_and_reads_every_object_twice(environ):
    land = busy_landing()
    spy = Spy(land.sink)
    code, out, err = command(land, environ, do_roundtrip=True, spy=spy)
    assert code == 0 and len(spy.reads) == 2 * len(land.sink.objects) and spy.listings == 2
    lines = out.splitlines()
    assert lines[-1] == ("OK: every object is valid, every event maps to a Bronze row and every transaction is complete, "
                         "and every event came back equal from the pass 2 conversion")
    assert "converting every event as pass 2 will and mapping it back (a second read of every object)" in lines
    n = len(land.sink.objects)
    assert f"round trip through the pass 2 conversion: 27 events in {n} objects, every one came back equal" in lines
    digests = {line.split()[0]: line.split()[1:] for line in lines if line.split() and line.split()[0] in
               (M, T, A, "loan_repayments", K, "total") and len(line.split()) in (2, 3)}
    oracle = landed(land)
    for table, events in oracle.items():
        assert digests[table] == [f"{len(events):,}", f"{sum(payload_hash(e) for e in events):x}"], table
    assert digests["total"] == ["27"]
    start = lines.index("converting every event as pass 2 will and mapping it back (a second read of every object)")
    assert start > lines.index(next(l for l in lines if l.startswith("transactions")))
    assert lines[start - 1] == "" and lines[start + 1].startswith("round trip through the pass 2 conversion")


def test_the_round_trip_reads_with_the_workers_setting(environ, monkeypatch):
    land = spread_landing(7)
    for workers, wide in (("1", False), ("4", True)):
        monkeypatch.setenv("BRONZE_READ_WORKERS", workers)
        spy = Concurrency(land.sink)
        assert command(land, environ, do_roundtrip=True, spy=spy)[0] == 0
        second_pass = spy.levels[7:]                    # pass 1 read the first 7 objects, the round trip the next 7
        assert len(second_pass) == 7 and (max(second_pass) > 1) is wide


def test_the_round_trips_progress_goes_to_stderr(environ):
    land = Landing()
    for _ in range(100):
        land.transaction((M, "c"))
        land.flush()
    code, out, err = command(land, environ, do_roundtrip=True)
    assert code == 0 and "round trip: 100 of 100 objects" in err.splitlines() and "round trip: 100" not in out


def test_a_failing_first_pass_stops_before_the_round_trip(environ):
    land = busy_landing()
    land.put(T, "2025-03-01", [event(T, "c", 0x9000, 0, 2)])        # an incomplete transaction
    spy = Spy(land.sink)
    code, out, err = command(land, environ, do_roundtrip=True, spy=spy)
    assert code == 2 and "converting every event" not in out and "round trip" not in out
    assert len(spy.reads) == len(land.sink.objects)             # only pass 1 read them


def test_a_round_trip_failure_is_a_contract_stop_with_one_error_line_and_no_ok(environ, monkeypatch):
    real = replay.row_to_event

    def lossy(table, row, *, source_db):
        out = real(table, row, source_db=source_db)
        out["flags"] = ["event_time_fallback"]
        return out

    monkeypatch.setattr(replay, "row_to_event", lossy)
    code, out, err = command(busy_landing(), environ, do_roundtrip=True)
    assert code == 2 and "OK:" not in out and "every one came back equal" not in out
    assert "converting every event as pass 2 will and mapping it back" in out
    errors = [l for l in err.splitlines() if l.startswith("error:")]
    assert len(errors) == 1 and "did not come back equal from the pass 2 conversion: flags" in errors[0]


def test_the_exit_code_of_a_round_trip_error_is_a_contract_stop():
    assert check.exit_code(RoundTripError("x")) == check.EXIT_CONTRACT
    assert check.reason(RoundTripError("one  line\nonly")) == "one line only"


def test_the_flag_is_accepted_and_documented(capsys, monkeypatch):
    monkeypatch.delenv(CREDENTIALS_VARIABLE, raising=False)
    assert check.main(["--help"]) == 0
    assert "--roundtrip" in capsys.readouterr().out
    assert check.main(["--roundtrip"]) == 1                      # no credentials: the guard stops it before anything opens
    assert "error:" in capsys.readouterr().err
    assert check.main(["--no-such-flag"]) == 1


def test_the_flag_reaches_execute(monkeypatch):
    seen = {}
    monkeypatch.setattr(check, "execute", lambda settings, **kwargs: seen.update(kwargs) or 0)
    assert check.main(["--roundtrip"]) == 0 and seen == {"do_roundtrip": True}
    seen.clear()
    assert check.main([]) == 0 and seen == {"do_roundtrip": False}


def test_importing_the_check_command_does_not_load_the_replay_module():
    # the round trip pulls in psycopg and the reconcile code; a plain bronze.check run, and the memory probe that imports
    # bronze.check, must not pay for them (the probe's 150 MB baseline test failed when this import was at the top)
    import subprocess
    import sys
    run = subprocess.run([sys.executable, "-c", "import sys, bronze.check; sys.exit(int('bronze.replay' in sys.modules))"])
    assert run.returncode == 0
