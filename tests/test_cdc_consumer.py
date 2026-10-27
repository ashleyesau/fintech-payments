"""tests/test_cdc_consumer.py: M4 step 4, the loop, without a database.

The reader is a scripted stand-in over captured pgoutput messages (tests/fakes.py: it follows the
real reader's rules) and the sink is the in-memory fake, so every ordering and failure can be
produced on demand. The same behaviour against a real slot is in test_cdc_consumer_live.py.
"""
import threading
import time
from datetime import datetime, timezone

import pytest

from cdc.consumer import Consumer, LagWatch, flush
from cdc.dedup import dedup, events_in, incomplete_transactions, key
from cdc.errors import DecodeError, ObjectMismatch, SinkError, SlotLost
from cdc.landing import build_batch
from cdc.lsn import format_lsn
from cdc.reader import SlotStatus
from tests.fakes import FakeReader, MemorySink, SimulatedCrash
from tests.helpers import begin_message, commit_message, pgoutput_scenario
from tests.test_cdc_envelope import NOW, assemble

PREFIX = "cdc/cdc_r01/"
MB = 1024 * 1024
CAP = 3072 * MB
LATER = datetime(2026, 10, 27, 9, 52, 30, 123000, tzinfo=timezone.utc)


def stream():
    """Five real transactions: inserts_utc (2) then the disbursement scenario (3), in LSN order."""
    return pgoutput_scenario("inserts_utc") + pgoutput_scenario("disbursement")


def transactions():
    return assemble("inserts_utc") + assemble("disbursement")


def consumer(reader, sink, **kwargs):
    kwargs.setdefault("batch_size", 1)
    kwargs.setdefault("workers", 4)
    kwargs.setdefault("poll_interval", 30.0)
    kwargs.setdefault("clock", lambda: NOW)
    return Consumer(reader, sink, database="fintech_cdc", prefix=PREFIX, **kwargs)


def all_events(sink):
    return [e for data in sink.objects.values() for e in events_in(data)]


def status(wal_status="reserved", safe=CAP, cap=CAP):
    return SlotStatus(wal_status, safe, cap, 0, 0)


# ---------------------------------------------------------------------------
# flush: objects uploaded in parallel, all or an error
# ---------------------------------------------------------------------------

def objects_for(*tx):
    return build_batch(tx or transactions(), "fintech_cdc", PREFIX, NOW)


def test_a_flush_lands_every_object_and_a_second_flush_finds_them_already_there():
    objects, sink = objects_for(), MemorySink()
    assert len(objects) == 5          # the disbursement transactions join the applications object
    first = flush(sink, objects, workers=4)
    assert (first.created, first.replayed) == (5, 0) and len(sink.objects) == 5
    again = flush(sink, build_batch(transactions(), "fintech_cdc", PREFIX, LATER), workers=4)
    assert (again.created, again.replayed) == (0, 5)
    assert sink.objects == {o.key: o.data for o in objects}          # the replay overwrote nothing


def test_an_empty_flush_does_nothing():
    sink = MemorySink()
    assert flush(sink, [], workers=8) == flush(sink, [], workers=1)
    assert sink.attempts == 0


def test_objects_are_uploaded_in_parallel_up_to_the_worker_count():
    objects = objects_for()
    lock, state = threading.Lock(), {"now": 0, "peak": 0}

    class Slow(MemorySink):
        def put_if_absent(self, key, data):
            with lock:
                state["now"] += 1
                state["peak"] = max(state["peak"], state["now"])
            time.sleep(0.05)
            with lock:
                state["now"] -= 1
            return super().put_if_absent(key, data)

    flush(Slow(), objects, workers=3)
    assert state["peak"] == 3                                        # parallel, and no more than asked
    state["peak"] = 0
    flush(Slow(), objects_for(), workers=1)
    assert state["peak"] == 1


def test_after_a_failure_uploads_not_yet_started_are_skipped_and_the_error_is_raised():
    objects, sink = objects_for(), MemorySink()
    sink.fail_put = lambda attempt, key: attempt == 2
    with pytest.raises(SinkError):
        flush(sink, objects, workers=1)
    assert len(sink.objects) == 1 and sink.attempts == 2             # the first landed, the third onward never started


def test_with_several_workers_the_uploads_in_flight_finish_and_the_failed_one_is_missing():
    objects = objects_for()
    failing = objects[2].key
    everyone_is_uploading = threading.Barrier(len(objects))     # all five are inside the upload before one fails

    class OneFails(MemorySink):
        def put_if_absent(self, key, data):
            everyone_is_uploading.wait(timeout=5)
            if key == failing:
                raise SinkError(f"injected failure writing {key}")
            time.sleep(0.05)
            return super().put_if_absent(key, data)

    sink = OneFails()
    with pytest.raises(SinkError, match="injected"):
        flush(sink, objects, workers=len(objects))
    assert set(sink.objects) == {o.key for o in objects} - {failing}


def test_the_first_failure_in_object_order_is_the_one_raised():
    objects, sink = objects_for(), MemorySink()
    sink.objects[objects[0].key] = b"someone else's bytes"           # a mismatch on the first object
    sink.fail_put = lambda attempt, key: key == objects[3].key       # and a sink failure on the fourth
    with pytest.raises(ObjectMismatch):
        flush(sink, objects, workers=6)


def test_an_interrupt_raised_in_a_worker_propagates():
    sink = MemorySink()

    def interrupt(attempt, key):
        raise KeyboardInterrupt

    sink.fail_put = interrupt
    with pytest.raises(KeyboardInterrupt):
        flush(sink, objects_for(), workers=2)


# ---------------------------------------------------------------------------
# LagWatch (M4-Q8)
# ---------------------------------------------------------------------------

def test_a_healthy_slot_and_an_unlimited_cap_are_silent():
    watch = LagWatch("cdc_r01")
    assert watch.check(status()) == []
    assert watch.check(status(safe=None, cap=None)) == []
    assert watch.check(status(safe=CAP // 2)) == []                  # exactly half is not under half


def test_under_half_warns_once_with_the_headroom_and_the_cap():
    watch = LagWatch("cdc_r01")
    [message] = watch.check(status(safe=1000 * MB))
    assert "cdc_r01" in message and "1,000 MB" in message and "3,072 MB" in message and "under half" in message
    assert "cannot be repaired" in message and "Pause the generator" in message
    assert watch.check(status(safe=900 * MB)) == []


def test_the_half_level_starts_just_under_half_the_cap():
    assert LagWatch("cdc_r01").check(status(safe=CAP // 2 + MB)) == []            # just over half
    assert len(LagWatch("cdc_r01").check(status(safe=CAP // 2 - MB))) == 1        # just under half
    assert len(LagWatch("cdc_r01").check(status(safe=1300 * MB))) == 1            # between a third and a half
    assert "under half" in LagWatch("cdc_r01").check(status(safe=CAP // 10 + 10 * MB))[0]
    assert "under a tenth" in LagWatch("cdc_r01").check(status(safe=CAP // 10 - MB))[0]


def test_under_a_tenth_warns_again_once():
    watch = LagWatch("cdc_r01")
    assert len(watch.check(status(safe=1000 * MB))) == 1
    [message] = watch.check(status(safe=300 * MB))
    assert "under a tenth" in message and "300 MB" in message
    assert watch.check(status(safe=200 * MB)) == []


def test_a_slot_that_jumps_straight_below_a_tenth_gets_one_warning_not_two():
    watch = LagWatch("cdc_r01")
    assert len(watch.check(status(safe=100 * MB))) == 1
    assert watch.check(status(safe=1000 * MB)) == []                 # the half warning is already covered


def test_the_warnings_fire_again_after_the_slot_recovers_above_half():
    watch = LagWatch("cdc_r01")
    assert len(watch.check(status(safe=1000 * MB))) == 1
    assert watch.check(status(safe=CAP)) == []                       # recovered: re-armed
    assert len(watch.check(status(safe=1000 * MB))) == 1


def test_an_unreserved_slot_warns_once_and_never_prints_a_negative_headroom():
    watch = LagWatch("cdc_r01")
    [message] = watch.check(status("unreserved", safe=-97 * MB))
    assert "unreserved" in message and "not lost yet" in message and "-97" not in message
    assert watch.check(status("unreserved", safe=-100 * MB)) == []


# ---------------------------------------------------------------------------
# the loop against a scripted reader and the fake sink
# ---------------------------------------------------------------------------

def test_a_drain_lands_every_batch_and_confirms_each_ones_end_lsn_after_its_objects():
    sink = MemorySink()
    seen = []
    reader = FakeReader(stream(), on_confirm=lambda lsn: seen.append((lsn, set(sink.objects))))
    summary = consumer(reader, sink).run(follow=False)
    expected = transactions()
    assert reader.confirmed == [t.end_lsn for t in expected]                 # end_lsn, one per batch
    assert all(t.end_lsn != t.commit_lsn for t in expected)
    assert (summary.batches, summary.transactions, summary.changes) == (5, 5, 11)
    assert summary.confirmed_lsn == expected[-1].end_lsn and summary.idle_polls == 1
    assert not summary.stopped_by_request
    # every confirm came after its transaction's objects had landed
    for (lsn, landed), transaction in zip(seen, expected):
        wanted = {o.key for o in build_batch([transaction], "fintech_cdc", PREFIX, NOW)}
        assert wanted <= landed, format_lsn(lsn)
    assert len({key(e) for e in all_events(sink)}) == 11


def test_a_bigger_batch_is_one_flush_and_one_confirm():
    sink, reader = MemorySink(), FakeReader(stream())
    summary = consumer(reader, sink, batch_size=1000).run(follow=False)
    assert reader.confirmed == [transactions()[-1].end_lsn] and summary.batches == 1
    assert len(sink.objects) == len({o.key for o in build_batch(transactions(), "fintech_cdc", PREFIX, NOW)})


def test_an_empty_slot_is_one_idle_poll_and_nothing_else():
    sink, reader = MemorySink(), FakeReader([])
    summary = consumer(reader, sink).run(follow=False)
    assert (summary.batches, summary.idle_polls, summary.confirmed_lsn) == (0, 1, None)
    assert sink.attempts == 0 and reader.confirmed == []


def test_a_trailing_empty_transaction_is_confirmed_so_the_slot_moves_past_it():
    empty = [(60_000_000, begin_message(60_000_100, 900)), (60_000_150, commit_message(60_000_100, 60_000_150))]
    sink, reader = MemorySink(), FakeReader(stream() + empty)
    summary = consumer(reader, sink, batch_size=1000).run(follow=False)
    assert reader.confirmed == [60_000_150]                                  # the empty transaction's end position
    assert summary.changes == 11 and summary.confirmed_lsn == 60_000_150


def test_a_batch_of_only_empty_transactions_uploads_nothing_and_still_confirms():
    empty = [(70_000_000, begin_message(70_000_100, 901)), (70_000_150, commit_message(70_000_100, 70_000_150))]
    sink, reader = MemorySink(), FakeReader(empty)
    summary = consumer(reader, sink).run(follow=False)
    assert sink.attempts == 0 and reader.confirmed == [70_000_150] and summary.batches == 1


def test_a_failed_upload_leaves_the_slot_unconfirmed_and_the_next_run_finishes_the_job():
    sink = MemorySink()
    reader = FakeReader(stream())
    sink.fail_put = lambda attempt, key: attempt == 4
    with pytest.raises(SinkError):
        consumer(reader, sink, batch_size=1000, workers=1).run(follow=False)
    assert reader.confirmed == [] and reader.position == 0
    landed_before = set(sink.objects)
    assert 0 < len(landed_before) < 5
    sink.fail_put = None
    summary = consumer(reader, sink, batch_size=1000, clock=lambda: LATER).run(follow=False)
    assert summary.objects_replayed == len(landed_before) and summary.objects_created == 5 - len(landed_before)
    assert reader.confirmed == [transactions()[-1].end_lsn]
    assert len({key(e) for e in dedup(all_events(sink))}) == 11


def test_a_crash_between_the_last_upload_and_the_confirm_is_absorbed_on_restart():
    sink = MemorySink()
    crashing = FakeReader(stream(), fail_confirm=SimulatedCrash("killed before the confirm"))
    with pytest.raises(SimulatedCrash):
        consumer(crashing, sink, batch_size=1000).run(follow=False)
    assert crashing.confirmed == []
    before = dict(sink.objects)
    assert len(before) == 5                                                   # everything had landed
    restarted = FakeReader(stream())                                          # the slot position did not move
    summary = consumer(restarted, sink, batch_size=1000, clock=lambda: LATER).run(follow=False)
    assert (summary.objects_created, summary.objects_replayed) == (0, 5)
    assert sink.objects == before                                             # the replay overwrote nothing
    assert restarted.confirmed == [transactions()[-1].end_lsn]
    events = all_events(sink)
    assert len(events) == 11 and len(dedup(events)) == 11


def test_an_object_that_exists_with_different_events_stops_the_run_without_confirming():
    sink, reader = MemorySink(), FakeReader(stream())
    wanted = build_batch(transactions()[:1], "fintech_cdc", PREFIX, NOW)[0]
    sink.objects[wanted.key] = b"not what this batch holds"
    with pytest.raises(ObjectMismatch):
        consumer(reader, sink, batch_size=1000).run(follow=False)
    assert reader.confirmed == []


def test_a_second_interrupt_leaves_the_slot_where_it_was():
    sink, reader = MemorySink(), FakeReader(stream())

    def interrupt(attempt, key):
        raise KeyboardInterrupt

    sink.fail_put = interrupt
    with pytest.raises(KeyboardInterrupt):
        consumer(reader, sink, batch_size=1000).run(follow=False)
    assert reader.confirmed == []


def test_a_stop_request_during_a_batch_lets_that_batch_land_and_confirm_then_stops():
    sink, reader = MemorySink(), FakeReader(stream())
    c = consumer(reader, sink, batch_size=1)

    class StopsDuringUpload(MemorySink):
        def put_if_absent(self, key, data):
            c.request_stop()                                                  # the first Ctrl-C, mid-flush
            return super().put_if_absent(key, data)

    c.sink = sink = StopsDuringUpload()
    summary = c.run(follow=False)
    first = transactions()[0]
    assert reader.confirmed == [first.end_lsn]                                # that batch was confirmed...
    assert summary.batches == 1 and summary.stopped_by_request                # ...and no further one started
    assert {o.key for o in build_batch([first], "fintech_cdc", PREFIX, NOW)} == set(sink.objects)


def test_follow_waits_for_the_poll_interval_and_a_stop_request_wakes_it_at_once():
    sink, reader = MemorySink(), FakeReader([])
    c = consumer(reader, sink, poll_interval=30.0)
    threading.Timer(0.2, c.request_stop).start()
    started = time.perf_counter()
    summary = c.run(follow=True)
    assert time.perf_counter() - started < 5 and summary.stopped_by_request
    assert summary.idle_polls == 1


def test_follow_picks_up_data_that_arrives_while_waiting():
    sink, reader = MemorySink(), FakeReader([])
    c = consumer(reader, sink, poll_interval=0.05, batch_size=1000)

    def data_arrives():
        time.sleep(0.2)
        fresh = FakeReader(stream())
        reader._transactions = fresh._transactions
        time.sleep(0.3)
        c.request_stop()

    threading.Thread(target=data_arrives).start()
    summary = c.run(follow=True)
    assert summary.batches >= 1 and summary.changes == 11 and summary.idle_polls >= 2


def test_a_lost_slot_stops_the_run_before_anything_is_read():
    sink, reader = MemorySink(), FakeReader(stream(), statuses=[status("lost", safe=None)])
    with pytest.raises(SlotLost):
        consumer(reader, sink).run(follow=False)
    assert reader.fetches == 0 and sink.attempts == 0


def test_lag_warnings_reach_the_warn_callback_once_per_level():
    sink, warnings = MemorySink(), []
    statuses = [status(safe=1000 * MB), status(safe=900 * MB), status(safe=200 * MB), status(safe=100 * MB)]
    reader = FakeReader(stream(), statuses=statuses)
    consumer(reader, sink, warn=warnings.append).run(follow=False)
    assert len(warnings) == 2 and "under half" in warnings[0] and "under a tenth" in warnings[1]


def test_each_flush_logs_one_line_with_its_counts_and_the_confirm_position():
    lines = []
    reader = FakeReader(stream())
    consumer(reader, MemorySink(), log=lines.append, batch_size=1000).run(follow=False)
    [line] = lines
    last = transactions()[-1].end_lsn
    assert "landed 11 changes from 5 transactions as 5 objects (5 new, 0 already there)" in line
    assert f"slot confirmed to {format_lsn(last)}" in line


def test_a_fetch_that_ends_inside_a_transaction_is_refused():
    class Truncated(FakeReader):
        def fetch(self, limit):
            return super().fetch(limit)[:-1]                                  # drop the Commit

    sink, reader = MemorySink(), Truncated(stream())
    with pytest.raises(DecodeError, match="whole transactions"):
        consumer(reader, sink, batch_size=1000).run(follow=False)   # four whole transactions, then one cut short
    assert sink.attempts == 0 and reader.confirmed == []              # refused before anything landed or was confirmed


def test_a_run_whose_confirms_do_not_move_the_slot_forward_stops_instead_of_looping():
    class Stuck(FakeReader):
        def confirm(self, end_lsn):
            self.confirmed.append(end_lsn)                                    # accepts it, but the slot stays put

    with pytest.raises(RuntimeError, match="not advancing"):
        consumer(Stuck(stream()), MemorySink()).run(follow=False)


def test_confirming_a_commit_lsn_would_return_the_same_transaction_for_ever_and_the_run_notices():
    # What M2 found: advancing to commit_lsn leaves the transaction in the slot. The fake slot
    # does the same when told to use commit_lsn, so this checks the loop's guard, not the fake.
    transactions_ = transactions()
    commit_of = {t.end_lsn: t.commit_lsn for t in transactions_}

    class CommitLsn(FakeReader):
        def confirm(self, end_lsn):
            super().confirm(commit_of[end_lsn])

    with pytest.raises(RuntimeError, match="not advancing"):
        consumer(CommitLsn(stream()), MemorySink()).run(follow=False)
