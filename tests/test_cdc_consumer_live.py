"""tests/test_cdc_consumer_live.py: the M4 done-when tests, against a real slot.

A real Reader and a real Postgres slot, with the in-memory sink standing in for the bucket (the
real writer is tested in step 5 and by the manual smoke test). Each transaction written here is
a Chain 5 one: a merchant snapshot, a payment and the risk event that points at it, committed
together, so every transaction has three changes in three tables.

Done when (CDC Plan, M4): a failed upload leaves the slot unconfirmed; a replayed batch is
absorbed by LSN dedup; a transaction and its risk events land together.
"""
from datetime import datetime, timezone

import psycopg
import pytest

from cdc.consumer import Consumer
from cdc.dedup import dedup, events_in, incomplete_transactions, key
from cdc.errors import SinkError
from cdc.reader import Reader
from tests.fakes import MemorySink, SimulatedCrash
from tests.test_cdc_reader import assemble, slot_position

PREFIX = "cdc/cdc_r01/"
PUBLICATION = "fintech_cdc"
NOW = datetime(2026, 10, 27, 9, 41, 7, 490000, tzinfo=timezone.utc)
LATER = datetime(2026, 10, 27, 9, 52, 30, 123000, tzinfo=timezone.utc)


def write_chain5(dsn, days, first=0):
    """One transaction per entry of `days` (the March 2025 day its rows belong to): a merchant
    snapshot, a payment and its risk event. Transactions with the same day share their
    (table, event date) objects."""
    with psycopg.connect(dsn) as conn:
        for n, day in enumerate(days, start=first):
            stamp = f"2025-03-{day:02d} 10:00+00"
            with conn.transaction():
                conn.execute(
                    "INSERT INTO merchants (merchant_id, archetype, province, onboarded_at, snapshot_at) "
                    "VALUES (%s, 'tutor', 'gauteng', '2025-01-01 00:00+00', %s)", (f"m-{n}", stamp))
                conn.execute(
                    "INSERT INTO merchant_transactions (transaction_id, merchant_id, event_type, channel, status, "
                    "amount_minor, created_at) VALUES (%s, %s, 'payment', 'card_present', 'completed', 12550, %s)",
                    (f"t-{n}", f"m-{n}", stamp))
                conn.execute(
                    "INSERT INTO risk_events (alert_id, entity_type, entity_id, merchant_id, rule_id, rule_type, "
                    "risk_score, risk_score_delta, action, action_by, action_ts) "
                    "VALUES (%s, 'transaction', %s, %s, 'rule-large-txn', 'large_txn_flag', 15, 5, 'flag_review', "
                    "'system_auto', %s)", (f"a-{n}", f"t-{n}", f"m-{n}", stamp))


def everything_waiting(dsn, slot):
    """What the slot holds, as whole transactions (a peek: nothing is consumed)."""
    with Reader(dsn, slot, PUBLICATION) as peek:
        messages = peek.fetch(10 ** 9)
    return messages, assemble(messages)


def expected_keys(transactions):
    return {(t.commit_lsn, p) for t in transactions for p in range(len(t.changes))}


def landed(sink):
    return [e for data in sink.objects.values() for e in events_in(data)]


def consumer(reader, sink, **kwargs):
    kwargs.setdefault("batch_size", 10_000)
    kwargs.setdefault("workers", 1)
    kwargs.setdefault("clock", lambda: NOW)
    return Consumer(reader, sink, database="fintech_cdc", prefix=PREFIX, **kwargs)


# ---------------------------------------------------------------------------
# A failed upload leaves the slot unconfirmed
# ---------------------------------------------------------------------------

def test_a_failed_upload_leaves_the_slot_unconfirmed_and_the_next_poll_returns_the_same_transactions(cdc_dsn, cdc_slot):
    write_chain5(cdc_dsn, [1, 2, 3])                      # three transactions on three dates: nine objects
    waiting, transactions = everything_waiting(cdc_dsn, cdc_slot)
    start = slot_position(cdc_dsn, cdc_slot)
    sink, warnings = MemorySink(), []
    sink.fail_put = lambda attempt, key: attempt == 4     # the first three objects land, the fourth fails

    with Reader(cdc_dsn, cdc_slot, PUBLICATION) as reader:
        with pytest.raises(SinkError):
            consumer(reader, sink, warn=warnings.append).run(follow=False)
        assert slot_position(cdc_dsn, cdc_slot) == start              # not confirmed
        assert len(sink.objects) == 3
        assert reader.fetch(10 ** 9) == waiting                       # the same messages, byte for byte
        sink.fail_put = None
        summary = consumer(reader, sink, clock=lambda: LATER).run(follow=False)

    assert (summary.objects_replayed, summary.objects_created) == (3, 6)      # what landed before is recognised
    assert slot_position(cdc_dsn, cdc_slot) == transactions[-1].end_lsn != transactions[-1].commit_lsn
    assert len(sink.objects) == 9
    assert {key(e) for e in landed(sink)} == expected_keys(transactions)
    assert warnings == []                                                     # a healthy slot is silent


def test_after_a_drain_the_slot_has_nothing_left_and_a_second_run_finds_nothing(cdc_dsn, cdc_slot):
    write_chain5(cdc_dsn, [1, 1, 2])
    _, transactions = everything_waiting(cdc_dsn, cdc_slot)
    sink = MemorySink()
    with Reader(cdc_dsn, cdc_slot, PUBLICATION) as reader:
        first = consumer(reader, sink, batch_size=1).run(follow=False)
        assert first.batches == 3 and first.confirmed_lsn == transactions[-1].end_lsn
        assert reader.fetch(10 ** 9) == []
        again = consumer(reader, sink).run(follow=False)
    assert (again.batches, again.idle_polls) == (0, 1)


# ---------------------------------------------------------------------------
# A replayed batch is absorbed by LSN dedup
# ---------------------------------------------------------------------------

def test_a_kill_between_the_last_upload_and_the_confirm_replays_the_batch_and_dedup_absorbs_it(cdc_dsn, cdc_slot):
    write_chain5(cdc_dsn, [1, 2, 3])
    _, transactions = everything_waiting(cdc_dsn, cdc_slot)
    start = slot_position(cdc_dsn, cdc_slot)
    sink = MemorySink()

    class DiesBeforeTheConfirm(Reader):
        def confirm(self, end_lsn):
            raise SimulatedCrash("killed after the last upload, before the confirm")

    with DiesBeforeTheConfirm(cdc_dsn, cdc_slot, PUBLICATION) as dying:
        with pytest.raises(SimulatedCrash):
            consumer(dying, sink).run(follow=False)
    assert slot_position(cdc_dsn, cdc_slot) == start
    first_run = dict(sink.objects)
    assert len(first_run) == 9                                        # everything had landed

    with Reader(cdc_dsn, cdc_slot, PUBLICATION) as reader:            # the restart
        summary = consumer(reader, sink, clock=lambda: LATER).run(follow=False)
    assert (summary.objects_created, summary.objects_replayed) == (0, 9)
    assert sink.objects == first_run                                  # nothing overwritten, nothing new
    assert slot_position(cdc_dsn, cdc_slot) == transactions[-1].end_lsn
    events = landed(sink)
    assert {key(e) for e in dedup(events)} == expected_keys(transactions) and len(events) == 9
    assert incomplete_transactions(dedup(events)) == {}


def test_a_replay_that_covers_more_data_overlaps_the_old_objects_and_dedup_still_gives_each_change_once(cdc_dsn, cdc_slot):
    # The first batch is one transaction; it lands but the confirm is killed. More data arrives, and
    # the restart reads a bigger batch, so its objects have different names that overlap the old ones.
    write_chain5(cdc_dsn, [1, 1, 2])
    sink = MemorySink()

    class DiesBeforeTheConfirm(Reader):
        def confirm(self, end_lsn):
            raise SimulatedCrash("killed before the confirm")

    with DiesBeforeTheConfirm(cdc_dsn, cdc_slot, PUBLICATION) as dying:
        with pytest.raises(SimulatedCrash):
            consumer(dying, sink, batch_size=1).run(follow=False)
    old = dict(sink.objects)
    assert len(old) == 3                                              # the first transaction's three objects

    write_chain5(cdc_dsn, [2, 4], first=3)                            # arrives while the consumer was down
    _, transactions = everything_waiting(cdc_dsn, cdc_slot)
    assert len(transactions) == 5
    with Reader(cdc_dsn, cdc_slot, PUBLICATION) as reader:
        consumer(reader, sink, clock=lambda: LATER).run(follow=False)

    assert set(old) < set(sink.objects) and all(sink.objects[k] == v for k, v in old.items())   # old ones untouched
    events = landed(sink)
    assert len(events) > len(expected_keys(transactions))             # the first transaction's events are there twice
    once = dedup(events)
    assert {key(e) for e in once} == expected_keys(transactions) and len(once) == 15
    assert incomplete_transactions(once) == {}


# ---------------------------------------------------------------------------
# A transaction and its risk events land together
# ---------------------------------------------------------------------------

def test_a_transaction_and_its_risk_events_land_in_the_same_flush(cdc_dsn, cdc_slot):
    write_chain5(cdc_dsn, [1, 1, 2, 3, 3, 4])
    _, transactions = everything_waiting(cdc_dsn, cdc_slot)
    sink = MemorySink()
    flushes = []
    with Reader(cdc_dsn, cdc_slot, PUBLICATION) as reader:
        c = consumer(reader, sink, batch_size=12)                     # about two transactions to a flush (8 messages for the first, 5 for each after)
        while (result := c.process_batch()) is not None:
            flushes.append(result)

    assert sum(f.transactions for f in flushes) == 6 and len(flushes) > 1 and any(f.transactions > 1 for f in flushes)
    seen = set()
    for flush_result in flushes:
        events = [e for k in flush_result.objects for e in events_in(sink.objects[k])]
        assert incomplete_transactions(events) == {}                  # every transaction in this flush is whole
        commits = {e["source"]["commit_lsn"] for e in events}
        assert not commits & seen                                     # and no transaction is split across flushes
        seen |= commits
        for commit in commits:                                        # the payment and its risk event
            payment = next(e for e in events if e["source"]["commit_lsn"] == commit and e["source"]["table"] == "merchant_transactions")
            risk = next(e for e in events if e["source"]["commit_lsn"] == commit and e["source"]["table"] == "risk_events")
            assert payment["transaction"]["id"] == risk["transaction"]["id"]
            assert payment["transaction"]["size"] == risk["transaction"]["size"] == 3
            assert payment["after"]["transaction_id"] == risk["after"]["entity_id"]
    assert seen == {t.commit_lsn for t in transactions}
    assert incomplete_transactions(dedup(landed(sink))) == {}
