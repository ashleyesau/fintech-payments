"""tests/test_cdc_landing.py: M4 step 2, objects, naming, the replay compare and the sink rules.

No database and no network. The transactions are the ones captured in
tests/fixtures/pgoutput.json and assembled by the code the consumer runs, so the data is
real: one transaction holds a payment and its risk event over two event dates (Chain 5), one
holds four applications and a repayment whose date falls back to its due date, and the
disbursement scenario is three transactions that belong in one object.
"""
import dataclasses
import gzip
import json
from datetime import datetime, timezone

import pytest

from cdc.envelope import build_events, to_line
from cdc.errors import ObjectMismatch, SinkError
from cdc.landing import build_batch, land_object, make_object, object_key, same_events
from tests.fakes import MemorySink
from tests.test_cdc_envelope import assemble

PREFIX = "cdc/cdc_r01/"
NOW = datetime(2026, 10, 27, 9, 41, 7, 490000, tzinfo=timezone.utc)
LATER = datetime(2026, 10, 27, 9, 52, 30, 123000, tzinfo=timezone.utc)

# Commit LSNs of the captured transactions, and their 16-digit hex, worked out by hand.
FIRST_INSERTS, SECOND_INSERTS = 45294944, 45297928        # 0000000002b32560, 0000000002b33108
INSERT, UPDATE, NOOP = 45327384, 45327904, 45328440       # 0000000002b3a418, ...a620, ...a838


def decode(obj):
    """The events inside an object, as parsed JSON, in order."""
    text = gzip.decompress(obj.data)
    assert text.endswith(b"\n")
    return [json.loads(line) for line in text[:-1].split(b"\n")]


def by_key(objects):
    return {o.key: o for o in objects}


def clock_free(event):
    return {k: v for k, v in event.items() if k not in ("ts_ms", "ingested_at")}


# ---------------------------------------------------------------------------
# Names (contract section 2)
# ---------------------------------------------------------------------------

def test_an_object_name_is_table_date_and_16_digit_lowercase_hex_lsns():
    assert object_key(PREFIX, "merchants", "2025-03-01", 45294944, 45297928) == (
        "cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_0000000002b32560_0000000002b33108.ndjson.gz")
    assert object_key("cdc/cdc_r02/", "risk_events", "2026-01-31", 0, 2 ** 53 - 1) == (
        "cdc/cdc_r02/risk_events/event_date=2026-01-31/lsn_0000000000000000_001fffffffffffff.ndjson.gz")


def test_names_sort_in_lsn_order():
    keys = [object_key(PREFIX, "merchants", "2025-03-01", n, n) for n in (9, 10, 255, 256, 2 ** 40)]
    assert keys == sorted(keys)


def test_the_names_of_a_real_batch_are_the_commit_lsns_by_table_and_date():
    objects = build_batch(assemble("inserts_utc"), "fintech_cdc", PREFIX, NOW)
    assert [o.key for o in objects] == [
        "cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_0000000002b32560_0000000002b32560.ndjson.gz",
        "cdc/cdc_r01/merchant_transactions/event_date=2025-03-02/lsn_0000000002b32560_0000000002b32560.ndjson.gz",
        "cdc/cdc_r01/risk_events/event_date=2025-03-02/lsn_0000000002b32560_0000000002b32560.ndjson.gz",
        "cdc/cdc_r01/loan_applications/event_date=2025-03-01/lsn_0000000002b33108_0000000002b33108.ndjson.gz",
        "cdc/cdc_r01/loan_repayments/event_date=2025-04-02/lsn_0000000002b33108_0000000002b33108.ndjson.gz",
    ]
    assert [o.events for o in objects] == [1, 1, 1, 4, 1]


def test_the_name_uses_the_commit_lsn_not_a_changes_own_lsn():
    # The four applications share a commit but each has its own, lower, change LSN.
    objects = by_key(build_batch(assemble("inserts_utc"), "fintech_cdc", PREFIX, NOW))
    applications = next(o for o in objects.values() if o.table == "loan_applications")
    assert applications.first_lsn == applications.last_lsn == SECOND_INSERTS
    assert {e["source"]["commit_lsn"] for e in decode(applications)} == {SECOND_INSERTS}
    assert len({e["source"]["change_lsn"] for e in decode(applications)}) == 4


# ---------------------------------------------------------------------------
# One object per table and event date; whole transactions; order
# ---------------------------------------------------------------------------

def test_a_payment_and_its_risk_event_land_in_one_batch_with_the_same_commit_lsn():
    objects = build_batch(assemble("inserts_utc")[:1], "fintech_cdc", PREFIX, NOW)
    by_table = {o.table: o for o in objects}
    payment = decode(by_table["merchant_transactions"])[0]
    risk = decode(by_table["risk_events"])[0]
    assert payment["source"]["commit_lsn"] == risk["source"]["commit_lsn"] == FIRST_INSERTS
    assert payment["transaction"]["id"] == risk["transaction"]["id"]
    assert (payment["transaction"]["position"], risk["transaction"]["position"]) == (1, 2)
    assert payment["transaction"]["size"] == risk["transaction"]["size"] == 3
    assert payment["event_date"] == risk["event_date"] == "2025-03-02"


def test_every_change_of_every_transaction_lands_exactly_once():
    transactions = assemble("inserts_utc") + assemble("disbursement")
    objects = build_batch(transactions, "fintech_cdc", PREFIX, NOW)
    landed = [(e["source"]["commit_lsn"], e["transaction"]["position"]) for o in objects for e in decode(o)]
    expected = [(t.commit_lsn, p) for t in transactions for p in range(len(t.changes))]
    assert sorted(landed) == sorted(expected)
    assert len(landed) == len(set(landed)) == sum(len(t.changes) for t in transactions) == 11
    # and a transaction is complete: its positions run 0 to size - 1 across all its objects
    for transaction in transactions:
        positions = sorted(p for c, p in landed if c == transaction.commit_lsn)
        assert positions == list(range(len(transaction.changes)))


def test_three_transactions_for_one_table_and_date_make_one_object_in_commit_order():
    objects = build_batch(assemble("disbursement"), "fintech_cdc", PREFIX, NOW)
    assert len(objects) == 1
    obj = objects[0]
    assert obj.key == "cdc/cdc_r01/loan_applications/event_date=2025-03-01/lsn_0000000002b3a418_0000000002b3a838.ndjson.gz"
    assert (obj.first_lsn, obj.last_lsn, obj.events) == (INSERT, NOOP, 3)
    events = decode(obj)
    assert [e["op"] for e in events] == ["c", "u", "u"]
    assert [e["flags"] for e in events] == [[], [], ["noop_update"]]
    keys = [(e["source"]["commit_lsn"], e["transaction"]["position"]) for e in events]
    assert keys == [(INSERT, 0), (UPDATE, 0), (NOOP, 0)]


def test_an_update_lands_in_its_inserts_partition_and_object():
    # The disbursement update has event_date from submission_ts, so it joins the insert's object.
    events = decode(build_batch(assemble("disbursement"), "fintech_cdc", PREFIX, NOW)[0])
    assert {e["event_date"] for e in events} == {"2025-03-01"}


def shifted(transaction, lsn_offset, submission_ts):
    """A copy of a captured loan_applications insert transaction, moved to another commit LSN and
    another event date (submission_ts decides it)."""
    (change_lsn, insert), = transaction.changes
    moved = dataclasses.replace(insert, new={**insert.new, "submission_ts": submission_ts})
    return dataclasses.replace(transaction, commit_lsn=transaction.commit_lsn + lsn_offset,
                               end_lsn=transaction.end_lsn + lsn_offset,
                               changes=((change_lsn + lsn_offset, moved),))


def test_the_same_table_on_two_dates_makes_two_objects_and_one_may_span_the_other():
    a = assemble("disbursement")[0]                                     # loan_applications, 2025-03-01
    b = shifted(a, 100, "2025-03-05T08:15:00.000000Z")                  # same table, 2025-03-05
    c = shifted(a, 200, "2025-03-01T09:00:00.000000Z")                  # back to 2025-03-01
    objects = by_key(build_batch([a, b, c], "fintech_cdc", PREFIX, NOW))
    assert sorted(o.event_date for o in objects.values()) == ["2025-03-01", "2025-03-05"]
    march_1 = next(o for o in objects.values() if o.event_date == "2025-03-01")
    march_5 = next(o for o in objects.values() if o.event_date == "2025-03-05")
    # the 1 March object covers both its transactions even though the 5 March one sits between them
    assert (march_1.first_lsn, march_1.last_lsn, march_1.events) == (a.commit_lsn, c.commit_lsn, 2)
    assert (march_5.first_lsn, march_5.last_lsn, march_5.events) == (b.commit_lsn, b.commit_lsn, 1)
    assert march_1.key.endswith(f"event_date=2025-03-01/lsn_{a.commit_lsn:016x}_{c.commit_lsn:016x}.ndjson.gz")


def test_a_repayment_with_no_payment_time_lands_under_its_due_date_with_the_flag():
    repayments = [o for o in build_batch(assemble("inserts_utc"), "fintech_cdc", PREFIX, NOW)
                  if o.table == "loan_repayments"]
    assert len(repayments) == 1 and repayments[0].event_date == "2025-04-02"
    assert decode(repayments[0])[0]["flags"] == ["event_time_fallback"]


def test_empty_transactions_and_an_empty_batch_make_no_objects():
    transactions = assemble("inserts_utc")
    assert build_batch([], "fintech_cdc", PREFIX, NOW) == []
    from cdc.transactions import TransactionAssembler
    from tests.helpers import begin_message, commit_message
    assembler = TransactionAssembler()
    assembler.feed(100, begin_message(500, 7))
    empty = assembler.feed(100, commit_message(500, 540))
    assert empty.changes == ()
    assert build_batch([empty], "fintech_cdc", PREFIX, NOW) == []
    # and an empty transaction among real ones changes nothing
    assert [o.key for o in build_batch([empty, *transactions], "fintech_cdc", PREFIX, NOW)] == \
           [o.key for o in build_batch(transactions, "fintech_cdc", PREFIX, NOW)]


# ---------------------------------------------------------------------------
# The bytes (contract section 2)
# ---------------------------------------------------------------------------

def test_the_object_is_gzipped_ndjson_with_lf_line_ends_no_bom_and_ascii():
    transactions = assemble("disbursement")
    obj = build_batch(transactions, "fintech_cdc", PREFIX, NOW)[0]
    assert obj.data[:2] == b"\x1f\x8b"
    text = gzip.decompress(obj.data)
    assert not text.startswith(b"\xef\xbb\xbf") and b"\r" not in text and text.isascii()
    lines = text.split(b"\n")
    assert lines[-1] == b"" and len(lines) == 4        # three events, each ended by one line feed
    expected = [to_line(e) for t in transactions for e in build_events(t, "fintech_cdc", NOW)]
    assert [line + b"\n" for line in lines[:-1]] == expected
    assert lines[0].startswith(b'{"envelope_version":1,"op":"c","source":{"db":"fintech_cdc"')


def test_the_gzip_header_carries_no_time_so_the_same_input_gives_the_same_bytes():
    first = build_batch(assemble("inserts_utc"), "fintech_cdc", PREFIX, NOW)
    again = build_batch(assemble("inserts_utc"), "fintech_cdc", PREFIX, NOW)
    for a, b in zip(first, again):
        assert a.data[4:8] == b"\x00\x00\x00\x00"       # the gzip MTIME field
        assert a.data == b.data


def test_a_replay_has_the_same_names_and_events_but_not_the_same_bytes():
    first = build_batch(assemble("inserts_utc"), "fintech_cdc", PREFIX, NOW)
    replay = build_batch(assemble("inserts_utc"), "fintech_cdc", PREFIX, LATER)
    assert [o.key for o in first] == [o.key for o in replay]
    assert all(a.data != b.data for a, b in zip(first, replay))   # ts_ms and ingested_at moved
    for a, b in zip(first, replay):
        assert [clock_free(e) for e in decode(a)] == [clock_free(e) for e in decode(b)]


def test_each_event_in_an_object_belongs_to_its_name():
    for obj in build_batch(assemble("inserts_utc") + assemble("disbursement"), "fintech_cdc", PREFIX, NOW):
        events = decode(obj)
        assert len(events) == obj.events
        assert {e["source"]["table"] for e in events} == {obj.table}
        assert {e["event_date"] for e in events} == {obj.event_date}
        assert events[0]["source"]["commit_lsn"] == obj.first_lsn
        assert events[-1]["source"]["commit_lsn"] == obj.last_lsn
        keys = [(e["source"]["commit_lsn"], e["transaction"]["position"]) for e in events]
        assert keys == sorted(keys)


def test_make_object_names_from_the_first_and_last_event():
    events = build_events(assemble("disbursement")[0], "fintech_cdc", NOW)
    obj = make_object(PREFIX, "loan_applications", "2025-03-01", events)
    assert obj.first_lsn == obj.last_lsn == INSERT and obj.events == 1


# ---------------------------------------------------------------------------
# The replay compare (M4-Q5, option B)
# ---------------------------------------------------------------------------

@pytest.fixture
def pair():
    first = build_batch(assemble("disbursement"), "fintech_cdc", PREFIX, NOW)[0]
    replay = build_batch(assemble("disbursement"), "fintech_cdc", PREFIX, LATER)[0]
    return first, replay


def regzip(events, final_newline=True):
    body = b"\n".join(json.dumps(e, separators=(",", ":")).encode() for e in events)
    return gzip.compress(body + (b"\n" if final_newline else b""), mtime=0)


def test_a_replay_with_a_later_clock_is_the_same_events(pair):
    first, replay = pair
    assert first.data != replay.data
    assert same_events(first.data, replay.data) and same_events(replay.data, first.data)
    assert same_events(first.data, first.data)


def test_any_other_difference_is_not_the_same_events(pair):
    first, _ = pair
    base = decode(first)

    def changed(mutate):
        events = json.loads(json.dumps(base))
        mutate(events)
        return regzip(events)

    assert not same_events(first.data, changed(lambda e: e[0]["after"].update(approved_amount=1)))
    assert not same_events(first.data, changed(lambda e: e[1]["before"].update(decision="declined")))
    assert not same_events(first.data, changed(lambda e: e[0].update(op="u")))
    assert not same_events(first.data, changed(lambda e: e[2].update(flags=[])))
    assert not same_events(first.data, changed(lambda e: e[0]["source"].update(commit_ts="2025-01-01T00:00:00.000000Z")))
    assert not same_events(first.data, changed(lambda e: e[0]["transaction"].update(position=5)))
    assert not same_events(first.data, changed(lambda e: e[0].update(event_date="2025-03-02")))
    assert not same_events(first.data, changed(lambda e: e.pop()))                  # a line missing
    assert not same_events(first.data, changed(lambda e: e.append(e[0])))           # a line added
    assert not same_events(first.data, changed(lambda e: e.reverse()))              # reordered


def test_a_clock_field_changing_alone_does_not_count(pair):
    first, _ = pair
    events = decode(first)
    for event in events:
        event["ts_ms"] += 99_999
        event["ingested_at"] = "2030-01-01T00:00:00.000000Z"
    assert same_events(first.data, regzip(events))


def test_an_existing_object_that_is_not_gzipped_ndjson_is_not_the_same(pair):
    first, _ = pair
    assert not same_events(b"", first.data)
    assert not same_events(b"not gzip at all", first.data)
    assert not same_events(first.data[:-8], first.data)                              # truncated gzip
    assert not same_events(gzip.compress(b"[]\n"), first.data)                       # JSON, not events
    assert not same_events(gzip.compress(b'{"op":"c"}\n'), first.data)               # no clock fields
    assert not same_events(regzip(decode(first), final_newline=False), first.data)   # no final line feed
    assert not same_events(gzip.compress(b"{broken\n"), first.data)
    for line in (b'"text"', b"5", b"null", b"true"):                                 # valid JSON, not an event
        assert not same_events(gzip.compress(line + b"\n"), first.data), line
    assert not same_events(gzip.compress(b"\xff\xfe\n"), first.data)               # not UTF-8


# ---------------------------------------------------------------------------
# land_object against the fake sink
# ---------------------------------------------------------------------------

def test_a_new_object_is_created(pair):
    first, _ = pair
    sink = MemorySink()
    assert land_object(sink, first) == "created"
    assert sink.objects == {first.key: first.data}


def test_a_replayed_object_is_recognised_and_the_original_is_left_alone(pair):
    first, replay = pair
    sink = MemorySink()
    land_object(sink, first)
    assert land_object(sink, replay) == "replayed"
    assert sink.objects == {first.key: first.data}       # never overwritten with the replay's bytes
    assert sink.reads == 1


def test_a_different_object_under_the_same_name_stops_the_consumer_and_is_not_overwritten(pair):
    first, replay = pair
    sink = MemorySink()
    land_object(sink, first)
    events = decode(replay)
    events[0]["after"]["approved_amount"] = 1
    forged = type(replay)(replay.key, replay.table, replay.event_date, replay.first_lsn, replay.last_lsn,
                          replay.events, regzip(events))
    with pytest.raises(ObjectMismatch) as raised:
        land_object(sink, forged)
    assert first.key in str(raised.value)
    assert sink.objects == {first.key: first.data}


def test_a_name_taken_by_garbage_stops_the_consumer(pair):
    first, _ = pair
    sink = MemorySink()
    sink.objects[first.key] = b"somebody else's bytes"
    with pytest.raises(ObjectMismatch):
        land_object(sink, first)
    assert sink.objects[first.key] == b"somebody else's bytes"


def test_a_failed_write_raises_and_leaves_nothing(pair):
    first, _ = pair
    sink = MemorySink()
    sink.fail_put = lambda attempt, key: True
    with pytest.raises(SinkError):
        land_object(sink, first)
    assert sink.objects == {}


def test_a_failed_read_during_the_compare_raises_instead_of_guessing(pair):
    first, replay = pair
    sink = MemorySink()
    land_object(sink, first)
    sink.fail_read = lambda key: True
    with pytest.raises(SinkError):
        land_object(sink, replay)
    assert sink.objects == {first.key: first.data}


# ---------------------------------------------------------------------------
# The fake follows the sink's rules (so the tests above mean something)
# ---------------------------------------------------------------------------

def test_the_fake_creates_once_and_never_overwrites():
    sink = MemorySink()
    assert sink.put_if_absent("a", b"one") is True
    assert sink.put_if_absent("a", b"two") is False
    assert sink.read("a") == b"one"
    assert sink.attempts == 2


def test_the_fake_fails_on_demand_by_attempt_number_and_stores_nothing_for_a_failure():
    sink = MemorySink()
    sink.fail_put = lambda attempt, key: attempt == 2
    assert sink.put_if_absent("a", b"1") is True
    with pytest.raises(SinkError):
        sink.put_if_absent("b", b"2")
    assert sink.put_if_absent("b", b"2") is True        # the third attempt works
    assert sorted(sink.objects) == ["a", "b"] and sink.attempts == 3


def test_reading_a_missing_object_is_a_sink_error():
    with pytest.raises(SinkError):
        MemorySink().read("nope")
