"""tests/test_bronze_events.py: G1 step 3, one landed object checked and reduced (bronze/events.py).

Every structural check has a case that breaks exactly it; each must stop with an EventError that names
the object and the event. The synthetic events are held to the real envelope first (Envelope accepts
them, to_line writes the same bytes), so a passing check is not a check against a toy format.
"""
import gzip
import json
from copy import deepcopy

import pytest

from bronze.errors import ContractError, EventError
from bronze.events import LSN_LIMIT, MAX_TRANSACTION_SIZE, check_event, event_hash, summarise_object
from bronze.landing import LandedObject
from cdc.columns import EXPECTED_COLUMNS
from cdc.envelope import Envelope, to_line
from tests.bronze_synth import DAY, FIRST_LSN, LSN_STEP, PREFIX, SOURCE_DB, encode, event, name

TABLES = tuple(EXPECTED_COLUMNS)
LAST_LSN = FIRST_LSN + 5 * LSN_STEP


def obj(table="merchants", date=DAY):
    return LandedObject(name(table, date, [event(table, "c", FIRST_LSN, 0, 1, date=date)]) , table, date, FIRST_LSN, LAST_LSN)


def valid(table="merchants", op="c"):
    return event(table, op, FIRST_LSN + LSN_STEP, 0, 1)


# ---------------------------------------------------------------------------
# the synthetic events are the real thing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table,op", [(t, "c") for t in TABLES] + [("loan_applications", "u")])
def test_a_synthetic_event_is_a_valid_envelope_and_encodes_like_the_consumer(table, op):
    e = event(table, op, FIRST_LSN, 0, 1, flags=["noop_update"] if op == "u" else [])
    assert Envelope.model_validate(e)
    assert encode([e]) == gzip.compress(to_line(Envelope.model_validate(e)), mtime=0)


@pytest.mark.parametrize("table,op", [(t, "c") for t in TABLES] + [("loan_applications", "u")])
def test_a_valid_event_passes_every_check(table, op):
    e = valid(table, op)
    assert check_event(e, obj(table)) == (FIRST_LSN + LSN_STEP, 0, 1, op)


# ---------------------------------------------------------------------------
# what breaks a check
# ---------------------------------------------------------------------------

def put(path, value):
    def apply(e):
        target = e
        for step in path[:-1]:
            target = target[step]
        target[path[-1]] = value
    return apply


def drop(path):
    def apply(e):
        target = e
        for step in path[:-1]:
            target = target[step]
        del target[path[-1]]
    return apply


def reorder(path):
    def apply(e):
        target = e
        for step in path[:-1]:
            target = target[step]
        target[path[-1]] = dict(reversed(list(target[path[-1]].items())))
    return apply


def rename_first_column(image):
    def apply(e):
        first = next(iter(e[image]))
        e[image] = {("renamed" if k == first else k): v for k, v in e[image].items()}
    return apply


def add_column(image):
    return lambda e: e[image].__setitem__("surprise", 1)


def drop_last_column(image):
    return lambda e: e[image].pop(list(e[image])[-1])


def make_update(e):
    e["op"], e["before"] = "u", dict(e["after"])


# name -> (table, op of the good event, edit that breaks the copy, text the error must contain)
BREAKS = {
    "a wrong envelope_version": ("merchants", "c", put(["envelope_version"], 2), "envelope_version is 2"),
    "envelope_version true": ("merchants", "c", put(["envelope_version"], True), "envelope_version is True"),
    "envelope_version as text": ("merchants", "c", put(["envelope_version"], "1"), "envelope_version is '1'"),
    "no envelope_version": ("merchants", "c", drop(["envelope_version"]), "missing envelope_version"),
    "an extra envelope field": ("merchants", "c", put(["surprise"], 1), "unexpected surprise"),
    "no flags field": ("merchants", "c", drop(["flags"]), "missing flags"),
    "op d": ("merchants", "c", put(["op"], "d"), "op is 'd'"),
    "op r": ("merchants", "c", put(["op"], "r"), "op is 'r'"),
    "op null": ("merchants", "c", put(["op"], None), "op is None"),
    "op a list": ("merchants", "c", put(["op"], ["c"]), "op is"),
    "an unknown table": ("merchants", "c", put(["source", "table"], "advances"), "not one of the five"),
    "a table that is not text": ("merchants", "c", put(["source", "table"], ["merchants"]), "not one of the five"),
    "a table that is not the objects": ("merchants", "c", put(["source", "table"], "risk_events"), "object name says merchants"),
    "an event_date that is not the objects": ("merchants", "c", put(["event_date"], "2025-03-02"), "object name says 2025-03-01"),
    "no source.change_lsn": ("merchants", "c", drop(["source", "change_lsn"]), "missing change_lsn"),
    "an extra source field": ("merchants", "c", put(["source", "surprise"], 1), "unexpected surprise"),
    "source that is not an object": ("merchants", "c", put(["source"], []), "source is not an object"),
    "no transaction.size": ("merchants", "c", drop(["transaction", "size"]), "missing size"),
    "transaction that is not an object": ("merchants", "c", put(["transaction"], None), "transaction is not an object"),
    "a negative commit_lsn": ("merchants", "c", put(["source", "commit_lsn"], -1), "commit_lsn -1"),
    "a commit_lsn of 2^63": ("merchants", "c", put(["source", "commit_lsn"], LSN_LIMIT), "commit_lsn"),
    "a float commit_lsn": ("merchants", "c", put(["source", "commit_lsn"], 45294944.0), "commit_lsn"),
    "a true commit_lsn": ("merchants", "c", put(["source", "commit_lsn"], True), "commit_lsn True"),
    "a text commit_lsn": ("merchants", "c", put(["source", "commit_lsn"], "5"), "commit_lsn '5'"),
    "a commit_lsn below the name's range": ("merchants", "c", put(["source", "commit_lsn"], FIRST_LSN - 1), "outside the object name's range"),
    "a commit_lsn above the name's range": ("merchants", "c", put(["source", "commit_lsn"], LAST_LSN + 1), "outside the object name's range"),
    "a position equal to the size": ("merchants", "c", put(["transaction", "position"], 1), "position 1"),
    "a negative position": ("merchants", "c", put(["transaction", "position"], -1), "position -1"),
    "a float position": ("merchants", "c", put(["transaction", "position"], 0.0), "position 0.0"),
    "a true position": ("merchants", "c", put(["transaction", "position"], True), "position True"),
    "a size of zero": ("merchants", "c", put(["transaction", "size"], 0), "size 0"),
    "a true size": ("merchants", "c", put(["transaction", "size"], True), "size True"),
    "a size nobody can have": ("merchants", "c", put(["transaction", "size"], MAX_TRANSACTION_SIZE + 1), "not plausible"),
    "an extra after column": ("merchants", "c", add_column("after"), "unexpected surprise"),
    "a missing after column": ("merchants", "c", drop_last_column("after"), "after columns differ"),
    "a renamed after column": ("merchants", "c", rename_first_column("after"), "missing merchant_id; unexpected renamed"),
    "after columns in another order": ("merchants", "c", reorder(["after"]), "different order"),
    "an after that is null": ("merchants", "c", put(["after"], None), "after is not an object"),
    "an after that is a list": ("merchants", "c", put(["after"], []), "after is not an object"),
    "an insert with a before image": ("merchants", "c", put(["before"], {"a": 1}), "an insert has a before image"),
    "an update on merchants": ("merchants", "c", lambda e: make_update(e), "only loan_applications is ever updated"),
    "an update on merchant_transactions": ("merchant_transactions", "c", lambda e: make_update(e), "only loan_applications"),
    "an update with no before image": ("loan_applications", "u", put(["before"], None), "an update has no before image"),
    "an update with an extra before column": ("loan_applications", "u", add_column("before"), "before columns differ"),
    "an update with a missing before column": ("loan_applications", "u", drop_last_column("before"), "before columns differ"),
    "an update with a renamed before column": ("loan_applications", "u", rename_first_column("before"), "before columns differ"),
    "an update with before columns in another order": ("loan_applications", "u", reorder(["before"]), "different order"),
    "an update whose before is a list": ("loan_applications", "u", put(["before"], []), "before is not an object"),
}


@pytest.mark.parametrize("label", BREAKS)
def test_each_break_stops_the_object_naming_where_and_what(label):
    table, op, edit, text = BREAKS[label]
    good = event(table, op, FIRST_LSN + LSN_STEP, 0, 1)
    other = event(table, op, FIRST_LSN + 2 * LSN_STEP, 0, 1)
    broken = deepcopy(good)
    edit(broken)
    landed = obj(table)
    with pytest.raises(EventError) as caught:
        summarise_object(landed, encode([other, broken]), source_db=SOURCE_DB)
    message = str(caught.value)
    assert landed.key in message and "event 2" in message, message
    assert text in message, message


@pytest.mark.parametrize("label", ["a wrong envelope_version", "an unknown table", "an extra after column"])
def test_an_event_error_is_a_contract_error(label):
    table, op, edit, _ = BREAKS[label]
    broken = valid(table, op)
    edit(broken)
    with pytest.raises(ContractError):
        summarise_object(obj(table), encode([broken]), source_db=SOURCE_DB)


def test_the_extra_column_message_says_what_to_do_about_a_schema_change():
    broken = valid()
    add_column("after")(broken)
    with pytest.raises(EventError, match="new migration, a CDC update and a new Bronze contract version"):
        summarise_object(obj(), encode([broken]), source_db=SOURCE_DB)


def test_the_order_of_the_envelope_fields_does_not_matter_but_the_order_of_columns_does():
    shuffled = dict(reversed(list(valid().items())))
    assert check_event(shuffled, obj()) == (FIRST_LSN + LSN_STEP, 0, 1, "c")


def test_a_non_object_event_is_refused():
    with pytest.raises(EventError, match="the event is not an object"):
        summarise_object(obj(), gzip.compress(b"[1]\n"), source_db=SOURCE_DB)


# ---------------------------------------------------------------------------
# objects that are not NDJSON
# ---------------------------------------------------------------------------

NOT_NDJSON = {
    "bytes that are not gzip": b"this is not gzip",
    "an empty object": gzip.compress(b""),
    "no final line feed": gzip.compress(json.dumps(valid()).encode()),
    "a line that is not json": gzip.compress(b"{not json}\n"),
    "a blank line": gzip.compress(b"\n"),
    "a truncated gzip stream": gzip.compress(encode([valid()]))[:20],
}


@pytest.mark.parametrize("label", NOT_NDJSON)
def test_an_object_that_is_not_gzipped_ndjson_of_events_stops_the_run(label):
    landed = obj()
    with pytest.raises(EventError) as caught:
        summarise_object(landed, NOT_NDJSON[label], source_db=SOURCE_DB)
    assert landed.key in str(caught.value) and "not valid gzipped NDJSON" in str(caught.value)


# ---------------------------------------------------------------------------
# what is kept
# ---------------------------------------------------------------------------

def test_a_summary_keeps_key_size_op_and_hash_of_each_event_in_order():
    events = [event("merchants", "c", FIRST_LSN, 0, 2), event("merchants", "c", FIRST_LSN, 1, 2, serial=1),
              event("merchants", "c", FIRST_LSN + LSN_STEP, 0, 1, serial=2)]
    summary = summarise_object(obj(), encode(events), source_db=SOURCE_DB)
    assert summary.obj == obj()
    assert summary.rows == [(FIRST_LSN, 0, 2, "c", event_hash(events[0])),
                            (FIRST_LSN, 1, 2, "c", event_hash(events[1])),
                            (FIRST_LSN + LSN_STEP, 0, 1, "c", event_hash(events[2]))]
    assert len({row[4] for row in summary.rows}) == 3


def test_text_with_non_ascii_characters_is_fine():
    e = valid()
    e["after"]["archetype"] = "caf\u00e9 \u2028"
    assert summarise_object(obj(), encode([e]), source_db=SOURCE_DB).rows[0][:4] == (FIRST_LSN + LSN_STEP, 0, 1, "c")


# ---------------------------------------------------------------------------
# the content hash
# ---------------------------------------------------------------------------

def test_the_hash_ignores_the_consumers_clock():
    a, b = valid(), valid()
    b["ts_ms"] += 99_999
    b["ingested_at"] = "2030-01-01T00:00:00.000000Z"
    assert event_hash(a) == event_hash(b)


def test_the_hash_ignores_the_order_of_keys_in_a_line():
    a = valid()
    assert event_hash(a) == event_hash(dict(reversed(list(a.items()))))


CHANGES = {
    "op": lambda e: e.__setitem__("op", "u"),
    "source.change_lsn": lambda e: e["source"].__setitem__("change_lsn", e["source"]["change_lsn"] + 1),
    "source.commit_ts": lambda e: e["source"].__setitem__("commit_ts", "2025-03-01T10:00:01.000000Z"),
    "source.db": lambda e: e["source"].__setitem__("db", "other"),
    "transaction.id": lambda e: e["transaction"].__setitem__("id", 1),
    "transaction.size": lambda e: e["transaction"].__setitem__("size", 2),
    "event_ts": lambda e: e.__setitem__("event_ts", "2025-03-01T09:00:00.000000Z"),
    "flags": lambda e: e.__setitem__("flags", ["noop_update"]),
    "before": lambda e: e.__setitem__("before", {"a": 1}),
    "an after value": lambda e: e["after"].__setitem__("merchant_id", "x"),
    "an after value from text to number": lambda e: e["after"].__setitem__("merchant_id", 0),
    "an after value from null to text": lambda e: e["after"].__setitem__("merchant_id", None),
}


@pytest.mark.parametrize("label", CHANGES)
def test_the_hash_sees_every_other_field(label):
    changed = valid()
    CHANGES[label](changed)
    assert event_hash(changed) != event_hash(valid())


def test_one_is_not_true_to_the_hash():
    a, b = valid(), valid()
    a["after"]["risk_score"], b["after"]["risk_score"] = 1, True
    assert event_hash(a) != event_hash(b)


# ---------------------------------------------------------------------------
# boundaries
# ---------------------------------------------------------------------------

def test_the_commit_lsn_limit_is_exactly_2_to_the_63_even_inside_the_names_range():
    wide = LandedObject("k", "merchants", DAY, LSN_LIMIT - 2, LSN_LIMIT + 2)
    assert check_event(event("merchants", "c", LSN_LIMIT - 1, 0, 1), wide)[0] == LSN_LIMIT - 1
    with pytest.raises(Exception, match="integer from 0 to 2\\^63 - 1"):
        check_event(event("merchants", "c", LSN_LIMIT, 0, 1), wide)


def test_the_commit_lsn_zero_is_allowed():
    zero = LandedObject("k", "merchants", DAY, 0, 5)
    assert check_event(event("merchants", "c", 0, 0, 1), zero)[0] == 0


def test_the_size_cap_is_inclusive():
    assert check_event(event("merchants", "c", FIRST_LSN, 0, MAX_TRANSACTION_SIZE), obj())[2] == MAX_TRANSACTION_SIZE
    with pytest.raises(Exception, match="not plausible"):
        check_event(event("merchants", "c", FIRST_LSN, 0, MAX_TRANSACTION_SIZE + 1), obj())


def test_the_hash_is_wide_enough_that_a_thousand_different_events_never_share_one():
    hashes = {event_hash(event("merchants", "c", FIRST_LSN, 0, 1, serial=n)) for n in range(1000)}
    assert len(hashes) == 1000
    assert max(hashes) > 2 ** 56
