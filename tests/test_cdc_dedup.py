"""tests/test_cdc_dedup.py: reading landed events back, the dedup key and the completeness rule
(contract section 7). No database."""
import gzip
import json

import pytest

from cdc.dedup import dedup, events_in, incomplete_transactions, key
from cdc.landing import build_batch
from tests.test_cdc_envelope import NOW, assemble

LATER = NOW.replace(minute=59)


def landed(transactions, now):
    return [e for o in build_batch(transactions, "fintech_cdc", "cdc/cdc_r01/", now) for e in events_in(o.data)]


def event(commit_lsn, position, size, **extra):
    return {"source": {"commit_lsn": commit_lsn}, "transaction": {"position": position, "size": size}, **extra}


def test_events_in_reads_an_object_back_in_order():
    obj = build_batch(assemble("disbursement"), "fintech_cdc", "cdc/cdc_r01/", NOW)[0]
    got = events_in(obj.data)
    assert [e["op"] for e in got] == ["c", "u", "u"]
    with pytest.raises(ValueError):
        events_in(gzip.compress(b'{"a":1}'))          # no final line feed


def test_the_key_is_the_commit_lsn_and_the_position_in_the_transaction():
    assert key(event(7, 3, 5)) == (7, 3)


def test_a_replayed_batch_dedups_to_the_original_events():
    transactions = assemble("inserts_utc") + assemble("disbursement")
    first, replay = landed(transactions, NOW), landed(transactions, LATER)
    assert len(first) == len(replay) == 11
    once = dedup(first + replay)
    assert [key(e) for e in once] == [key(e) for e in first]
    assert once == first                                   # the first copy is kept
    assert len({key(e) for e in once}) == 11


def test_dedup_keeps_distinct_changes_that_share_a_commit_or_a_position():
    events = [event(1, 0, 2), event(1, 1, 2), event(2, 0, 1), event(1, 0, 2)]
    assert [key(e) for e in dedup(events)] == [(1, 0), (1, 1), (2, 0)]


def test_a_complete_transaction_has_every_position_once_across_objects():
    assert incomplete_transactions(landed(assemble("inserts_utc"), NOW)) == {}
    assert incomplete_transactions([]) == {}


def test_a_transaction_with_a_missing_position_is_reported():
    events = landed(assemble("inserts_utc"), NOW)
    lsn = events[0]["source"]["commit_lsn"]
    kept = [e for e in events if not (e["source"]["commit_lsn"] == lsn and e["transaction"]["position"] == 1)]
    problems = incomplete_transactions(kept)
    assert list(problems) == [lsn] and "[0, 2] of 3" in problems[lsn]


def test_events_that_disagree_about_the_size_are_reported():
    problems = incomplete_transactions([event(1, 0, 2), event(1, 1, 3)])
    assert "disagree" in problems[1]


def test_duplicates_do_not_count_as_extra_positions():
    events = landed(assemble("inserts_utc"), NOW)
    assert incomplete_transactions(events + events) == {}
