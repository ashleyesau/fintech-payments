"""tests/test_cdc_slot_decoding.py: M2 against a live slot.

The decoder tests in test_cdc_decoder.py use captured bytes. These read a real slot to check
what the captured bytes cannot show: that the decoder agrees with the LSNs and transaction ids
the slot itself reports (which settles the contract's open `change_lsn`), and which position
the consumer may confirm. Needs wal_level=logical (decision D9); see tests/conftest.py.
"""
from collections import Counter

import psycopg
import pytest

from cdc.lsn import format_lsn, parse_lsn
from cdc.pgoutput import Begin, Commit, Decoder, Insert, Relation
from simulator import db
from simulator.runner import run_backfill
from tests.conftest import TABLES
from tests.helpers import peek_changes, settings_for


def merchant_row(merchant_id: str) -> str:
    return (f"INSERT INTO merchants (merchant_id, archetype, province, onboarded_at, snapshot_at) "
            f"VALUES ('{merchant_id}', 'tutor', 'gauteng', '2025-01-01 00:00+00', '2025-03-01 21:59+00')")


def transactions_in(reader, slot):
    """Peek the slot and group the decoded messages by transaction, with the slot's own LSNs."""
    decoder = Decoder()
    transactions, current = [], None
    for lsn, xid, data in peek_changes(reader, slot):
        message = decoder.decode(data)
        if isinstance(message, Begin):
            current = {"begin": message, "begin_lsn": parse_lsn(lsn), "xid": xid, "changes": [], "commit": None}
        elif isinstance(message, Commit):
            current["commit"], current["commit_row_lsn"] = message, parse_lsn(lsn)
            transactions.append(current)
        elif not isinstance(message, Relation):
            current["changes"].append((parse_lsn(lsn), message))
    return transactions


@pytest.mark.slow
def test_a_real_backfill_decodes_completely_and_agrees_with_the_slot(cdc_dsn, cdc_slot, tmp_path):
    writer = db.connect(cdc_dsn)
    summary = run_backfill(settings_for(tmp_path, days=45, count=100), writer)
    writer.close()
    with psycopg.connect(cdc_dsn, autocommit=True) as reader:
        transactions = transactions_in(reader, cdc_slot)

    inserts = Counter(m.relation.name for t in transactions for _, m in t["changes"] if isinstance(m, Insert))
    assert inserts == Counter({k: v for k, v in summary.rows_written.items() if k in TABLES and v})
    assert sum(len(t["changes"]) for t in transactions) == sum(inserts.values()), "only inserts in this run"

    previous_end = 0
    for t in transactions:
        begin, commit = t["begin"], t["commit"]
        assert begin.final_lsn == commit.commit_lsn
        assert begin.xid == t["xid"], "Begin.xid is the xid the slot reports"
        assert t["commit_row_lsn"] == commit.end_lsn, "the slot's lsn on a Commit row is the commit's end_lsn"
        assert commit.end_lsn > commit.commit_lsn > previous_end
        previous_end = commit.end_lsn

        change_lsns = [lsn for lsn, _ in t["changes"]]
        assert change_lsns == sorted(set(change_lsns)), "change LSNs are distinct and in order"
        assert change_lsns[0] == t["begin_lsn"], "the slot gives Begin the LSN of the first change"
        assert all(lsn < commit.commit_lsn for lsn in change_lsns), "change_lsn is below commit_lsn"


def test_confirming_the_commit_lsn_replays_the_transaction_but_the_end_lsn_does_not(cdc_dsn, cdc_slot):
    """Which position the consumer confirms. The contract's dedup key uses commit_lsn, but
    pg_replication_slot_advance must be given the Commit message's end_lsn."""
    with psycopg.connect(cdc_dsn, autocommit=True) as conn:
        for merchant_id in ("m-1", "m-2", "m-3"):
            conn.execute(merchant_row(merchant_id))
        first = transactions_in(conn, cdc_slot)[0]["commit"]

        conn.execute("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (cdc_slot, format_lsn(first.commit_lsn)))
        assert len(transactions_in(conn, cdc_slot)) == 3, "advancing to commit_lsn leaves the transaction in the slot"

        conn.execute("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (cdc_slot, format_lsn(first.end_lsn)))
        remaining = transactions_in(conn, cdc_slot)
        assert [t["changes"][0][1].new["merchant_id"] for t in remaining] == ["m-2", "m-3"]


def test_a_bounded_read_finishes_the_transaction_it_is_in(cdc_dsn, cdc_slot):
    """upto_nchanges is a floor, not a cap: a transaction is never cut."""
    with psycopg.connect(cdc_dsn, autocommit=True) as conn:
        with conn.transaction():
            for merchant_id in ("m-1", "m-2", "m-3", "m-4", "m-5"):
                conn.execute(merchant_row(merchant_id))
        conn.execute(merchant_row("m-6"))
        bounded = peek_changes(conn, cdc_slot, limit=2)
    decoder = Decoder()
    messages = [decoder.decode(data) for _, _, data in bounded]
    assert sum(isinstance(m, Insert) for m in messages) == 5, "all five inserts of the first transaction, not two"
    assert isinstance(messages[0], Begin) and isinstance(messages[-1], Commit)
