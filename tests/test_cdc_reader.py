"""tests/test_cdc_reader.py: M4 step 3, the reader, against a live slot.

Needs wal_level=logical (decision D9); see tests/conftest.py. The slot, the throwaway database and
the publication come from the `cdc_slot` fixture. The rows are plain merchant inserts, written in
transactions of chosen sizes, so the tests control exactly what the slot holds.

What cannot be done inside a normal test is a real lost slot (it needs a cluster-wide cap change and
a restart); that end-to-end case is M5. Here the lost-slot handling is checked with errors built
to carry the exact messages a real Postgres 16.15 gave when one was produced by hand (recorded in
DECISION_LOG, "M4 build notes").
"""
from collections import Counter

import psycopg
import pytest
from psycopg import errors as pg_errors
from psycopg import pq

from cdc.errors import SlotLost, SlotMissing
from cdc.lsn import format_lsn, parse_lsn
from cdc.reader import Reader, SlotStatus
from cdc.transactions import TransactionAssembler

PUBLICATION = "fintech_cdc"

# What Postgres said when a slot was lost: version 16 (checked by hand), and a later release (seen on GitHub's
# runner, where the first version of the reader failed to recognise it), plus a wording nobody has seen yet.
LOST_WORDINGS = [
    'can no longer get changes from replication slot "cdc_x"',
    'replication slot "cdc_x" cannot be advanced',
    'can no longer access replication slot "cdc_x"',
    'some future wording for a slot that is gone',
]
BACKWARDS = "cannot advance replication slot to 0/E726A68, minimum is 0/E726AA0"


def write_transactions(dsn, sizes):
    """One committed transaction per entry of `sizes`, each inserting that many merchant rows."""
    with psycopg.connect(dsn) as conn:
        for t, size in enumerate(sizes):
            with conn.transaction():
                for i in range(size):
                    conn.execute(
                        "INSERT INTO merchants (merchant_id, archetype, province, onboarded_at, snapshot_at) "
                        "VALUES (%s, 'tutor', 'gauteng', '2025-01-01 00:00+00', '2025-03-01 21:59+00')",
                        (f"m-{t}-{i}",))


def assemble(messages):
    """Whole transactions out of fetched (lsn, bytes) pairs, by the code the consumer will run."""
    assembler = TransactionAssembler()
    done = [t for lsn, data in messages if (t := assembler.feed(lsn, data)) is not None]
    assert not assembler.in_transaction, "a fetch must end on a Commit"
    return done


def kinds(messages):
    return Counter(chr(data[0]) for _, data in messages)


@pytest.fixture
def reader(cdc_dsn, cdc_slot):
    with Reader(cdc_dsn, cdc_slot, PUBLICATION) as r:
        yield r


def slot_position(cdc_dsn, slot):
    with psycopg.connect(cdc_dsn, autocommit=True) as c:
        return parse_lsn(c.execute("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = %s",
                                   (slot,)).fetchone()[0])


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------

def test_an_empty_slot_returns_nothing(reader):
    assert reader.fetch(10) == []


def test_a_bounded_fetch_returns_whole_transactions_and_ends_on_a_commit(cdc_dsn, reader):
    write_transactions(cdc_dsn, [3, 2, 4])
    everything = reader.fetch(1000)
    assert [len(t.changes) for t in assemble(everything)] == [3, 2, 4]
    assert kinds(everything)["B"] == kinds(everything)["C"] == 3 and kinds(everything)["I"] == 9
    first = reader.fetch(1)                  # a limit of 1 still returns the whole first transaction
    assert chr(first[0][1][0]) == "B" and [len(t.changes) for t in assemble(first)] == [3]
    # The bound is a floor on the messages returned (Postgres counts every message, Begin and Commit
    # included, not just row changes) and never splits a transaction: whatever the limit, the result
    # is a prefix of the whole stream that ends on a Commit, and is shorter than the limit only
    # when the slot has nothing more.
    for limit in (1, 2, 5, 7, 10, 13, 1000):
        got = reader.fetch(limit)
        assemble(got)
        assert got == everything[:len(got)], limit
        assert len(got) >= limit or got == everything, limit


def test_messages_come_in_slot_order_with_the_slots_own_lsns(cdc_dsn, reader):
    write_transactions(cdc_dsn, [2, 2])
    messages = reader.fetch(1000)
    lsns = [lsn for lsn, _ in messages]
    assert lsns == sorted(lsns)
    transactions = assemble(messages)
    # the Commit row carries end_lsn, the position to confirm (M2)
    assert lsns[-1] == transactions[-1].end_lsn and transactions[0].end_lsn < transactions[1].commit_lsn


def test_fetching_does_not_consume_anything(cdc_dsn, cdc_slot, reader):
    write_transactions(cdc_dsn, [2, 2])
    before = slot_position(cdc_dsn, cdc_slot)
    assert reader.fetch(1000) == reader.fetch(1000)
    assert slot_position(cdc_dsn, cdc_slot) == before


@pytest.mark.parametrize("limit", [0, -1])
def test_a_limit_below_one_is_refused_because_postgres_reads_zero_as_no_limit(reader, limit):
    with pytest.raises(ValueError, match="no limit at all"):
        reader.fetch(limit)


def test_timestamps_are_read_in_utc_whatever_the_connection_default_is(cdc_dsn, cdc_slot):
    write_transactions(cdc_dsn, [1])
    kolkata = cdc_dsn + " options='-c TimeZone=Asia/Kolkata'"
    with psycopg.connect(kolkata, autocommit=True) as control:     # a plain reader of that connection
        raw = control.execute(
            "SELECT data FROM pg_logical_slot_peek_binary_changes(%s, NULL, NULL, 'proto_version', '1', "
            "'publication_names', %s)", (cdc_slot, PUBLICATION)).fetchall()
    assert any(b"+05:30" in bytes(d) for (d,) in raw), "the control must show the zone, or this test proves nothing"
    with Reader(kolkata, cdc_slot, PUBLICATION) as r:
        messages = r.fetch(1000)
    inserts = [data for _, data in messages if data[:1] == b"I"]
    assert inserts and all(b"+00" in d and b"+05:30" not in d for d in inserts)


# ---------------------------------------------------------------------------
# confirm
# ---------------------------------------------------------------------------

def test_confirming_a_transactions_end_lsn_removes_it_and_only_it(cdc_dsn, cdc_slot, reader):
    write_transactions(cdc_dsn, [3, 2, 4])
    transactions = assemble(reader.fetch(1000))
    reader.confirm(transactions[0].end_lsn)
    assert slot_position(cdc_dsn, cdc_slot) == transactions[0].end_lsn
    again = assemble(reader.fetch(1000))
    assert [t.commit_lsn for t in again] == [t.commit_lsn for t in transactions[1:]]
    assert [len(t.changes) for t in again] == [2, 4]


def test_confirming_the_commit_lsn_leaves_the_transaction_in_the_slot(cdc_dsn, reader):
    # Why the consumer confirms end_lsn (M2): this call does what it is told, and the transaction stays.
    write_transactions(cdc_dsn, [3, 2])
    transactions = assemble(reader.fetch(1000))
    reader.confirm(transactions[0].commit_lsn)
    assert assemble(reader.fetch(1000))[0].commit_lsn == transactions[0].commit_lsn


def test_confirming_everything_leaves_nothing_to_fetch(cdc_dsn, reader):
    write_transactions(cdc_dsn, [3, 2])
    transactions = assemble(reader.fetch(1000))
    reader.confirm(transactions[-1].end_lsn)
    assert reader.fetch(1000) == []


def test_confirming_the_same_position_twice_is_harmless(cdc_dsn, cdc_slot, reader):
    write_transactions(cdc_dsn, [2])
    end = assemble(reader.fetch(1000))[0].end_lsn
    reader.confirm(end)
    reader.confirm(end)
    assert slot_position(cdc_dsn, cdc_slot) == end


def test_confirming_backwards_raises_postgres_error_and_the_slot_stays_put(cdc_dsn, cdc_slot, reader):
    write_transactions(cdc_dsn, [2, 2])
    transactions = assemble(reader.fetch(1000))
    reader.confirm(transactions[1].end_lsn)
    with pytest.raises(pg_errors.ObjectNotInPrerequisiteState, match="cannot advance replication slot"):
        reader.confirm(transactions[0].end_lsn)
    assert slot_position(cdc_dsn, cdc_slot) == transactions[1].end_lsn


def test_a_position_that_was_never_fetched_cannot_be_confirmed_so_unread_changes_cannot_be_skipped(cdc_dsn, cdc_slot, reader):
    # Postgres would accept a position past the end of the WAL and silently skip everything unread.
    write_transactions(cdc_dsn, [2, 2, 2])
    first = assemble(reader.fetch(1))[0]
    with psycopg.connect(cdc_dsn, autocommit=True) as c:
        wal_end = parse_lsn(c.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0])
    before = slot_position(cdc_dsn, cdc_slot)
    with pytest.raises(RuntimeError, match="confirming unread changes would lose them"):
        reader.confirm(wal_end)
    with pytest.raises(RuntimeError):
        reader.confirm(first.end_lsn + 1)
    assert slot_position(cdc_dsn, cdc_slot) == before
    assert [len(t.changes) for t in assemble(reader.fetch(1000))] == [2, 2, 2]   # nothing was lost


def test_a_new_reader_has_read_nothing_so_it_cannot_confirm(cdc_dsn, cdc_slot, reader):
    write_transactions(cdc_dsn, [2])
    end = assemble(reader.fetch(1000))[0].end_lsn
    with Reader(cdc_dsn, cdc_slot, PUBLICATION) as fresh:
        with pytest.raises(RuntimeError, match="only read up to"):
            fresh.confirm(end)


def test_a_new_reader_after_a_restart_resumes_from_the_confirmed_position(cdc_dsn, cdc_slot):
    write_transactions(cdc_dsn, [2, 3])
    with Reader(cdc_dsn, cdc_slot, PUBLICATION) as first:
        transactions = assemble(first.fetch(1000))
        first.confirm(transactions[0].end_lsn)
    with Reader(cdc_dsn, cdc_slot, PUBLICATION) as second:
        assert [len(t.changes) for t in assemble(second.fetch(1000))] == [3]


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

def test_status_of_a_healthy_slot(cdc_dsn, cdc_slot, reader):
    status = reader.status()
    with psycopg.connect(cdc_dsn, autocommit=True) as c:
        setting_mb = c.execute("SELECT setting::bigint FROM pg_settings WHERE name = 'max_slot_wal_keep_size'").fetchone()[0]
    expected_cap = None if setting_mb < 0 else setting_mb * 1024 * 1024
    assert status.wal_status == "reserved" and not status.lost
    assert status.cap_bytes == expected_cap
    assert status.confirmed_flush_lsn == slot_position(cdc_dsn, cdc_slot)
    assert status.restart_lsn is not None and status.restart_lsn <= status.confirmed_flush_lsn
    if expected_cap is not None:
        # measured from a WAL segment boundary, so it can run up to one 16 MB segment over the cap
        assert 0 < status.safe_wal_size <= expected_cap + 16 * 1024 * 1024


def test_status_follows_the_slot_as_it_is_confirmed(cdc_dsn, reader):
    write_transactions(cdc_dsn, [3])
    before = reader.status().confirmed_flush_lsn
    end = assemble(reader.fetch(1000))[0].end_lsn
    reader.confirm(end)
    assert reader.status().confirmed_flush_lsn == end > before


class FakeConnection:
    """Stands in for the psycopg connection to feed the reader rows and errors Postgres gave."""
    def __init__(self, row=None, error=None, wal_status="reserved"):
        self.row, self.error, self.calls = row, error, []
        self.wal_status = wal_status           # what the slot answers when asked whether it is lost
        self.asked_status = False
        self.info = type("Info", (), {"dbname": "fintech_cdc"})()

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        self.asked_status = sql.startswith("SELECT wal_status FROM pg_replication_slots")
        if self.asked_status:
            return self
        if self.error is not None:
            raise self.error
        return self

    def fetchone(self):
        return (self.wal_status,) if self.asked_status else self.row

    def fetchall(self):
        return self.row

    def close(self):
        pass


def pg_error(cls, primary):
    return cls(primary, info={pq.DiagnosticField.MESSAGE_PRIMARY: primary.encode(),
                              pq.DiagnosticField.SQLSTATE: b"55000"})


def test_status_of_a_lost_slot_has_no_safe_size(cdc_dsn, cdc_slot, reader):
    reader._conn = FakeConnection(row=("lost", None, "0/EA1D838", None, 16 * 1024 * 1024))
    status = reader.status()
    assert status.lost and status.safe_wal_size is None and status.restart_lsn is None
    assert status.cap_bytes == 16 * 1024 * 1024 and status.confirmed_flush_lsn == parse_lsn("0/EA1D838")


def test_status_of_an_unreserved_slot_can_be_negative(reader):
    reader._conn = FakeConnection(row=("unreserved", -97_413_328, "0/EA1D870", "0/EA1D838", 16 * 1024 * 1024))
    status = reader.status()
    assert status.wal_status == "unreserved" and status.safe_wal_size == -97_413_328 and not status.lost


def test_an_unlimited_cap_reads_as_none(reader):
    reader._conn = FakeConnection(row=("reserved", None, "0/EA1D870", "0/EA1D838", -1))
    assert reader.status().cap_bytes is None


# ---------------------------------------------------------------------------
# lost and missing slots
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("wording", LOST_WORDINGS)
def test_a_lost_slot_on_a_fetch_is_a_slotlost_that_says_what_to_do_whatever_postgres_called_it(reader, wording):
    reader._conn = FakeConnection(error=pg_error(pg_errors.ObjectNotInPrerequisiteState, wording), wal_status="lost")
    with pytest.raises(SlotLost) as raised:
        reader.fetch(10)
    message = str(raised.value)
    assert reader.slot_name in message and "cannot be repaired" in message
    assert "same seed" in message and "new slot name" in message and "cdc_r02" in message


@pytest.mark.parametrize("wording", LOST_WORDINGS)
def test_a_slot_lost_while_a_batch_was_uploading_is_a_slotlost_on_the_confirm_whatever_postgres_called_it(cdc_dsn, reader, wording):
    write_transactions(cdc_dsn, [1])
    end = assemble(reader.fetch(10))[0].end_lsn
    reader._conn = FakeConnection(error=pg_error(pg_errors.ObjectNotInPrerequisiteState, wording), wal_status="lost")
    with pytest.raises(SlotLost):
        reader.confirm(end)


@pytest.mark.parametrize("wording", LOST_WORDINGS + [BACKWARDS, "replication slot is active"])
def test_the_slots_own_state_decides_so_the_same_error_on_a_slot_that_is_not_lost_is_postgres_own_error(cdc_dsn, reader, wording):
    write_transactions(cdc_dsn, [1])
    end = assemble(reader.fetch(10))[0].end_lsn
    for state in ("reserved", "extended", "unreserved"):
        reader._conn = FakeConnection(error=pg_error(pg_errors.ObjectNotInPrerequisiteState, wording), wal_status=state)
        with pytest.raises(pg_errors.ObjectNotInPrerequisiteState):
            reader.confirm(end)
        with pytest.raises(pg_errors.ObjectNotInPrerequisiteState):
            reader.fetch(10)


def test_the_slot_is_asked_only_when_the_error_is_of_the_kind_that_can_mean_lost(cdc_dsn, reader):
    write_transactions(cdc_dsn, [1])
    reader._conn = FakeConnection(error=pg_error(pg_errors.UndefinedObject, 'replication slot "cdc_x" does not exist'))
    with pytest.raises(SlotMissing):
        reader.fetch(10)
    assert not any(sql.startswith("SELECT wal_status") for sql, _ in reader._conn.calls)
    reader._conn = FakeConnection(row=[])
    assert reader.fetch(10) == [] and not any(sql.startswith("SELECT wal_status") for sql, _ in reader._conn.calls)


def test_a_missing_slot_is_a_clear_error_for_every_call(cdc_dsn):
    with Reader(cdc_dsn, "cdc_no_such_slot", PUBLICATION) as r:
        for call in (lambda: r.fetch(10), r.status):
            with pytest.raises(SlotMissing) as raised:
                call()
            assert "cdc_no_such_slot" in str(raised.value) and "does not exist" in str(raised.value)
            assert psycopg.conninfo.conninfo_to_dict(cdc_dsn)["dbname"] in str(raised.value)
        r._read_to = 10 ** 6
        with pytest.raises(SlotMissing):
            r.confirm(10)


def test_a_confirm_that_lands_somewhere_other_than_asked_is_refused(cdc_dsn, reader):
    write_transactions(cdc_dsn, [1])
    end = assemble(reader.fetch(10))[0].end_lsn
    reader._conn = FakeConnection(row=(format_lsn(end + 1000),))
    with pytest.raises(RuntimeError, match="the slot is at"):
        reader.confirm(end)


# ---------------------------------------------------------------------------
# connections
# ---------------------------------------------------------------------------

def test_an_unreachable_database_raises_the_connection_error_and_a_closed_reader_cannot_be_used(cdc_dsn, cdc_slot):
    with pytest.raises(psycopg.OperationalError):
        Reader("host=localhost port=1 dbname=x user=sim password=sim connect_timeout=2", cdc_slot, PUBLICATION)
    r = Reader(cdc_dsn, cdc_slot, PUBLICATION)
    r.close()
    with pytest.raises(psycopg.OperationalError):
        r.fetch(10)
