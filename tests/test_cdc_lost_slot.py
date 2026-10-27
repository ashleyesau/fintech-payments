"""tests/test_cdc_lost_slot.py: M5, a real lost slot and a stalled consumer (CDC Plan section 11: "a deliberately
tiny max_slot_wal_keep_size with a stalled consumer: the invalid slot is detected and reported clearly").

These run on a throwaway cluster started with a 16 MB cap (tests/pgcluster.py), because a `-c` flag overrides
ALTER SYSTEM and the shared test cluster must never lose a slot. WAL is added with pg_logical_emit_message,
which pins WAL without any decodable change, and CHECKPOINT then removes what the slot no longer holds.
The premises (the cap, the slot's wal_status at each stage) are asserted, so a test cannot pass vacuously.
"""
import io
import os

import psycopg
import pytest

from cdc.errors import SlotLost
from cdc.lag import execute as lag_report
from cdc.main import execute
from cdc.reader import Reader
from tests.conftest import MIGRATIONS, new_cdc_slot
from tests.fakes import MemorySink
from tests.pgcluster import CAP_BYTES, small_cap_cluster
from tests.test_cdc_consumer_live import expected_keys, write_chain5
from tests.test_cdc_failures import keys_landed, waiting
from tests.test_cdc_main import live_settings
from tests.test_cdc_reader import slot_position

PUBLICATION = "fintech_cdc"


@pytest.fixture(autouse=True)
def default_cdc_settings(monkeypatch):
    for name in [n for n in os.environ if n.startswith("CDC_")]:
        monkeypatch.delenv(name)


@pytest.fixture(scope="module")
def cluster():
    with small_cap_cluster(MIGRATIONS) as dsn:
        yield dsn


@pytest.fixture
def slot(cluster):
    with new_cdc_slot(cluster) as name:
        yield name


# ---------------------------------------------------------------------------
# making WAL pile up behind a slot nobody is reading
# ---------------------------------------------------------------------------

def emit_wal(dsn, megabytes):
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SELECT pg_logical_emit_message(true, 'fill', repeat('a', 1000000)) FROM generate_series(1, %s)",
                     (megabytes,))


def slot_state(dsn, slot):
    with psycopg.connect(dsn, autocommit=True) as conn:
        return conn.execute("SELECT wal_status, safe_wal_size FROM pg_replication_slots WHERE slot_name = %s",
                            (slot,)).fetchone()


def fill_until(dsn, slot, reached, limit_mb=120):
    for _ in range(limit_mb):
        status, safe = slot_state(dsn, slot)
        if reached(status, safe):
            return status, safe
        emit_wal(dsn, 1)
    raise AssertionError(f"the slot never reached the stage wanted: {slot_state(dsn, slot)}")


def lose_slot(dsn, slot):
    """Pile up WAL well past the cap and checkpoint until Postgres has removed what the slot held."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        for _ in range(6):
            emit_wal(dsn, 20)
            conn.execute("CHECKPOINT")
            conn.execute("CHECKPOINT")
            if slot_state(dsn, slot)[0] == "lost":
                return
    raise AssertionError(f"the slot was not lost: {slot_state(dsn, slot)}")


class CountingReader(Reader):
    fetches = 0

    def fetch(self, limit):
        CountingReader.fetches += 1
        return super().fetch(limit)


def run(dsn, slot, sink=None, **settings):
    """The consumer, once, as the command line runs it. Returns (exit code, stdout, stderr, sink, fetches)."""
    sink = sink if sink is not None else MemorySink()
    CountingReader.fetches = 0
    out, err = io.StringIO(), io.StringIO()
    code = execute(live_settings(dsn, slot, **settings), False,
                   reader_factory=lambda s: CountingReader(s.dsn, s.slot_name, s.publication),
                   sink_factory=lambda s: sink, out=out, err=err, handle_signals=False)
    return code, out.getvalue(), err.getvalue(), sink, CountingReader.fetches


# ---------------------------------------------------------------------------
# the cluster and a healthy slot
# ---------------------------------------------------------------------------

def test_the_small_cap_is_in_force_and_a_fresh_slot_is_silent(cluster, slot):
    with Reader(cluster, slot, PUBLICATION) as reader:
        status = reader.status()
    assert status.cap_bytes == CAP_BYTES and status.wal_status == "reserved" and not status.lost
    code, out, err, sink, fetches = run(cluster, slot)
    assert code == 0 and err == "" and out.strip().splitlines()[-1] == "done: nothing was waiting in the slot"


# ---------------------------------------------------------------------------
# a stalled consumer: the warnings
# ---------------------------------------------------------------------------

def test_a_stalled_slot_is_warned_under_half_the_cap_and_then_under_a_tenth(cluster, slot):
    half, tenth = CAP_BYTES / 2, CAP_BYTES / 10
    fill_until(cluster, slot, lambda status, safe: safe is not None and tenth < safe < half)
    code, out, err, _, _ = run(cluster, slot)
    assert code == 0                                                       # a warning does not stop the consumer
    lines = err.strip().splitlines()
    assert len(lines) == 1 and lines[0].startswith("WARNING: slot ") and "under half of the 16 MB cap" in lines[0]
    assert "MB of WAL headroom left" in lines[0] and "cannot be repaired" in lines[0]

    fill_until(cluster, slot, lambda status, safe: safe is not None and 0 < safe < tenth)
    code, out, err, _, _ = run(cluster, slot)
    lines = err.strip().splitlines()
    assert code == 0 and len(lines) == 1 and "under a tenth of the 16 MB cap" in lines[0]


def test_an_unreserved_slot_is_warned_and_is_still_readable_so_what_is_waiting_lands(cluster, slot):
    write_chain5(cluster, [1, 2])
    waiting_now = waiting(cluster, slot)
    fill_until(cluster, slot, lambda status, safe: status == "unreserved")
    assert slot_state(cluster, slot)[0] == "unreserved"                    # the stage this test is about
    code, out, err, sink, _ = run(cluster, slot)
    assert code == 0
    assert err.strip().splitlines()[0].startswith("WARNING: slot ") and "unreserved" in err and "not lost yet" in err
    assert keys_landed(sink) == expected_keys(waiting_now)                 # past the cap, not yet lost: it was all read
    assert slot_position(cluster, slot) == waiting_now[-1].end_lsn


# ---------------------------------------------------------------------------
# a lost slot
# ---------------------------------------------------------------------------

def test_a_lost_slot_stops_the_consumer_with_exit_4_and_the_plain_message_before_it_reads_anything(cluster, slot):
    write_chain5(cluster, [1, 2])
    lose_slot(cluster, slot)
    status, safe = slot_state(cluster, slot)
    assert status == "lost" and safe is None                               # the stage this test is about

    code, out, err, sink, fetches = run(cluster, slot)
    assert code == 4
    lines = err.strip().splitlines()
    assert len(lines) == 1 and lines[0].startswith("error: ") and f"Slot {slot} is lost" in lines[0]
    assert "cannot be repaired" in lines[0] and "same seed" in lines[0] and "cdc_r02" in lines[0]
    assert "done:" not in out
    assert fetches == 0 and sink.attempts == 0                             # stopped at the status check, nothing read


def test_the_reader_turns_postgres_own_lost_slot_error_into_slotlost(cluster, slot):
    write_chain5(cluster, [1])
    lose_slot(cluster, slot)
    with Reader(cluster, slot, PUBLICATION) as reader:
        assert reader.status().lost
        with pytest.raises(SlotLost, match="cannot be repaired"):
            reader.fetch(10)                                               # the real "can no longer get changes" error


def test_a_slot_lost_while_a_batch_is_uploading_is_reported_at_the_confirm_as_exit_4(cluster, slot):
    write_chain5(cluster, [1, 2, 3])

    class LosesTheSlotDuringTheFirstUpload(MemorySink):
        done = False

        def put_if_absent(self, key, data):
            if not self.done:
                self.done = True
                lose_slot(cluster, slot)                                   # the cap is passed while the flush is under way
            return super().put_if_absent(key, data)

    sink = LosesTheSlotDuringTheFirstUpload()
    code, out, err, sink, fetches = run(cluster, slot, sink, upload_workers=1)
    assert code == 4 and fetches == 1                                      # it had read the batch before the slot was lost
    assert len(sink.objects) == 9                                          # the flush finished
    lines = err.strip().splitlines()
    assert len(lines) == 1 and f"Slot {slot} is lost" in lines[0] and "cannot be repaired" in lines[0]
    assert slot_state(cluster, slot)[0] == "lost" and "done:" not in out


def lag(dsn, slot):
    out, err = io.StringIO(), io.StringIO()
    code = lag_report(live_settings(dsn, slot), None, out=out, err=err)
    return code, out.getvalue(), err.getvalue()


def test_the_lag_report_follows_a_stalled_slot_down_through_every_level_to_lost_and_exits_four(cluster, slot):
    half, tenth = CAP_BYTES / 2, CAP_BYTES / 10
    code, out, err = lag(cluster, slot)
    assert code == 0 and err == "" and "cap 16.0 MB" in out and "level               healthy" in out

    fill_until(cluster, slot, lambda status, safe: safe is not None and tenth < safe < half)
    code, out, err = lag(cluster, slot)
    assert code == 0 and "under half the cap is left" in out and ": reserved" in out

    fill_until(cluster, slot, lambda status, safe: safe is not None and 0 < safe < tenth)
    code, out, err = lag(cluster, slot)
    assert code == 0 and "under a tenth of the cap is left" in out

    fill_until(cluster, slot, lambda status, safe: status == "unreserved")
    code, out, err = lag(cluster, slot)
    assert code == 0 and ": unreserved" in out and "past the cap but not lost yet" in out and "headroom -" in out

    lose_slot(cluster, slot)
    code, out, err = lag(cluster, slot)
    assert code == 4 and ": lost" in out and "LOST" in out and "cannot be repaired" in out
    assert "held by the slot    none" in out

