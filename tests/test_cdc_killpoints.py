"""tests/test_cdc_killpoints.py: M5, forced restarts give the same landed result (CDC Plan section 11, check 4).

Two slots on one database see exactly the same changes. One is drained by an uninterrupted consumer.
The other is drained by a consumer that is killed over and over: attempt k may perform k operations
(a fetch, an upload or a confirm) and dies before the next one, then a fresh reader, as a restarted
process would have, tries again with one more allowed. So every point between two operations of a
whole run is a kill point, in turn. The restarted run must land exactly what the uninterrupted one
did, and end at the same slot position. One more test kills a real process with SIGKILL in the middle
of a flush, against a directory sink (tests/file_sink.py).
"""
import contextlib
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest
from psycopg import conninfo

from cdc.consumer import Consumer
from cdc.dedup import dedup, events_in, incomplete_transactions, key
from cdc.landing import same_events
from cdc.reader import Reader
from tests.conftest import new_cdc_slot
from tests.fakes import MemorySink, SimulatedCrash
from tests.file_sink import FileSink
from tests.test_cdc_consumer_live import expected_keys, write_chain5
from tests.test_cdc_reader import assemble, slot_position

PUBLICATION = "fintech_cdc"
PREFIX = "cdc/cdc_r01/"
NOW = datetime(2026, 10, 27, 9, 41, 7, 490000, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parent.parent
DAYS = [1, 1, 2, 3, 3, 4, 5, 5, 6, 7]      # ten transactions over seven event dates: several batches, many objects


class Budget:
    """How many operations one attempt may perform before it is killed."""
    def __init__(self, allowed):
        self.allowed, self.used, self.killed_before = allowed, 0, None
        self._lock = threading.Lock()

    def tick(self, what):
        with self._lock:
            self.used += 1
            if self.used > self.allowed:
                self.killed_before = what
                raise SimulatedCrash(f"killed before {what} number {self.used}")


class MortalReader(Reader):
    def __init__(self, dsn, slot, budget):
        super().__init__(dsn, slot, PUBLICATION)
        self.budget = budget

    def fetch(self, limit):
        self.budget.tick("fetch")
        return super().fetch(limit)

    def confirm(self, end_lsn):
        self.budget.tick("confirm")
        super().confirm(end_lsn)


class MortalSink:
    """Wraps a sink; an upload that has been allowed really lands, and the process may die right after."""
    def __init__(self, sink, budget):
        self.sink, self.budget = sink, budget

    def put_if_absent(self, key, data):
        self.budget.tick("upload")
        return self.sink.put_if_absent(key, data)

    def read(self, key):
        return self.sink.read(key)


def consumer(reader, sink, batch_size, workers, database="fintech_cdc"):
    return Consumer(reader, sink, database=database, prefix=PREFIX, batch_size=batch_size, workers=workers,
                    clock=lambda: NOW)


def drain(dsn, slot, sink, batch_size=12, workers=1, database="fintech_cdc"):
    with Reader(dsn, slot, PUBLICATION) as reader:
        return consumer(reader, sink, batch_size, workers, database).run(follow=False)


def drain_with_kills(dsn, slot, sink, batch_size=12, workers=1):
    """Kill and restart until a run completes. An attempt may perform `allowed` operations and then dies before
    the next one. The allowance grows by one after each death, so within a batch every point is tried in turn:
    before the fetch, before each upload, before the confirm. When an attempt got the slot to move (a batch
    confirmed) the allowance goes back to zero, so the next batch gets the same treatment. Returns what each
    dead attempt was killed before."""
    died, allowed = [], 0
    while True:
        budget, before = Budget(allowed), slot_position(dsn, slot)
        try:
            with MortalReader(dsn, slot, budget) as reader:
                consumer(reader, MortalSink(sink, budget), batch_size, workers).run(follow=False)
            return died
        except SimulatedCrash:
            died.append(budget.killed_before)
            allowed = 0 if slot_position(dsn, slot) != before else allowed + 1
            assert len(died) < 500, "the consumer never finishes"


@pytest.fixture
def two_slots(cdc_dsn):
    """Two slots created before any data, so both see every change that follows."""
    with new_cdc_slot(cdc_dsn) as first:
        second = first + "_b"
        with psycopg.connect(cdc_dsn, autocommit=True) as control:
            control.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (second,))
            try:
                yield first, second
            finally:
                control.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s",
                                (second,))


def waiting_transactions(dsn, slot):
    with Reader(dsn, slot, PUBLICATION) as peek:
        return assemble(peek.fetch(10 ** 9))


def assert_same_landing(dsn, slots, uninterrupted, killed):
    a, b = slots
    assert set(killed.objects) == set(uninterrupted.objects)              # the same names: batch boundaries did not move
    for name in uninterrupted.objects:
        assert same_events(uninterrupted.objects[name], killed.objects[name]), name
    assert slot_position(dsn, a) == slot_position(dsn, b)                 # both ended at the same confirmed position


# ---------------------------------------------------------------------------
# killed at every point between two operations
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("workers", [1, 8])
def test_a_consumer_killed_and_restarted_at_every_point_lands_exactly_what_an_uninterrupted_one_does(cdc_dsn, two_slots, workers):
    a, b = two_slots
    write_chain5(cdc_dsn, DAYS)
    transactions = waiting_transactions(cdc_dsn, a)
    assert len(transactions) == len(DAYS)
    uninterrupted, killed = MemorySink(), MemorySink()
    clean = drain(cdc_dsn, a, uninterrupted, workers=workers)
    assert clean.batches >= 4 and len(uninterrupted.objects) >= 12

    died = drain_with_kills(cdc_dsn, b, killed, workers=workers)

    assert len(died) >= 30 and {"fetch", "upload", "confirm"} <= set(died)   # killed before every kind of operation
    assert_same_landing(cdc_dsn, (a, b), uninterrupted, killed)
    events = [e for data in killed.objects.values() for e in events_in(data)]
    assert {key(e) for e in dedup(events)} == expected_keys(transactions)    # nothing lost
    assert incomplete_transactions(dedup(events)) == {}
    assert len(events) == len(transactions) * 3                               # and, with the same names, nothing doubled


def test_the_kill_points_include_dying_after_every_upload_of_a_batch_and_before_its_confirm(cdc_dsn, two_slots):
    a, b = two_slots
    write_chain5(cdc_dsn, [1, 2, 3])
    killed = MemorySink()
    start = slot_position(cdc_dsn, b)
    with MortalReader(cdc_dsn, b, Budget(1 + 9)) as reader:                    # one fetch, nine uploads, then the confirm
        with pytest.raises(SimulatedCrash, match="before confirm"):
            consumer(reader, MortalSink(killed, reader.budget), 10_000, 1).run(follow=False)
    assert len(killed.objects) == 9                                           # everything had landed
    assert slot_position(cdc_dsn, b) == start                                 # and the slot had not moved
    with Reader(cdc_dsn, b, PUBLICATION) as reader:
        summary = consumer(reader, killed, 10_000, 1).run(follow=False)
    assert (summary.objects_created, summary.objects_replayed) == (0, 9)


# ---------------------------------------------------------------------------
# a real process, killed
# ---------------------------------------------------------------------------

def landed_objects(directory):
    return [p for p in Path(directory).rglob("*.ndjson.gz")] if Path(directory).exists() else []


def test_a_real_process_killed_with_sigkill_in_the_middle_of_a_flush_loses_nothing(cdc_dsn, two_slots, tmp_path):
    a, b = two_slots
    write_chain5(cdc_dsn, DAYS)
    transactions = waiting_transactions(cdc_dsn, a)
    info = conninfo.conninfo_to_dict(cdc_dsn)
    uninterrupted = MemorySink()
    drain(cdc_dsn, a, uninterrupted, batch_size=10_000, database=info["dbname"])   # one batch of 21 objects
    start = slot_position(cdc_dsn, b)

    env = {k: v for k, v in os.environ.items() if not k.startswith("CDC_")}
    env.update({"PYTHONPATH": str(ROOT), "CDC_HOST": info["host"], "CDC_PORT": str(info["port"]), "CDC_DB": info["dbname"],
                "CDC_USER": info["user"], "CDC_PASSWORD": info["password"], "CDC_SLOT_NAME": b, "CDC_UPLOAD_WORKERS": "1",
                "FILE_SINK_DELAY": "0.4"})
    landed = tmp_path / "landed"
    command = [sys.executable, "-W", "ignore", "-m", "tests.consume_with_file_sink", str(landed)]

    process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    deadline = time.time() + 60
    while len(landed_objects(landed)) < 3 and process.poll() is None and time.time() < deadline:
        time.sleep(0.05)
    assert process.poll() is None, "the process finished before it could be killed: " + process.stderr.read().decode()
    process.send_signal(signal.SIGKILL)
    process.wait(timeout=30)
    process.stdout.close(), process.stderr.close()
    assert process.returncode == -signal.SIGKILL

    sink = FileSink(landed)
    partly = len(sink.objects())
    assert 3 <= partly < len(uninterrupted.objects)                           # killed with the flush half done
    assert slot_position(cdc_dsn, b) == start                                 # the slot had not moved

    env["FILE_SINK_DELAY"] = "0"
    result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    total = len(uninterrupted.objects)
    assert f"({total - partly} objects new, {partly} already there)" in result.stdout    # the ones that landed are recognised
    landed_now = {name.split("/", 2)[2]: data for name, data in sink.objects().items()}    # without cdc/<slot>/
    expected_now = {name.split("/", 2)[2]: data for name, data in uninterrupted.objects.items()}
    assert set(landed_now) == set(expected_now)
    for name, data in expected_now.items():
        assert same_events(data, landed_now[name]), name
    assert slot_position(cdc_dsn, b) == slot_position(cdc_dsn, a)
    events = [e for data in landed_now.values() for e in events_in(data)]
    assert {key(e) for e in dedup(events)} == expected_keys(transactions)
