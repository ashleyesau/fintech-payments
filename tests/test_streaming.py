"""tests/test_streaming.py: pacing, row ordering and the streaming run.

The pacing and ordering tests need no database. The streaming runs write to the throwaway
database from conftest.py and skip when Postgres is not reachable. Nothing here really
sleeps: the pacer takes a fake clock.
"""
import logging
from datetime import date, datetime, timedelta

import numpy as np
import pytest

from simulator import streaming
from simulator.common import SAST, end_of_day
from simulator.runner import DayResult, MerchantDay, RunError, RunSettings, run_backfill
from simulator.state import load_checkpoint
from simulator.streaming import (
    LAG_WARNING_SECONDS,
    Pacer,
    TimedRow,
    build_timeline,
    replay_day,
    run_stream,
    write_row,
)
from tests.helpers import LONG_RUN, START, reset, settings_for, table_contents

DAY = date(2025, 6, 2)


def at(hour, minute=0, second=0, day=DAY) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=SAST)


class FakeClock:
    """A clock that only moves when something sleeps or `spend` is called."""

    def __init__(self):
        self.now = 100.0
        self.slept: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds

    def spend(self, seconds: float) -> None:
        self.now += seconds


def pacer_with(clock: FakeClock, speed: float = 3600) -> Pacer:
    return Pacer(speed, clock=clock.clock, sleep=clock.sleep)


# ---------------------------------------------------------------------------
# Pacer
# ---------------------------------------------------------------------------

def test_each_gap_is_slept_divided_by_the_speed_factor():
    clock = FakeClock()
    pacer = pacer_with(clock, speed=3600)
    pacer.start(at(0))
    assert pacer.wait_until(at(1)) == 0.0   # one simulated hour is one real second
    assert pacer.wait_until(at(1, 30)) == 0.0
    assert clock.slept == pytest.approx([1.0, 0.5])


def test_a_row_at_the_same_time_as_the_last_does_not_sleep():
    clock = FakeClock()
    pacer = pacer_with(clock)
    pacer.start(at(0))
    pacer.wait_until(at(2))
    pacer.wait_until(at(2))
    assert clock.slept == pytest.approx([2.0])


def test_a_slow_write_makes_the_next_row_late_instead_of_pushing_everything_back():
    clock = FakeClock()
    pacer = pacer_with(clock)
    pacer.start(at(0))
    pacer.wait_until(at(1))           # sleeps 1.0 s
    clock.spend(5.0)                  # the write took 5 s
    late = pacer.wait_until(at(1, 30))  # due 0.5 s after the first row
    assert late == pytest.approx(4.5)
    assert clock.slept == pytest.approx([1.0])  # no sleep: the writer is catching up


def test_a_late_writer_catches_up_to_the_original_schedule():
    clock = FakeClock()
    pacer = pacer_with(clock)
    pacer.start(at(0))
    pacer.wait_until(at(1))   # due at 1.0 s: sleeps
    clock.spend(5.0)          # a slow write: the clock is now at 6.0 s
    lateness = [pacer.wait_until(at(h, m)) for h, m in [(1, 30), (2, 0), (4, 0)]]
    assert lateness == pytest.approx([4.5, 4.0, 2.0])  # due at 1.5, 2.0 and 4.0 s
    assert pacer.wait_until(at(8)) == 0.0              # due at 8.0 s: back on schedule
    assert clock.slept == pytest.approx([1.0, 2.0])


def test_an_earlier_timestamp_after_a_later_one_is_not_reported_as_late():
    clock = FakeClock()
    pacer = pacer_with(clock)
    pacer.start(at(0))
    pacer.wait_until(at(3))
    assert pacer.wait_until(at(1)) == 0.0
    assert clock.slept == pytest.approx([3.0])


def test_the_pacer_needs_a_positive_speed_and_a_start():
    with pytest.raises(ValueError):
        Pacer(0)
    with pytest.raises(RuntimeError):
        Pacer(60).wait_until(at(1))


# ---------------------------------------------------------------------------
# build_timeline
# ---------------------------------------------------------------------------

def transaction(tx_id, when) -> dict:
    return {"transaction_id": tx_id, "created_at": when}


def risk(alert_id, entity_id, when) -> dict:
    return {"alert_id": alert_id, "entity_id": entity_id, "action_ts": when}


def day_result(*merchant_days) -> DayResult:
    return DayResult(day=DAY, merchant_days=list(merchant_days), churned=0, onboarded=0, snapshots=[])


def test_rows_from_different_merchants_are_interleaved_by_time():
    a = MerchantDay("a", [transaction("a1", at(9)), transaction("a2", at(11))], [], {})
    b = MerchantDay("b", [transaction("b1", at(10)), transaction("b2", at(12))], [], {})
    order = [item.row["transaction_id"] for item in build_timeline(day_result(a, b))]
    assert order == ["a1", "b1", "a2", "b2"]


def test_a_risk_event_for_a_transaction_travels_with_it():
    tx = transaction("t1", at(9))
    alert = risk("r1", "t1", at(9, 0, 1))
    timeline = build_timeline(day_result(MerchantDay("a", [tx], [alert], {})))
    assert [(i.kind, i.row["transaction_id"]) for i in timeline] == [("transaction", "t1")]
    assert timeline[0].attached == [alert]


def test_a_merchant_level_risk_event_is_its_own_row_at_its_own_time():
    tx = transaction("t1", at(9))
    merchant_alert = risk("r2", "m-1", at(9, 30))
    timeline = build_timeline(day_result(MerchantDay("a", [tx], [merchant_alert], {})))
    assert [(i.kind, i.when) for i in timeline] == [("transaction", at(9)), ("risk", at(9, 30))]
    assert timeline[0].attached == []


def lending_rows(application_at, update_at, repayment_at) -> dict:
    return {
        "loan_applications": [{"application_id": "app", "decision_ts": application_at}],
        "loan_application_updates": [{"application_id": "app", "disbursed_at": update_at}],
        "loan_repayments": [{"repayment_id": "rep", "payment_ts": repayment_at}],
    }


def test_an_application_comes_before_its_disbursement_and_that_before_a_repayment_at_the_same_instant():
    when = at(10)
    timeline = build_timeline(day_result(MerchantDay("a", [], [], lending_rows(when, when, when))))
    assert [i.kind for i in timeline] == ["application", "update", "repayment"]


def test_an_unpaid_repayment_with_no_payment_time_falls_in_the_morning_collection_window():
    rows = lending_rows(at(15), at(16), None)
    timeline = build_timeline(day_result(MerchantDay("a", [], [], rows)))
    repayment = next(i for i in timeline if i.kind == "repayment")
    assert repayment.when == at(8)
    assert timeline[0] is repayment


def test_every_row_appears_exactly_once():
    tx = [transaction(f"t{i}", at(9, i)) for i in range(5)]
    alerts = [risk("r1", "t2", at(9, 2)), risk("r2", "m", at(9, 10))]
    md = MerchantDay("a", tx, alerts, lending_rows(at(11), at(12), at(7)))
    timeline = build_timeline(day_result(md, MerchantDay("b", [transaction("b0", at(8))], [], {})))
    rows = [i.row for i in timeline] + [r for i in timeline for r in i.attached]
    assert len(rows) == 5 + 2 + 3 + 1
    assert timeline == sorted(timeline, key=lambda i: (i.when, streaming._RANK[i.kind]))


# ---------------------------------------------------------------------------
# write_row and replay_day with a recording writer
# ---------------------------------------------------------------------------

class RecordingWriter:
    def __init__(self):
        self.calls: list[tuple] = []

    def write_events(self, events, risk_rows):
        self.calls.append(("events", events[0]["transaction_id"], [r["alert_id"] for r in risk_rows]))

    def write(self, stream, rows):
        self.calls.append(("write", stream, len(rows)))

    def disburse(self, update):
        self.calls.append(("disburse", update["application_id"]))

    def write_snapshots(self, rows):
        self.calls.append(("snapshots", len(rows)))

    def end_of_day(self):
        self.calls.append(("end_of_day",))


def test_each_kind_of_row_goes_to_its_writer_call():
    writer = RecordingWriter()
    alert = risk("r1", "t1", at(9))
    write_row(writer, TimedRow(at(9), "transaction", transaction("t1", at(9)), [alert]))
    write_row(writer, TimedRow(at(9), "risk", risk("r2", "m", at(9))))
    write_row(writer, TimedRow(at(9), "application", {"application_id": "app"}))
    write_row(writer, TimedRow(at(9), "update", {"application_id": "app"}))
    write_row(writer, TimedRow(at(9), "repayment", {"repayment_id": "rep"}))
    assert writer.calls == [
        ("events", "t1", ["r1"]), ("write", "risk_events", 1), ("write", "loan_applications", 1),
        ("disburse", "app"), ("write", "loan_repayments", 1),
    ]


def test_replay_writes_rows_in_time_order_then_snapshots_at_the_end_of_the_day():
    clock = FakeClock()
    writer = RecordingWriter()
    a = MerchantDay("a", [transaction("a1", at(9)), transaction("a2", at(15))], [], {})
    b = MerchantDay("b", [transaction("b1", at(12))], [], {})
    result = DayResult(day=DAY, merchant_days=[a, b], churned=0, onboarded=0,
                       snapshots=[{"merchant_id": "a"}, {"merchant_id": "b"}])
    replay_day(result, writer, pacer_with(clock))
    assert [c[1] for c in writer.calls[:3]] == ["a1", "b1", "a2"]
    assert writer.calls[3:] == [("snapshots", 2), ("end_of_day",)]
    # 09:00, 12:00, 15:00 and 23:59 on the day, at 3,600x: the whole day takes 23.98 s.
    assert sum(clock.slept) == pytest.approx((end_of_day(DAY) - at(0)).total_seconds() / 3600)


def test_a_quiet_day_still_takes_a_day_to_pass_and_writes_its_snapshots():
    clock = FakeClock()
    writer = RecordingWriter()
    result = DayResult(day=DAY, merchant_days=[], churned=0, onboarded=0, snapshots=[{"merchant_id": "a"}])
    replay_day(result, writer, pacer_with(clock))
    assert writer.calls == [("snapshots", 1), ("end_of_day",)]
    assert sum(clock.slept) == pytest.approx(86340 / 3600)


def test_a_writer_that_falls_behind_logs_a_warning_and_still_writes_every_row(caplog):
    clock = FakeClock()

    class SlowWriter(RecordingWriter):
        def write_events(self, events, risk_rows):
            clock.spend(30.0)  # each write costs 30 real seconds
            super().write_events(events, risk_rows)

    writer = SlowWriter()
    rows = [transaction(f"t{i}", at(9, i)) for i in range(4)]  # a minute apart: 0.017 s of schedule each
    result = day_result(MerchantDay("a", rows, [], {}))
    with caplog.at_level(logging.WARNING, logger="simulator"):
        replay_day(result, writer, pacer_with(clock))
    assert [c[1] for c in writer.calls if c[0] == "events"] == ["t0", "t1", "t2", "t3"]
    assert any("behind the schedule" in r.message and "none dropped" in r.message for r in caplog.records)


def test_an_on_time_writer_logs_no_warning(caplog):
    clock = FakeClock()
    rows = [transaction("t0", at(9)), transaction("t1", at(10))]
    with caplog.at_level(logging.WARNING, logger="simulator"):
        replay_day(day_result(MerchantDay("a", rows, [], {})), RecordingWriter(), pacer_with(clock))
    assert not caplog.records
    assert LAG_WARNING_SECONDS == 1.0


# ---------------------------------------------------------------------------
# Streaming against Postgres
# ---------------------------------------------------------------------------

def stream_settings(tmp_path, count=30) -> RunSettings:
    return RunSettings(START, None, count, checkpoint_path=str(tmp_path / "checkpoint.json"))


def test_streaming_after_a_backfill_writes_the_same_rows_as_backfilling_those_days(conn, tmp_path):
    count = 30
    run_backfill(settings_for(tmp_path, days=103, count=count), conn)
    expected = table_contents(conn)
    reset(conn)
    (tmp_path / "checkpoint.json").unlink()

    run_backfill(settings_for(tmp_path, days=100, count=count), conn)
    clock = FakeClock()
    summary = run_stream(stream_settings(tmp_path, count), conn, 3, 3600, pacer=pacer_with(clock))

    assert summary.resumed and summary.first_day == START + timedelta(days=100)
    assert summary.last_day == START + timedelta(days=102) and summary.days == 3
    assert summary.rows_written["merchant_transactions"] > 0
    assert table_contents(conn) == expected
    assert sum(clock.slept) == pytest.approx(3 * 86340 / 3600, rel=0.05)  # about 72 s of schedule
    assert load_checkpoint(str(tmp_path / "checkpoint.json")).last_day == START + timedelta(days=102)


@pytest.mark.slow
def test_streaming_with_loans_in_flight_matches_backfill(conn, tmp_path, monkeypatch, uninterrupted):
    from simulator import lending

    for state in ("growing", "stable"):
        monkeypatch.setitem(lending._PROPENSITY, state, lending._PROPENSITY[state] * 10)
    count = LONG_RUN["count"]
    run_backfill(settings_for(tmp_path, days=LONG_RUN["days"] - 4, count=count), conn)
    run_stream(stream_settings(tmp_path, count), conn, 4, 3600, pacer=pacer_with(FakeClock()))
    assert table_contents(conn) == uninterrupted


@pytest.mark.slow
def test_a_stream_that_stops_part_way_through_a_day_resumes_without_duplicates(conn, tmp_path, monkeypatch, uninterrupted):
    from simulator import lending

    class Stop(Exception):
        pass

    class StoppingPacer(Pacer):
        """Raises on the Nth wait of the first streamed day, between two row commits."""
        def __init__(self, stop_after):
            clock = FakeClock()
            super().__init__(3600, clock=clock.clock, sleep=clock.sleep)
            self.remaining = stop_after

        def wait_until(self, sim_time):
            self.remaining -= 1
            if self.remaining == 0:
                raise Stop
            return super().wait_until(sim_time)

    for state in ("growing", "stable"):
        monkeypatch.setitem(lending._PROPENSITY, state, lending._PROPENSITY[state] * 10)
    count = LONG_RUN["count"]
    run_backfill(settings_for(tmp_path, days=LONG_RUN["days"] - 4, count=count), conn)
    with pytest.raises(Stop):
        run_stream(stream_settings(tmp_path, count), conn, 4, 3600, pacer=StoppingPacer(stop_after=40))
    half_written = table_contents(conn)["merchant_transactions"]
    assert 0 < len(half_written) < len(uninterrupted["merchant_transactions"])
    assert load_checkpoint(str(tmp_path / "checkpoint.json")).last_day == START + timedelta(days=LONG_RUN["days"] - 5)

    np.random.seed(555)
    run_stream(stream_settings(tmp_path, count), conn, 4, 3600, pacer=pacer_with(FakeClock()))
    assert table_contents(conn) == uninterrupted


def test_streaming_with_no_checkpoint_and_a_used_database_is_refused(conn, tmp_path):
    np.random.seed(0)
    run_backfill(settings_for(tmp_path, days=20, count=20), conn)
    (tmp_path / "checkpoint.json").unlink()
    with pytest.raises(RunError, match="empty database"):
        run_stream(stream_settings(tmp_path, 20), conn, 1, 3600, pacer=pacer_with(FakeClock()))


def test_streaming_under_different_flags_than_the_checkpoint_is_refused(conn, tmp_path):
    np.random.seed(0)
    run_backfill(settings_for(tmp_path, days=20, count=20), conn)
    run = RunSettings(START, None, 20, enable_risk_events=False, checkpoint_path=str(tmp_path / "checkpoint.json"))
    with pytest.raises(RunError, match="flags"):
        run_stream(run, conn, 1, 3600, pacer=pacer_with(FakeClock()))


def test_a_stream_needs_at_least_one_day(conn, tmp_path):
    with pytest.raises(RunError, match="at least 1"):
        run_stream(stream_settings(tmp_path), conn, 0, 3600)


def test_a_fresh_stream_starts_from_the_start_date_with_an_empty_database(conn, tmp_path):
    np.random.seed(0)
    summary = run_stream(stream_settings(tmp_path, 100), conn, 2, 3600, pacer=pacer_with(FakeClock()))
    assert summary.resumed is False and summary.first_day == START and summary.days == 2
    assert load_checkpoint(str(tmp_path / "checkpoint.json")).last_day == START + timedelta(days=1)
