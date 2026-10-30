"""tests/test_measure_bronze_scale.py: scripts/measure_bronze_scale.py, the P6 measurement at 11M events.

The 11M run itself is a one-off (scripts/ is not in CI). What is tested here is the script's own logic: the statistics,
every threshold on both sides, every hard check passing and failing, and one whole measurement at 5 transactions."""
import copy
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import measure_bronze_scale as scale  # noqa: E402

BIG = "merchant_transactions"


def facts(**changes):
    """The facts of a clean measurement of 5,500 transactions, with some replaced."""
    base = {
        "transactions": 5_500, "tx_size": 2_000, "merchants_per_tx": 50, "events_landed": 11_000_000,
        "objects_landed": 11_000, "batch_rows": 100_000,
        "phases": {"write: pass 1": {"seconds": 1000.0, "peak_mb_so_far": None},
                   "write: pass 2": {"seconds": 1500.0, "peak_mb_so_far": 1500}},
        "appended": {"merchants": 275_000, "loan_applications": 0, "loan_repayments": 0, "risk_events": 0, BIG: 10_725_000},
        "landed": {"merchants": 275_000, "loan_applications": 0, "loan_repayments": 0, "risk_events": 0, BIG: 10_725_000},
        "batches": {"merchants": 3, "loan_applications": 0, "loan_repayments": 0, "risk_events": 0, BIG: 108},
        "read_back": {"merchants": 275_000, "loan_applications": 0, "loan_repayments": 0, "risk_events": 0, BIG: 10_725_000},
        "marker_written": True, "files_created": 900,
        "second_run": {"appended": 0, "created": 0, "marker_written": False, "catalog_copied": False,
                       "unchanged": {"files": True, "snapshots": True, "catalog_bytes": True, "bucket_objects": True}},
        "check": {"problems": [], "rows": {"merchants": 275_000, "loan_applications": 0, "loan_repayments": 0,
                                           "risk_events": 0, BIG: 10_725_000}},
        "peak_mb": 1_800,
        "appends": [{"table": BIG, "rows": 100_000, "seconds": 2.0} for _ in range(108)],
        "samples": [[i * 10.0, "write: pass 2", int((1_000 + i * 0.5) * 1e6), i * 100_000] for i in range(1, 109)],
    }
    base.update(changes)
    return base


# ---------------------------------------------------------------------------
# the statistics
# ---------------------------------------------------------------------------

def test_the_slope_is_the_least_squares_line():
    assert scale.slope([(0, 1), (1, 3), (2, 5)]) == pytest.approx(2.0)
    assert scale.slope([(0, 5), (1, 5), (2, 5)]) == pytest.approx(0.0)
    assert scale.slope([(0, 10), (1, 5)]) == pytest.approx(-5.0)
    assert scale.slope([(0, 0), (1, 1), (2, 1), (3, 3)]) == pytest.approx(0.9)     # a noisy line, not just the end points


def test_the_slope_needs_two_different_x():
    assert scale.slope([]) is None and scale.slope([(1, 2)]) is None and scale.slope([(1, 2), (1, 9)]) is None


def test_the_decile_ratio_is_the_last_tenth_over_the_first_tenth():
    assert scale.decile_ratio([1.0] * 10 + [3.0]) is not None
    assert scale.decile_ratio([1.0] * 10) == 1.0
    assert scale.decile_ratio([1.0] * 5 + [2.0] * 5 + [4.0] * 10) == pytest.approx(4.0 / 1.0)
    assert scale.decile_ratio([2.0, 2.0] + [3.0] * 16 + [6.0, 6.0]) == pytest.approx(3.0)      # the tenth is two of twenty values
    assert scale.decile_ratio(list(range(1, 101))) == pytest.approx(sum(range(91, 101)) / sum(range(1, 11)))


def test_the_decile_ratio_needs_ten_values_and_a_nonzero_start():
    assert scale.decile_ratio([1.0] * 9) is None and scale.decile_ratio([]) is None
    assert scale.decile_ratio([0.0] * 10 + [5.0] * 10) is None


def test_memory_now_is_what_the_process_holds_now_and_falls_when_it_lets_go():
    now = scale.current_memory_bytes()
    assert 10_000_000 < now < 64_000_000_000
    held = bytearray(300_000_000)
    for page in range(0, len(held), 4096):
        held[page] = 1
    during = scale.current_memory_bytes()
    assert during > now + 250_000_000
    del held
    assert scale.current_memory_bytes() < during - 200_000_000          # the peak would not fall


def test_the_sampler_takes_a_sample_on_entry_and_on_exit_and_labels_each_with_the_phase_and_rows():
    with scale.Sampler(1_000) as sampler:
        sampler.phase, sampler.rows_done = "middle", 42
    assert [(phase, rows) for _, phase, _, rows in sampler.samples] == [("start", 0), ("middle", 42)]
    assert all(memory > 1_000_000 and seconds >= 0 for seconds, _, memory, _ in sampler.samples)


def test_the_sampler_keeps_sampling_at_its_interval():
    import time
    with scale.Sampler(0.02) as sampler:
        time.sleep(0.3)
    assert 5 <= len(sampler.samples) <= 40
    assert [s[0] for s in sampler.samples] == sorted(s[0] for s in sampler.samples)


# ---------------------------------------------------------------------------
# what the facts mean
# ---------------------------------------------------------------------------

def test_a_clean_measurement_passes_every_hard_check_and_raises_no_flag():
    verdict = scale.evaluate(facts())
    assert len(verdict["hard"]) == 12 and all(verdict["hard"].values()), verdict["hard"]
    assert verdict["flags"] == []
    figures = verdict["figures"]
    assert figures["commit seconds, first tenth against last tenth (merchant_transactions)"] == "x1.00"
    assert figures["memory growth in the big table's pass 2, MB per million rows"] == "5.0"
    assert figures["peak process memory, MB"] == "1,800"
    assert figures["files the write created (Class A on GCS), with the marker, the copy and a listing"] == "903 of 5,000"
    assert figures["the write on the Air (estimate, sandbox seconds x 3.1), minutes"] == "129"


@pytest.mark.parametrize("name, changes", [
    (("every landed change was appended", "the appended rows are the landed events",
      "the rows read back equal the rows appended"), {"appended": {**facts()["appended"], BIG: 10_724_999}}),
    ("every landed change was appended", {"landed": {**facts()["landed"], "merchants": 274_999}}),
    ("every landed change was appended", {"appended": {**facts()["appended"], BIG: 10_724_999},
                                          "landed": {**facts()["landed"], BIG: 10_724_999},
                                          "read_back": {**facts()["read_back"], BIG: 10_724_999},
                                          "events_landed": 10_999_999}),
    ("the appended rows are the landed events", {"events_landed": 11_000_001}),
    ("the rows read back equal the rows appended", {"read_back": {**facts()["read_back"], BIG: 10_725_001}}),
    ("the first run wrote a marker", {"marker_written": False}),
    ("the second run appended nothing and created nothing", {"second_run": {**facts()["second_run"], "appended": 1}}),
    ("the second run appended nothing and created nothing", {"second_run": {**facts()["second_run"], "created": 1}}),
    ("the second run wrote no marker and copied no catalog", {"second_run": {**facts()["second_run"], "marker_written": True}}),
    ("the second run wrote no marker and copied no catalog", {"second_run": {**facts()["second_run"], "catalog_copied": True}}),
    ("the second run changed no file", {"second_run": {**facts()["second_run"], "unchanged": {**facts()["second_run"]["unchanged"], "files": False}}}),
    ("the second run changed no snapshot", {"second_run": {**facts()["second_run"], "unchanged": {**facts()["second_run"]["unchanged"], "snapshots": False}}}),
    ("the second run changed no catalog byte", {"second_run": {**facts()["second_run"], "unchanged": {**facts()["second_run"]["unchanged"], "catalog_bytes": False}}}),
    ("the second run changed no bucket object", {"second_run": {**facts()["second_run"], "unchanged": {**facts()["second_run"]["unchanged"], "bucket_objects": False}}}),
    ("the check found no problem", {"check": {**facts()["check"], "problems": ["risk_events: 1 keys more than once"]}}),
    ("the check counted every landed event", {"check": {**facts()["check"], "rows": {**facts()["check"]["rows"], BIG: 10_724_999}}}),
])
def test_each_hard_check_fails_on_its_own(name, changes):
    hard = scale.evaluate(facts(**changes))["hard"]
    assert [n for n, ok in hard.items() if not ok] == (list(name) if isinstance(name, tuple) else [name])


def test_the_expected_counts_follow_the_transactions_and_their_shape():
    small = facts(transactions=5, events_landed=10_000,
                  appended={"merchants": 250, "loan_applications": 0, "loan_repayments": 0, "risk_events": 0, BIG: 9_750},
                  landed={"merchants": 250, "loan_applications": 0, "loan_repayments": 0, "risk_events": 0, BIG: 9_750},
                  read_back={"merchants": 250, "loan_applications": 0, "loan_repayments": 0, "risk_events": 0, BIG: 9_750},
                  check={"problems": [], "rows": {"merchants": 250, "loan_applications": 0, "loan_repayments": 0,
                                                  "risk_events": 0, BIG: 9_750}})
    assert all(scale.evaluate(small)["hard"].values())
    assert not all(scale.evaluate({**small, "tx_size": 1_000})["hard"].values())
    assert not all(scale.evaluate({**small, "merchants_per_tx": 40})["hard"].values())


def slow_commits(factor):
    return [{"table": BIG, "rows": 100_000, "seconds": 1.0} for _ in range(20)] + \
           [{"table": BIG, "rows": 100_000, "seconds": factor} for _ in range(20)] + \
           [{"table": "merchants", "rows": 275_000, "seconds": 500.0}]


def test_commits_that_slow_down_are_flagged_above_twice_and_not_at_twice():
    flagged = scale.evaluate(facts(appends=slow_commits(2.2)))["flags"]
    assert len(flagged) == 1 and flagged[0].startswith("commits slow down: the last tenth takes x") and "x2.20" in flagged[0]
    assert scale.evaluate(facts(appends=slow_commits(2.0)))["flags"] == []
    assert scale.evaluate(facts(appends=slow_commits(1.0)))["flags"] == []


def test_only_the_big_tables_commits_are_timed_for_the_slowdown():
    appends = [{"table": "merchants", "rows": 1, "seconds": 1.0}] * 20 + [{"table": "merchants", "rows": 1, "seconds": 9.0}] * 20
    verdict = scale.evaluate(facts(appends=appends))
    assert verdict["flags"] == []
    assert verdict["figures"]["commit seconds, first tenth against last tenth (merchant_transactions)"] is None


def growing(per_million):
    return [[i * 10.0, "write: pass 2", int((1_000 + per_million * i * 0.1) * 1e6), i * 100_000] for i in range(1, 109)]


def test_memory_that_grows_with_the_table_is_flagged_above_fifteen_per_million_rows():
    flagged = scale.evaluate(facts(samples=growing(600)))["flags"]
    assert len(flagged) == 1 and flagged[0].startswith("memory grows with the table: 600.0 MB per million rows")
    assert scale.evaluate(facts(samples=growing(16)))["flags"][0].startswith("memory grows with the table: 16.0")
    assert scale.evaluate(facts(samples=growing(15)))["flags"] == []
    assert scale.evaluate(facts(samples=growing(0)))["flags"] == []


def test_the_growth_uses_only_pass_2_samples_with_rows_appended_and_needs_a_million_rows():
    other_phase = [[10.0 * i, "second run", int((1_000 + 900 * i) * 1e6), i * 100_000] for i in range(1, 109)]
    assert scale.evaluate(facts(samples=other_phase))["flags"] == []
    before_rows = [[10.0 * i, "write: pass 2", int((1_000 + 900 * i) * 1e6), 0] for i in range(1, 109)]
    assert scale.evaluate(facts(samples=before_rows))["figures"]["memory growth in the big table's pass 2, MB per million rows"] is None
    short = [[10.0 * i, "write: pass 2", int((1_000 + 900 * i) * 1e6), i * 90_000] for i in range(1, 11)]       # 0.81 million rows
    assert scale.evaluate(facts(samples=short))["figures"]["memory growth in the big table's pass 2, MB per million rows"] is None
    span = [[10.0 * i, "write: pass 2", int((1_000 + 900 * i) * 1e6), i * 100_000] for i in range(1, 12)]        # exactly 1.0 million rows
    assert scale.evaluate(facts(samples=span))["figures"]["memory growth in the big table's pass 2, MB per million rows"] == "9000.0"


def test_the_growth_flag_is_strictly_above_fifteen(monkeypatch):
    monkeypatch.setattr(scale, "slope", lambda points: 15.0)
    assert scale.evaluate(facts())["flags"] == []
    monkeypatch.setattr(scale, "slope", lambda points: 15.001)
    assert len(scale.evaluate(facts())["flags"]) == 1


def test_samples_before_any_row_was_appended_do_not_enter_the_growth():
    flat = [[10.0 * i, "write: pass 2", 1_000_000_000, i * 100_000] for i in range(1, 109)]
    early = [[1.0, "write: pass 2", 100_000_000, 0] for _ in range(20)]
    verdict = scale.evaluate(facts(samples=early + flat))
    assert verdict["flags"] == []
    assert verdict["figures"]["memory growth in the big table's pass 2, MB per million rows"] == "0.0"


def test_peak_memory_is_flagged_above_four_thousand_megabytes_only():
    assert scale.evaluate(facts(peak_mb=4_001))["flags"] == ["peak memory 4,001 MB is above 4,000 MB, half of the Air's 8 GiB"]
    assert scale.evaluate(facts(peak_mb=4_000))["flags"] == []


def test_the_class_a_figure_counts_the_files_plus_three_and_is_flagged_above_five_thousand():
    assert scale.evaluate(facts(files_created=4_997))["flags"] == []
    flagged = scale.evaluate(facts(files_created=4_998))["flags"]
    assert flagged == ["the write needs about 5,001 Class A operations, more than the 5,000 a month"]


def test_the_air_estimate_is_the_two_passes_times_the_p5_ratio():
    figures = scale.evaluate(facts())["figures"]
    assert figures["the write on the Air (estimate, sandbox seconds x 3.1), minutes"] == "129"
    other = facts(phases={"write: pass 1": {"seconds": 600.0, "peak_mb_so_far": None},
                          "write: pass 2": {"seconds": 0.0, "peak_mb_so_far": 1}})
    assert scale.evaluate(other)["figures"]["the write on the Air (estimate, sandbox seconds x 3.1), minutes"] == "31"


def test_the_report_text_names_every_phase_check_figure_and_flag():
    flagged = facts(peak_mb=5_000, second_run={**facts()["second_run"], "appended": 3})
    text = scale.render(flagged, scale.evaluate(flagged))
    assert "11,000,000 events in 11,000 objects (5,500 transactions)" in text
    assert "write: pass 1" in text and "write: pass 2" in text and "1,500" in text
    assert "  FAIL the second run appended nothing and created nothing" in text
    assert "  ok   the first run wrote a marker" in text
    assert "  peak process memory, MB: 5,000" in text and "peak memory 5,000 MB is above" in text
    assert "merchant_transactions" in text and "10,725,000" in text
    assert "too few points" not in text
    assert "flags\n  none" in scale.render(facts(), scale.evaluate(facts()))
    assert "too few points" in scale.render(facts(samples=[]), scale.evaluate(facts(samples=[])))


# ---------------------------------------------------------------------------
# a whole measurement, at 5 transactions
# ---------------------------------------------------------------------------

def test_a_whole_measurement_at_five_transactions(tmp_path):
    said = []
    work = tmp_path / "work"
    result = scale.measure(5, work, batch_rows=3_000, sample_seconds=0.05, say=said.append)
    assert (result["events_landed"], result["objects_landed"]) == (10_000, 10)
    assert result["appended"] == {"merchants": 250, "loan_applications": 0, "loan_repayments": 0, "risk_events": 0, BIG: 9_750}
    assert result["batches"][BIG] == 3 and result["read_back"] == result["appended"]
    verdict = scale.evaluate(result)
    assert all(verdict["hard"].values()), verdict["hard"]
    assert [a["rows"] for a in result["appends"] if a["table"] == BIG] == [3_900, 3_900, 1_950] or \
        sum(a["rows"] for a in result["appends"] if a["table"] == BIG) == 9_750
    assert {a["table"] for a in result["appends"]} == {BIG, "merchants"} and all(a["seconds"] > 0 for a in result["appends"])
    assert sum(a["rows"] for a in result["appends"] if a["table"] == BIG) == 9_750
    assert [a["rows"] for a in result["appends"] if a["table"] == "merchants"] == [250]
    assert result["files_created"] > 0 and result["peak_mb"] > 0 and result["work"] == str(work)
    phases = {s[1] for s in result["samples"]}
    assert {"write: pass 1", "write: pass 2", "second run", "check"} <= phases and len(result["samples"]) >= 6
    rows_seen = {s[3] for s in result["samples"]}
    assert rows_seen <= {0, 3_900, 7_800, 9_750} and {3_900, 7_800} & rows_seen      # the big table's progress, not the merchants'
    assert set(result["phases"]) == {"write: pass 1", "write: pass 2", "write (both passes)", "read back", "second run", "check"}
    assert any("== second run" in m for m in said) and any(m.startswith("   check: ") for m in said)
    json.dumps(result)                                                    # everything in it can be written as JSON


def test_the_append_timer_is_removed_again_after_a_measurement(tmp_path):
    from pyiceberg.table import Table
    before = Table.append
    scale.measure(2, tmp_path / "w", batch_rows=3_000, sample_seconds=0.05)
    assert Table.append is before


def test_the_append_timer_is_removed_when_a_measurement_fails(tmp_path, monkeypatch):
    from pyiceberg.table import Table
    before = Table.append
    monkeypatch.setattr(scale, "check", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError):
        scale.measure(2, tmp_path / "w", batch_rows=3_000, sample_seconds=0.05)
    assert Table.append is before


def test_an_existing_work_directory_is_refused_and_left_alone(tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    (work / "keep.txt").write_text("mine")
    with pytest.raises(FileExistsError, match="nothing is deleted"):
        scale.measure(2, work)
    assert (work / "keep.txt").read_text() == "mine" and list(work.iterdir()) == [work / "keep.txt"]


def test_the_command_writes_a_report_and_exits_zero_on_a_pass(tmp_path, capsys):
    work = tmp_path / "work"
    assert scale.main(["--work", str(work), "--transactions", "3", "--batch-rows", "3000", "--sample-seconds", "0.05"]) == 0
    out = capsys.readouterr().out
    assert "PASS: 12 of 12 hard checks; 0 flags" in out and f"rm -r {work}" in out
    report = json.loads((work / "report.json").read_text())
    assert set(report) == {"facts", "verdict"} and all(report["verdict"]["hard"].values())


def test_the_command_exits_one_when_a_hard_check_fails_and_still_writes_the_report(tmp_path, capsys, monkeypatch):
    real = scale.evaluate

    def broken(facts_):
        verdict = real(facts_)
        verdict["hard"]["the check found no problem"] = False
        return verdict

    monkeypatch.setattr(scale, "evaluate", broken)
    work = tmp_path / "work"
    assert scale.main(["--work", str(work), "--transactions", "2", "--batch-rows", "3000", "--sample-seconds", "0.05"]) == 1
    assert "FAIL: 11 of 12 hard checks" in capsys.readouterr().out and (work / "report.json").exists()


def test_the_command_refuses_an_existing_work_directory_with_exit_one(tmp_path, capsys):
    assert scale.main(["--work", str(tmp_path)]) == 1
    assert "error: " in capsys.readouterr().err


def run_with_second_run_changed(monkeypatch, change):
    """measure() over a run function whose second call is changed by `change(result, warehouse_dir)`."""
    real, calls = scale.run, []

    def changed(reader, catalog, store, **kwargs):
        result = real(reader, catalog, store, **kwargs)
        calls.append(1)
        return change(result, Path(kwargs["warehouse"][len("file://"):])) if len(calls) == 2 else result

    monkeypatch.setattr(scale, "run", changed)


def test_a_second_run_that_changes_a_file_is_seen(tmp_path, monkeypatch):
    run_with_second_run_changed(monkeypatch, lambda result, warehouse: ((warehouse / "stray.parquet").write_text("x"), result)[1])
    result = scale.measure(2, tmp_path / "w", batch_rows=3_000, sample_seconds=0.05)
    assert result["second_run"]["unchanged"] == {"files": False, "snapshots": True, "catalog_bytes": True, "bucket_objects": True}
    assert scale.evaluate(result)["hard"]["the second run changed no file"] is False


def test_a_second_run_that_appends_creates_or_writes_a_marker_is_seen(tmp_path, monkeypatch):
    import dataclasses

    def pretend(result, warehouse):
        tables = {name: dataclasses.replace(t, rows_appended=3, created=True) for name, t in result.tables.items()}
        marker = dataclasses.replace(result.marker, written="20300101T000000000000Z", catalog_copied=True)
        return dataclasses.replace(result, tables=tables, marker=marker)

    run_with_second_run_changed(monkeypatch, pretend)
    result = scale.measure(2, tmp_path / "w", batch_rows=3_000, sample_seconds=0.05)
    assert result["second_run"] == {"appended": 15, "created": 5, "marker_written": True, "catalog_copied": True,
                                    "unchanged": {"files": True, "snapshots": True, "catalog_bytes": True, "bucket_objects": True}}
    failed = [n for n, ok in scale.evaluate(result)["hard"].items() if not ok]
    assert failed == ["the second run appended nothing and created nothing", "the second run wrote no marker and copied no catalog"]


def test_a_check_that_finds_a_problem_is_carried_into_the_facts(tmp_path, monkeypatch):
    from bronze.replay import Report
    monkeypatch.setattr(scale, "check", lambda *a, **k: Report(problems=["risk_events: Bronze holds 1 keys more than once"]))
    result = scale.measure(2, tmp_path / "w", batch_rows=3_000, sample_seconds=0.05)
    assert result["check"]["problems"] == ["risk_events: Bronze holds 1 keys more than once"] and result["check"]["rows"] == {}
    assert not scale.evaluate(result)["hard"]["the check found no problem"]
