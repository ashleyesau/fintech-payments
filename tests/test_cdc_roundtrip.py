"""tests/test_cdc_roundtrip.py: M5, the round trip (CDC Plan section 11, checks 1 to 3).

A seeded backfill with loans is written into a database that already has a slot, then drained by the
real reader and consumer into the in-memory sink. The landed events are replayed into a blank database
(tests/roundtrip.py: Postgres rebuilds each row from the event's `after` image) and compared with the
source tables; the counts and the order are checked on the landed objects. Slow: about a minute.
"""
import copy
import gzip
import io
import json
from collections import Counter
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from cdc.consumer import Consumer
from cdc.dedup import dedup, events_in, incomplete_transactions, key
from cdc.errors import SinkError
from cdc.landing import object_key
from cdc.lsn import format_lsn
from cdc.reader import Reader
from cdc.reconcile import DirectoryObjects, MemoryObjects, execute as reconcile_command, parse_name, reconcile
from tests.test_cdc_main import live_settings, settings
from simulator import db, lending, repayments
from simulator.db import STREAMS
from simulator.runner import RunSettings, run_backfill
from tests.conftest import new_cdc_slot
from tests.fakes import MemorySink
from tests.helpers import LONG_RUN, START, table_contents
from tests.roundtrip import apply_events, blank_database, in_commit_order, landed_events, partition_order_problems

pytestmark = pytest.mark.slow

NOW = datetime(2026, 10, 27, 9, 41, 7, 490000, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def run(cdc_dsn, tmp_path_factory):
    """The seeded run, landed in the in-memory sink by the real consumer, with the source database
    still holding the rows. Everything in this module reads from it (nothing else may touch the tables)."""
    with new_cdc_slot(cdc_dsn) as slot:
        with pytest.MonkeyPatch.context() as patch:
            for state in ("growing", "stable"):
                patch.setitem(lending._PROPENSITY, state, lending._PROPENSITY[state] * 10)
            # a weaker first strike, so there are unpaid attempts (no payment time) to exercise the due-date fallback
            patch.setitem(repayments._DEBI, "first_strike_base_success_rate", 0.5)
            writer = db.connect(cdc_dsn)
            settings = RunSettings(START, START + timedelta(days=LONG_RUN["days"]), LONG_RUN["count"],
                                   checkpoint_path=str(tmp_path_factory.mktemp("roundtrip") / "checkpoint.json"))
            run_backfill(settings, writer)
            writer.close()
        plant_awkward_rows(cdc_dsn)
        sink = MemorySink()
        with Reader(cdc_dsn, slot, "fintech_cdc") as reader:
            summary = Consumer(reader, sink, database="fintech_cdc", prefix="cdc/cdc_r01/", batch_size=10_000,
                               workers=8, clock=lambda: NOW).run(follow=False)
        with psycopg.connect(cdc_dsn) as conn:
            source = table_contents(conn)
        yield {"dsn": cdc_dsn, "sink": sink, "summary": summary, "source": source}


AWKWARD_REASONS = ["a,b", 'say "hi"', "back\\slash", "", None, "NULL", "line\nbreak", "ünï 😀 日本語", "{braces}"]
AWKWARD_TEXT = 'Ünï "co" \\ 😀\nsecond line'


def plant_awkward_rows(dsn):
    """Rows built to break a careless decoder: an array with commas, quotes, a backslash, an empty string, a
    NULL element, the string 'NULL', a line break, non-ASCII text and braces; text with quotes, backslashes,
    non-ASCII and a line break; a timestamp with microseconds in a +05:30 offset; the largest integer the
    contract allows; a payment one microsecond before midnight UTC. One transaction, so they travel together."""
    with psycopg.connect(dsn) as conn:
        with conn.transaction():
            conn.execute(
                "INSERT INTO merchants (merchant_id, archetype, province, onboarded_at, snapshot_at) "
                "VALUES ('m-ünï-😀', 'tutor', 'gauteng', '2025-01-01 00:00+00', '2025-03-01 21:59:59.999999+00')")
            conn.execute(
                "INSERT INTO merchant_transactions (transaction_id, merchant_id, event_type, channel, status, amount_minor, "
                "auth_code, device_id, is_test, settlement_date, created_at) "
                "VALUES ('txn-awkward', 'm-ünï-😀', 'payment', 'card_present', 'completed', 9007199254740991, "
                "E'é\\\\\"', '日本語', true, '2025-03-04', '2025-03-01 23:59:59.999999+00')")
            conn.execute(
                "INSERT INTO risk_events (alert_id, entity_type, entity_id, merchant_id, rule_id, rule_type, risk_score, "
                "action, action_ts) VALUES ('alert-ünï', 'transaction', 'txn-awkward', 'm-ünï-😀', 'rule-ünï', "
                "'large_txn_flag', 15, 'flag_review', '2025-03-01 23:59:59.999999+00')")
            conn.execute(
                "INSERT INTO loan_applications (application_id, merchant_id, applicant_user_id, business_id, "
                "application_channel, submission_ts, business_registration, requested_amount, requested_term_months, "
                "decision, decline_reasons, total_cost_percent, fica_status) "
                "VALUES ('app-awkward', 'm-ünï-😀', 'user-é', 'biz-1', 'web', '2025-03-01 08:15:00.123456+05:30', "
                "%s, 9007199254740991, 6, 'declined', %s, 0.0700, 'pass')",
                (AWKWARD_TEXT, AWKWARD_REASONS))


def first_difference(source, replayed):
    for table in source:
        if source[table] != replayed[table]:
            a, b = source[table], replayed[table]
            if len(a) != len(b):
                return f"{table}: {len(a)} rows in the source, {len(b)} replayed"
            for x, y in zip(a, b):
                if x != y:
                    return f"{table}: source {x!r} differs from replayed {y!r}"
    return None


def replay(dsn, events):
    with blank_database(dsn) as replay_dsn:
        with psycopg.connect(replay_dsn) as conn:
            apply_events(conn, events)
            conn.commit()
            return table_contents(conn)


# ---------------------------------------------------------------------------
# the run is rich enough to mean something
# ---------------------------------------------------------------------------

def test_the_seeded_run_reaches_every_table_and_every_path_the_check_is_about(run):
    events = landed_events(run["sink"])
    by_table = Counter(e["source"]["table"] for e in events)
    assert set(by_table) == set(STREAMS), by_table
    assert min(by_table.values()) > 0 and len(events) > 20_000
    assert Counter(e["op"] for e in events)["u"] > 0, "no disbursement updates: the run did not originate loans"
    assert any("event_time_fallback" in e["flags"] for e in events), "no unpaid repayment fell back to its due date"
    assert run["summary"].batches > 1 and run["summary"].objects_replayed == 0
    assert run["summary"].changes == len(events)


def test_the_awkward_values_reach_the_landed_events_intact(run):
    events = {e["after"].get("application_id") or e["after"].get("transaction_id") or e["after"].get("alert_id")
              or e["after"]["merchant_id"]: e for e in landed_events(run["sink"])
              if e["after"].get("application_id") == "app-awkward" or e["after"].get("transaction_id") == "txn-awkward"
              or e["after"].get("alert_id") == "alert-ünï" or e["after"]["merchant_id"] == "m-ünï-😀" and e["source"]["table"] == "merchants"}
    application = events["app-awkward"]["after"]
    assert application["decline_reasons"] == AWKWARD_REASONS            # NULL element and the string 'NULL' kept apart
    assert application["business_registration"] == AWKWARD_TEXT
    assert application["applicant_user_id"] == "user-é"
    assert application["requested_amount"] == 9007199254740991
    assert application["total_cost_percent"] == "0.0700"               # a string, with its scale
    assert application["submission_ts"] == "2025-03-01T02:45:00.123456Z"   # +05:30 parsed, not assumed
    assert events["app-awkward"]["event_date"] == "2025-03-01"
    payment = events["txn-awkward"]
    assert payment["after"]["created_at"] == "2025-03-01T23:59:59.999999Z"
    assert payment["event_date"] == "2025-03-02"                       # 01:59 the next day in South Africa
    assert payment["after"]["auth_code"] == 'é\\"' and payment["after"]["device_id"] == "日本語"
    assert payment["after"]["is_test"] is True and payment["after"]["settlement_date"] == "2025-03-04"
    assert events["alert-ünï"]["after"]["rule_id"] == "rule-ünï"
    assert events["m-ünï-😀"]["after"]["merchant_id"] == "m-ünï-😀"


# ---------------------------------------------------------------------------
# check 1: replayed events equal the source
# ---------------------------------------------------------------------------

def test_landed_events_replayed_into_a_blank_database_equal_the_source_tables(run):
    replayed = replay(run["dsn"], in_commit_order(landed_events(run["sink"])))
    assert first_difference(run["source"], replayed) is None
    assert replayed == run["source"]
    assert all(len(rows) > 0 for rows in replayed.values())


def test_events_that_landed_twice_replay_to_the_same_tables_once_deduplicated(run):
    events = landed_events(run["sink"])
    again = copy.deepcopy(events)
    for event in again:                                    # a replay differs only in the consumer's clock
        event["ts_ms"] += 600_000
        event["ingested_at"] = "2026-10-27T09:51:07.490000Z"
    landed_twice = events + again
    assert len(dedup(landed_twice)) == len(events)
    replayed = replay(run["dsn"], in_commit_order(landed_twice))
    assert replayed == run["source"]


def test_without_dedup_the_double_landed_events_are_refused_so_the_check_is_not_lenient(run):
    events = landed_events(run["sink"])
    with blank_database(run["dsn"]) as replay_dsn, psycopg.connect(replay_dsn) as conn:
        with pytest.raises(psycopg.errors.UniqueViolation):
            apply_events(conn, sorted(events + events, key=key))


def test_one_wrong_value_in_one_event_is_caught_by_the_comparison(run):
    events = in_commit_order(landed_events(run["sink"]))
    target = next(e for e in events if e["source"]["table"] == "merchant_transactions")
    broken = copy.deepcopy(events)
    broken[events.index(target)]["after"]["amount_minor"] += 1
    replayed = replay(run["dsn"], broken)
    assert replayed != run["source"]
    assert first_difference(run["source"], replayed).startswith("merchant_transactions:")


def test_a_missing_event_is_caught_by_the_comparison(run):
    events = in_commit_order(landed_events(run["sink"]))
    last_update = max(i for i, e in enumerate(events) if e["op"] == "u")
    without = events[:last_update] + events[last_update + 1:]
    replayed = replay(run["dsn"], without)
    assert replayed != run["source"] and first_difference(run["source"], replayed).startswith("loan_applications:")


# ---------------------------------------------------------------------------
# check 2: the counts
# ---------------------------------------------------------------------------

def test_landed_inserts_equal_the_row_counts_and_updates_equal_the_disbursed_loans(run):
    events = landed_events(run["sink"])
    inserts = Counter(e["source"]["table"] for e in events if e["op"] == "c")
    updates = Counter(e["source"]["table"] for e in events if e["op"] == "u")
    for table in STREAMS:
        assert inserts[table] == len(run["source"][table]), table
    assert set(updates) == {"loan_applications"}
    with psycopg.connect(run["dsn"]) as conn:
        disbursed = conn.execute("SELECT count(1) FROM loan_applications WHERE disbursed_at IS NOT NULL").fetchone()[0]
    assert updates["loan_applications"] == disbursed > 0
    assert len(events) == sum(inserts.values()) + disbursed


def test_no_disbursement_update_is_a_no_op_in_a_run_without_replays(run):
    updates = [e for e in landed_events(run["sink"]) if e["op"] == "u"]
    assert all("noop_update" not in e["flags"] and e["before"] != e["after"] for e in updates)
    assert all(e["before"]["disbursed_at"] is None and e["after"]["disbursed_at"] is not None for e in updates)


# ---------------------------------------------------------------------------
# check 3: order, and transactions
# ---------------------------------------------------------------------------

def test_no_lsn_goes_backwards_within_a_table_and_event_date(run):
    assert partition_order_problems(run["sink"]) == []


def test_the_order_check_can_fail(run):
    sink = MemorySink()
    sink.objects = dict(run["sink"].objects)
    name = next(n for n in sorted(sink.objects) if "/merchant_transactions/" in n)
    events = events_in(sink.objects[name])
    assert len(events) > 1
    events[0], events[1] = events[1], events[0]                         # two events swapped inside one object
    sink.objects[name] = gzip.compress(
        b"".join(json.dumps(e, separators=(",", ":")).encode() + b"\n" for e in events), mtime=0)
    assert partition_order_problems(sink)


def test_every_transaction_landed_whole_and_the_run_has_no_duplicates(run):
    events = landed_events(run["sink"])
    assert incomplete_transactions(events) == {}
    assert len({key(e) for e in events}) == len(events)


# ---------------------------------------------------------------------------
# the reconciler (cdc/reconcile.py), the definition of done's check, on the same run
# ---------------------------------------------------------------------------

def reconciled(run, objects=None):
    return reconcile(MemoryObjects(dict(run["sink"].objects if objects is None else objects)), run["dsn"])


def rewrite(data, change):
    """An object's gzipped bytes with its events changed by `change(events)`."""
    events = events_in(data)
    change(events)
    return gzip.compress(b"".join(json.dumps(e, separators=(",", ":")).encode() + b"\n" for e in events), mtime=0)


def pick(run, table):
    return next(n for n in sorted(run["sink"].objects) if f"/{table}/" in n)


def test_the_run_reconciles_and_every_count_is_reported(run):
    result = reconciled(run)
    assert result.ok, result.problems
    assert result.objects == len(run["sink"].objects) and result.duplicates == 0 and result.notes == []
    assert result.inserts == {t: len(run["source"][t]) for t in result.inserts}
    assert result.events == sum(result.inserts.values()) + result.updates and result.updates > 0 and result.megabytes > 0


def test_a_replay_that_landed_again_under_other_names_is_absorbed_and_noted(run):
    # A replay that covered less data than the original batch writes an object with another name (its LSN range
    # differs) holding events that are already in the original: pick an object that spans several transactions
    # and land its first transaction's events again.
    objects = dict(run["sink"].objects)
    name = max((n for n in objects if "/loan_applications/" in n and parse_name(n)[2] != parse_name(n)[3]),
               key=lambda n: len(events_in(objects[n])))      # an application and its later disbursement update
    events = events_in(objects[name])
    first = events[0]["source"]["commit_lsn"]
    again_events = [e for e in events if e["source"]["commit_lsn"] == first]
    assert 0 < len(again_events) < len(events)
    new_name = object_key("cdc/cdc_r01/", "loan_applications", events[0]["event_date"], first, first)
    assert new_name not in objects and new_name != name
    objects[new_name] = gzip.compress(b"".join(json.dumps(e, separators=(",", ":")).encode() + b"\n" for e in again_events), mtime=0)
    result = reconciled(run, objects)
    assert result.ok, result.problems
    assert result.duplicates == len(again_events)
    assert result.inserts["loan_applications"] == len(run["source"]["loan_applications"])
    assert len(result.notes) == 1 and "overlap" in result.notes[0]


def test_an_object_that_is_missing_is_caught_by_the_counts_and_by_the_digest(run):
    objects = dict(run["sink"].objects)
    del objects[pick(run, "risk_events")]
    problems = " | ".join(reconciled(run, objects).problems)
    assert "risk_events:" in problems and "inserts landed" in problems and "the replayed table differs" in problems


def test_one_wrong_value_is_found_and_the_difference_is_explained(run):
    objects = dict(run["sink"].objects)
    name = pick(run, "merchant_transactions")
    objects[name] = rewrite(objects[name], lambda events: events[3]["after"].update(amount_minor=events[3]["after"]["amount_minor"] + 1))
    problems = reconciled(run, objects).problems
    assert len(problems) == 1 and problems[0].startswith("merchant_transactions: the replayed table differs")
    assert "1 with different values" in problems[0]


def test_a_missing_update_is_found(run):
    objects = dict(run["sink"].objects)
    name = next(n for n in sorted(objects) if "/loan_applications/" in n and any(e["op"] == "u" for e in events_in(objects[n])))
    objects[name] = rewrite(objects[name], lambda events: events.remove(next(e for e in events if e["op"] == "u")))
    problems = " | ".join(reconciled(run, objects).problems)
    assert "updates landed for" in problems and "the replayed table differs" in problems


def test_an_incomplete_transaction_is_found(run):
    objects = dict(run["sink"].objects)
    name = pick(run, "risk_events")
    objects[name] = rewrite(objects[name], lambda events: events.pop(0))
    problems = " | ".join(reconciled(run, objects).problems)
    assert "landed positions" in problems and "of " in problems


def test_an_object_that_is_not_what_its_name_says_is_found(run):
    objects = dict(run["sink"].objects)
    name = pick(run, "merchants")
    objects[name.replace("/merchants/", "/risk_events/")] = objects.pop(name)           # the name lies about the table
    problems = " | ".join(reconciled(run, objects).problems)
    assert "holds events of other tables" in problems


def test_events_out_of_order_inside_an_object_are_found(run):
    objects = dict(run["sink"].objects)
    name = pick(run, "merchant_transactions")
    objects[name] = rewrite(objects[name], lambda events: events.reverse())
    assert any("do not ascend strictly" in p for p in reconciled(run, objects).problems)


def test_garbage_and_strangers_are_found(run):
    objects = dict(run["sink"].objects)
    objects[pick(run, "merchants")] = b"not gzip at all"
    objects["cdc/cdc_r01/readme.txt.ndjson.gz"] = b"x"
    objects["cdc/cdc_r01/widgets/event_date=2025-03-01/lsn_0000000000000001_0000000000000002.ndjson.gz"] = b"x"
    problems = " | ".join(reconciled(run, objects).problems)
    assert "not gzipped NDJSON" in problems and "not an object name" in problems and "widgets is not one of the five tables" in problems


def test_an_empty_prefix_does_not_reconcile_because_the_source_has_rows(run):
    problems = reconciled(run, {}).problems
    assert any("inserts landed, the source has" in p for p in problems) and any("the replayed table differs" in p for p in problems)


# the command

def settings_for_run(run, **kwargs):
    return live_settings(run["dsn"], "cdc_r01", **kwargs)


def test_the_command_reconciles_a_directory_laid_out_like_the_bucket_and_exits_zero(run, tmp_path):
    for name, data in run["sink"].objects.items():
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    (tmp_path / "cdc" / "cdc_r02").mkdir(parents=True)
    (tmp_path / "cdc" / "cdc_r02" / "stranger.ndjson.gz").write_bytes(b"another run's object, outside the prefix")
    out, err = io.StringIO(), io.StringIO()
    code = reconcile_command(settings_for_run(run), directory=str(tmp_path), out=out, err=err)
    assert code == 0 and err.getvalue() == ""
    lines = out.getvalue().splitlines()
    assert lines[0].startswith("reconciling directory ") and "with fintech_cdc_test_" in lines[0]
    assert lines[-1].startswith("RECONCILED: ") and "the replay equals fintech_cdc_test_" in lines[-1]
    assert sum("the replay equals the source" in line for line in lines) == 5


def test_the_command_exits_two_and_lists_every_problem_when_it_does_not_reconcile(run):
    objects = dict(run["sink"].objects)
    del objects[pick(run, "risk_events")]
    out, err = io.StringIO(), io.StringIO()
    code = reconcile_command(settings_for_run(run), objects_factory=lambda s: MemoryObjects(objects), out=out, err=err)
    assert code == 2 and "RECONCILED" not in out.getvalue()
    lines = err.getvalue().splitlines()
    assert lines[-1].startswith("error: not reconciled: ") and all(line.startswith("PROBLEM: ") for line in lines[:-1])


def test_a_bucket_failure_exits_three_and_a_dropped_connection_exits_one(run):
    class Broken:
        def keys(self):
            raise SinkError("could not list cdc/cdc_r01/ in bucket cdc_landing: gave up after 5 attempts")

    out, err = io.StringIO(), io.StringIO()
    assert reconcile_command(settings_for_run(run), objects_factory=lambda s: Broken(), out=out, err=err) == 3
    assert err.getvalue().startswith("error: could not list cdc/cdc_r01/") and len(err.getvalue().splitlines()) == 1
    out, err = io.StringIO(), io.StringIO()
    code = reconcile_command(settings(host="localhost", port=1, password="s3cret-pa55word"),
                             objects_factory=lambda s: MemoryObjects({}), out=out, err=err)
    assert code == 1 and "s3cret-pa55word" not in out.getvalue() + err.getvalue()

