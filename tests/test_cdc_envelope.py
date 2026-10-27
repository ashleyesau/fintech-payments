"""tests/test_cdc_envelope.py: M3, the change-event envelope, the event-date rule and the drift check.

No database needed: the transactions come from the raw pgoutput messages captured in
tests/fixtures/pgoutput.json, assembled by the same code the consumer will run. The live
check, every row of a seeded run, is in test_cdc_envelope_live.py.
"""
import dataclasses
import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from cdc import types
from cdc.columns import EXPECTED_COLUMNS, EXPECTED_TYPES, check_relation
from cdc.envelope import Envelope, build_events, event_time, sast_date, to_line
from cdc.errors import DecodeError, SchemaDriftError, UnsupportedChange
from cdc.pgoutput import Relation
from cdc.transactions import TransactionAssembler
from tests.helpers import (
    PGOUTPUT_FIXTURES, begin_message, commit_message, pgoutput_scenario, relation_message,
)

NOW = datetime(2026, 10, 27, 9, 41, 7, 490000, tzinfo=timezone.utc)  # the contract's example instant
COLUMN_COUNTS = {"merchants": 11, "merchant_transactions": 32, "loan_applications": 24,
                 "loan_repayments": 22, "risk_events": 13}


def assemble(name):
    assembler = TransactionAssembler()
    done = [t for lsn, data in pgoutput_scenario(name) if (t := assembler.feed(lsn, data)) is not None]
    assert not assembler.in_transaction
    return done


def events(name):
    return [e for t in assemble(name) for e in build_events(t, "fintech_cdc", NOW)]


def real_relations() -> dict[str, Relation]:
    out = {}
    for t in assemble("inserts_utc"):
        for _, message in t.changes:
            out[message.relation.name] = message.relation
    return out


# ---------------------------------------------------------------------------
# The envelope's shape (contract section 3)
# ---------------------------------------------------------------------------

def test_every_event_has_every_field_in_the_contract_order():
    got = events("inserts_utc")
    assert len(got) == 8
    for event in got:
        dumped = event.model_dump(by_alias=True)
        assert list(dumped) == ["envelope_version", "op", "source", "transaction", "event_ts", "event_date",
                                "ts_ms", "ingested_at", "flags", "before", "after"]
        assert list(dumped["source"]) == ["db", "schema", "table", "commit_lsn", "change_lsn", "commit_ts"]
        assert list(dumped["transaction"]) == ["id", "position", "size"]
        assert dumped["envelope_version"] == 1


def test_before_and_after_hold_every_column_in_table_order():
    for event in events("inserts_utc"):
        table = event.source.table
        assert len(event.after) == COLUMN_COUNTS[table]
        assert tuple(event.after) == EXPECTED_COLUMNS[table]


def test_source_and_transaction_fields_for_a_multi_table_transaction():
    first = assemble("inserts_utc")[0]
    got = build_events(first, "fintech_cdc", NOW)
    assert [e.source.table for e in got] == ["merchants", "merchant_transactions", "risk_events"]
    assert [e.transaction.position for e in got] == [0, 1, 2]
    assert {e.transaction.size for e in got} == {3}
    assert {e.transaction.id for e in got} == {PGOUTPUT_FIXTURES["inserts_utc"]["messages"][0]["xid"]}
    assert {e.source.commit_lsn for e in got} == {first.commit_lsn}
    assert {e.source.commit_ts for e in got} == {got[0].source.commit_ts}
    assert got[0].source.commit_ts.endswith("Z") and len(got[0].source.commit_ts) == 27
    change_lsns = [e.source.change_lsn for e in got]
    assert change_lsns == sorted(set(change_lsns)) and change_lsns[-1] < first.commit_lsn
    assert {(e.source.db, e.source.schema_) for e in got} == {("fintech_cdc", "public")}


def test_the_second_transaction_has_five_changes_at_positions_zero_to_four():
    second = build_events(assemble("inserts_utc")[1], "fintech_cdc", NOW)
    assert [e.transaction.position for e in second] == [0, 1, 2, 3, 4]
    assert [e.source.table for e in second] == ["loan_applications"] * 4 + ["loan_repayments"]


def test_the_dedup_key_is_unique_and_a_transaction_is_complete():
    got = events("inserts_utc") + events("disbursement")
    keys = [(e.source.commit_lsn, e.transaction.position) for e in got]
    assert len(keys) == len(set(keys))
    by_commit = {}
    for e in got:
        by_commit.setdefault(e.source.commit_lsn, []).append(e)
    for commit_events in by_commit.values():
        assert sorted(e.transaction.position for e in commit_events) == list(range(commit_events[0].transaction.size))


def test_ts_ms_and_ingested_at_are_one_instant_to_the_millisecond():
    for event in events("inserts_utc"):
        assert event.ts_ms == 1793094067490
        assert event.ingested_at == "2026-10-27T09:41:07.490000Z"  # the contract's example
    later = datetime(2026, 10, 27, 9, 41, 7, 490999, tzinfo=timezone.utc)
    got = build_events(assemble("inserts_utc")[0], "fintech_cdc", later)
    assert got[0].ts_ms == 1793094067490 and got[0].ingested_at == "2026-10-27T09:41:07.490000Z"


def test_an_empty_transaction_has_no_events_but_still_has_an_end_lsn_to_confirm():
    assembler = TransactionAssembler()
    assert assembler.feed(100, begin_message(500, 7)) is None
    transaction = assembler.feed(100, commit_message(500, 540))
    assert transaction.changes == () and transaction.end_lsn == 540
    assert build_events(transaction, "fintech_cdc", NOW) == []


# ---------------------------------------------------------------------------
# Event time and event_date (contract section 6)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table, column, value, date_", [
    ("merchants", "snapshot_at", "2025-03-01T21:59:00.000000Z", "2025-03-01"),              # 23:59 SAST snapshot
    ("merchant_transactions", "created_at", "2025-03-01T22:30:12.345678Z", "2025-03-02"),   # the contract's example
    ("loan_applications", "submission_ts", "2025-03-01T08:15:00.000000Z", "2025-03-01"),
    ("risk_events", "action_ts", "2025-03-01T22:30:12.345678Z", "2025-03-02"),
    ("loan_repayments", "payment_ts", "2025-04-02T06:00:00.000000Z", "2025-04-02"),
])
def test_each_table_takes_its_own_event_time(table, column, value, date_):
    row = {column: value, "due_date": "2000-01-01"}
    assert event_time(table, row) == (value, date_, [])


@pytest.mark.parametrize("utc, sast", [
    ("2025-03-01T21:59:59.999999Z", "2025-03-01"),   # 23:59:59.999999 SAST
    ("2025-03-01T22:00:00.000000Z", "2025-03-02"),   # midnight SAST
    ("2025-03-01T00:00:00.000000Z", "2025-03-01"),
    ("2025-02-28T22:00:00.000000Z", "2025-03-01"),   # month boundary
    ("2024-02-28T22:00:00.000000Z", "2024-02-29"),   # into a leap day
    ("2024-02-29T22:00:00.000000Z", "2024-03-01"),   # out of a leap day
    ("2025-12-31T22:00:00.000000Z", "2026-01-01"),   # year boundary
    ("2025-12-31T21:59:59.000000Z", "2025-12-31"),
])
def test_event_date_is_the_south_african_date_not_the_utc_date(utc, sast):
    assert sast_date(utc) == sast


@pytest.mark.parametrize("due, event_ts", [
    ("2025-05-02", "2025-05-01T22:00:00.000000Z"),   # the contract's example
    ("2025-03-01", "2025-02-28T22:00:00.000000Z"),
    ("2024-03-01", "2024-02-29T22:00:00.000000Z"),
    ("2025-01-01", "2024-12-31T22:00:00.000000Z"),
])
def test_an_unpaid_repayment_takes_its_date_from_due_date_and_is_flagged(due, event_ts):
    assert event_time("loan_repayments", {"payment_ts": None, "due_date": due}) == (event_ts, due, ["event_time_fallback"])
    assert sast_date(event_ts) == due, "the fallback event_ts is due_date at 00:00 SAST"


@pytest.mark.parametrize("table, row", [
    ("merchants", {"snapshot_at": None}),
    ("merchant_transactions", {"created_at": None}),
    ("loan_applications", {"submission_ts": None}),
    ("risk_events", {"action_ts": None}),
    ("loan_repayments", {"payment_ts": None, "due_date": None}),
])
def test_a_row_with_no_event_time_stops_the_consumer(table, row):
    with pytest.raises(DecodeError, match="no event time"):
        event_time(table, row)


def test_the_fixture_repayment_is_an_unpaid_attempt_with_the_fallback():
    repayment = events("inserts_utc")[-1]
    assert repayment.source.table == "loan_repayments" and repayment.after["payment_ts"] is None
    assert (repayment.event_ts, repayment.event_date, repayment.flags) == (
        "2025-04-01T22:00:00.000000Z", "2025-04-02", ["event_time_fallback"])


# ---------------------------------------------------------------------------
# Updates: the disbursement stays with its insert
# ---------------------------------------------------------------------------

def test_a_disbursement_update_stays_in_its_inserts_partition():
    insert, update, replay = events("disbursement")
    assert (insert.op, update.op, replay.op) == ("c", "u", "u")
    assert update.event_ts == insert.event_ts == replay.event_ts == "2025-03-01T08:15:00.000000Z"
    assert update.event_date == insert.event_date == replay.event_date == "2025-03-01"
    assert update.after["disbursed_at"] == "2025-03-03T09:30:00.000000Z", "disbursed two days later"
    assert update.event_date != sast_date(update.after["disbursed_at"]), "not partitioned by disbursed_at"


def test_an_update_carries_the_full_old_row_and_a_replay_is_flagged_as_a_no_op():
    insert, update, replay = events("disbursement")
    assert insert.before is None and insert.flags == []
    assert update.before is not None and update.before["disbursed_at"] is None and update.flags == []
    assert replay.before == replay.after and replay.flags == ["noop_update"]
    assert (insert.source.commit_lsn < update.source.commit_lsn < replay.source.commit_lsn)
    assert tuple(update.before) == tuple(update.after) == EXPECTED_COLUMNS["loan_applications"]


# ---------------------------------------------------------------------------
# The Envelope model enforces the contract
# ---------------------------------------------------------------------------

def valid() -> dict:
    return events("disbursement")[1].model_dump(by_alias=True)


def test_a_valid_event_round_trips_through_the_model():
    assert Envelope(**valid()).model_dump(by_alias=True) == valid()


@pytest.mark.parametrize("change", [
    {"envelope_version": 2},
    {"op": "d"},
    {"op": "r"},
    {"op": "c"},                                    # an insert must not carry a before row
    {"before": None},                               # an update must carry one
    {"flags": ["something_else"]},
    {"event_ts": "2025-03-01T08:15:00Z"},           # no fractional digits
    {"event_ts": "2025-03-01T08:15:00.000000+02:00"},
    {"event_ts": "2025-03-01 08:15:00.000000Z"},
    {"event_date": "2025-3-1"},
    {"ingested_at": "yesterday"},
    {"ts_ms": -1},
    {"unexpected": 1},
    {"after": None},
    {"source": {**valid()["source"], "table": "transactions"}},
    {"source": {**valid()["source"], "schema": "other"}},
    {"source": {**valid()["source"], "commit_lsn": 2 ** 53}},
    {"source": {**valid()["source"], "commit_lsn": -1}},
    {"source": {**valid()["source"], "commit_ts": "2026-10-27T09:41:07.490000+00:00"}},
    {"source": {**valid()["source"], "unexpected": 1}},
    {"transaction": {"id": 1, "position": 1, "size": 1}},
    {"transaction": {"id": 1, "position": 0, "size": 0}},
    {"transaction": {"id": 1, "position": -1, "size": 1}},
])
def test_an_event_that_breaks_the_contract_cannot_be_built(change):
    with pytest.raises(ValidationError):
        Envelope(**{**valid(), **change})


def test_an_event_is_immutable():
    event = events("disbursement")[0]
    with pytest.raises(ValidationError):
        event.op = "u"


# ---------------------------------------------------------------------------
# Serialisation (contract section 2)
# ---------------------------------------------------------------------------

def test_a_line_is_compact_ascii_json_ending_in_one_newline_with_the_contract_key_order():
    for event in events("inserts_utc"):
        line = to_line(event)
        assert line.endswith(b"\n") and line.count(b"\n") == 1 and not line.startswith(b"\xef\xbb\xbf")
        assert line.isascii()
        parsed = json.loads(line)
        assert parsed == event.model_dump(by_alias=True)
        assert list(parsed) == list(event.model_dump(by_alias=True))
        assert list(parsed["after"]) == list(event.after)
        assert b'": ' not in line and b'", ' not in line, "no spaces after separators"


def test_the_fast_path_writes_the_same_bytes_as_compact_json_dumps():
    """Pydantic's serialiser and json.dumps agree on every ASCII event, so the fast path is safe."""
    got = events("inserts_utc") + events("disbursement")
    for event in got:
        expected = json.dumps(event.model_dump(by_alias=True), separators=(",", ":"), ensure_ascii=True).encode() + b"\n"
        assert to_line(event) == expected


def test_unicode_and_awkward_text_survive_a_line():
    row_event = next(e for e in events("inserts_utc") if e.source.table == "merchant_transactions")
    assert row_event.after["batch_id"] == 'batch-\u00e9 "q" \\ , caf\u00e9 \u4e2d'
    assert json.loads(to_line(row_event))["after"]["batch_id"] == row_event.after["batch_id"]


def test_a_newline_or_a_unicode_line_separator_in_a_value_cannot_split_a_line():
    transaction = assemble("inserts_utc")[0]
    change_lsn, insert = transaction.changes[1]
    awkward = "a\nb\r\nc\u2028d\u2029e\x85f"
    changed = dataclasses.replace(insert, new={**insert.new, "batch_id": awkward})
    transaction = dataclasses.replace(transaction, changes=((change_lsn, changed),))
    (event,) = build_events(transaction, "fintech_cdc", NOW)
    line = to_line(event)
    assert len(line.split(b"\n")) == 2 and len(line.decode().splitlines()) == 1
    assert json.loads(line)["after"]["batch_id"] == awkward


# ---------------------------------------------------------------------------
# Assembling transactions (what stops the consumer)
# ---------------------------------------------------------------------------

def test_a_transaction_is_returned_only_at_its_commit():
    assembler = TransactionAssembler()
    results = []
    for lsn, data in pgoutput_scenario("disbursement"):
        results.append((data[:1], assembler.feed(lsn, data), assembler.in_transaction))
    assert [kind for kind, done, _ in results if done] == [b"C"] * 3
    assert all(still_open for kind, done, still_open in results if kind in (b"I", b"U"))
    assert results[0][2] is True and results[-1][2] is False


def test_the_transaction_carries_the_commit_position_and_the_position_to_confirm():
    for t in assemble("inserts_utc"):
        assert t.end_lsn > t.commit_lsn and t.commit_ts.tzinfo is not None
        assert all(lsn < t.commit_lsn for lsn, _ in t.changes)


def test_a_delete_and_a_truncate_stop_the_consumer_at_that_message():
    assembler = TransactionAssembler()
    seen = []
    with pytest.raises(UnsupportedChange, match="delete on risk_events"):
        for lsn, data in pgoutput_scenario("delete_and_truncate"):
            seen.append(data[:1])
            assembler.feed(lsn, data)
    assert seen[-1] == b"D"


def test_an_update_on_a_default_identity_table_stops_the_consumer():
    assembler = TransactionAssembler()
    with pytest.raises(UnsupportedChange, match="only loan_applications"):
        for lsn, data in pgoutput_scenario("default_identity_updates"):
            assembler.feed(lsn, data)


@pytest.mark.parametrize("messages, pattern", [
    ([commit_message(10, 12)], "Commit without a Begin"),
    ([begin_message(10, 1), begin_message(20, 2)], "Begin inside an open transaction"),
    ([begin_message(10, 1), commit_message(11, 12)], "differs from the Begin"),
])
def test_a_broken_transaction_frame_stops_the_consumer(messages, pattern):
    assembler = TransactionAssembler()
    with pytest.raises(DecodeError, match=pattern):
        for message in messages:
            assembler.feed(1, message)


def test_a_change_outside_a_transaction_stops_the_consumer():
    first_insert = next(data for _, data in pgoutput_scenario("inserts_utc") if data[:1] == b"I")
    assembler = TransactionAssembler()
    for lsn, data in pgoutput_scenario("inserts_utc"):
        if data[:1] == b"R":
            assembler.feed(lsn, data)  # a Relation message alone is fine
            break
    with pytest.raises(DecodeError, match="outside a transaction"):
        assembler.feed(5, first_insert)


# ---------------------------------------------------------------------------
# The drift check (decision D6)
# ---------------------------------------------------------------------------

def test_the_five_real_tables_pass_and_their_column_order_is_the_models():
    found = real_relations()
    assert set(found) == set(EXPECTED_COLUMNS) and len(found) == 5
    for name, relation in found.items():
        check_relation(relation)
        assert tuple(c.name for c in relation.columns) == EXPECTED_COLUMNS[name]


def drifted(name: str, columns) -> Relation:
    return dataclasses.replace(real_relations()[name], columns=tuple(columns))


def test_an_added_column_is_detected():
    relation = real_relations()["merchants"]
    extra = dataclasses.replace(relation.columns[0], name="legal_name")
    with pytest.raises(SchemaDriftError, match="in the database but not the model: legal_name"):
        check_relation(drifted("merchants", (*relation.columns, extra)))


def test_a_dropped_column_is_detected():
    relation = real_relations()["loan_repayments"]
    with pytest.raises(SchemaDriftError, match="in the model but not the database: write_off_flag"):
        check_relation(drifted("loan_repayments", relation.columns[:-1]))


def test_a_renamed_column_is_detected_as_one_added_and_one_missing():
    relation = real_relations()["risk_events"]
    renamed = [dataclasses.replace(c, name="rule_kind") if c.name == "rule_type" else c for c in relation.columns]
    with pytest.raises(SchemaDriftError, match=r"rule_kind.*rule_type"):
        check_relation(drifted("risk_events", renamed))


def test_reordered_columns_are_detected():
    relation = real_relations()["merchants"]
    swapped = (relation.columns[1], relation.columns[0], *relation.columns[2:])
    with pytest.raises(SchemaDriftError, match="different order"):
        check_relation(drifted("merchants", swapped))


def test_another_table_or_schema_is_detected():
    relation = real_relations()["merchants"]
    with pytest.raises(SchemaDriftError, match="not one of the five tables"):
        check_relation(dataclasses.replace(relation, name="merchants_archive"))
    with pytest.raises(SchemaDriftError, match="not in the public schema"):
        check_relation(dataclasses.replace(relation, schema="staging"))


def test_a_relation_message_that_drifts_in_the_middle_of_a_stream_stops_the_consumer():
    columns = [(c.name, c.type_oid) for c in real_relations()["risk_events"].columns]
    columns[3] = ("merchant_ref", columns[3][1])
    messages = [data for _, data in pgoutput_scenario("inserts_utc")]
    assembler = TransactionAssembler()
    assembler.feed(1, messages[0])                       # Begin
    assembler.feed(2, messages[1])                       # a good Relation message
    with pytest.raises(SchemaDriftError, match="merchant_ref"):
        assembler.feed(3, relation_message(99, "risk_events", columns))


def test_a_drifted_table_is_caught_before_any_row_of_it_is_decoded_into_an_event():
    """An Insert after a drifted Relation message never happens: the Relation stops the run."""
    assembler = TransactionAssembler()
    assembler.feed(1, begin_message(10, 1))
    columns = [(c.name, c.type_oid) for c in real_relations()["merchants"].columns] + [("extra", 25)]
    with pytest.raises(SchemaDriftError):
        assembler.feed(2, relation_message(77, "merchants", columns))


# ---------------------------------------------------------------------------
# The type drift check
# ---------------------------------------------------------------------------

def with_type(relation: Relation, column: str, type_oid: int) -> Relation:
    changed = [dataclasses.replace(c, type_oid=type_oid) if c.name == column else c for c in relation.columns]
    return dataclasses.replace(relation, columns=tuple(changed))


def a_wrong_type_for(allowed: frozenset) -> int:
    """A type the contract maps, that this column must not have."""
    return next(oid for oid in (types.TEXT, types.BOOL, types.DATE, types.NUMERIC, types.TIMESTAMPTZ) if oid not in allowed)


ALL_COLUMNS = [(table, column) for table, columns in EXPECTED_COLUMNS.items() for column in columns]


def test_the_expected_types_cover_every_column_of_every_table():
    assert {t: tuple(c) for t, c in EXPECTED_TYPES.items()} == EXPECTED_COLUMNS
    assert len(ALL_COLUMNS) == 11 + 32 + 24 + 22 + 13
    assert all(oids <= set(types.CONVERTERS) for columns in EXPECTED_TYPES.values() for oids in columns.values())


@pytest.mark.parametrize("table, column", ALL_COLUMNS)
def test_a_change_of_any_columns_type_is_detected(table, column):
    relation = real_relations()[table]
    wrong = a_wrong_type_for(EXPECTED_TYPES[table][column])
    with pytest.raises(SchemaDriftError, match=rf"{column} is .*the model expects"):
        check_relation(with_type(relation, column, wrong))


def test_the_message_names_the_column_the_new_type_and_the_expected_one():
    relation = with_type(real_relations()["merchants"], "risk_score", types.TEXT)
    with pytest.raises(SchemaDriftError, match=r"merchants: column types differ.*risk_score is text, the model expects bigint or integer"):
        check_relation(relation)
    relation = with_type(real_relations()["loan_applications"], "decline_reasons", types.TEXT)
    with pytest.raises(SchemaDriftError, match=r"decline_reasons is text, the model expects text\[\]"):
        check_relation(relation)


def test_every_changed_column_is_listed_not_only_the_first():
    relation = with_type(with_type(real_relations()["merchants"], "is_metro", types.TEXT), "snapshot_at", types.DATE)
    with pytest.raises(SchemaDriftError) as caught:
        check_relation(relation)
    assert "is_metro is text" in str(caught.value) and "snapshot_at is date" in str(caught.value)


def test_integer_and_bigint_are_interchangeable_because_both_are_json_integers():
    for table, columns in EXPECTED_TYPES.items():
        relation = real_relations()[table]
        for column, allowed in columns.items():
            if allowed == frozenset({types.INT4, types.INT8}):
                current = next(c.type_oid for c in relation.columns if c.name == column)
                other = types.INT8 if current == types.INT4 else types.INT4
                check_relation(with_type(relation, column, other))


def test_an_integer_column_that_becomes_numeric_is_detected():
    relation = real_relations()["merchant_transactions"]
    with pytest.raises(SchemaDriftError, match="amount_minor is numeric"):
        check_relation(with_type(relation, "amount_minor", types.NUMERIC))


def test_a_relation_message_that_changes_a_type_in_the_middle_of_a_stream_stops_the_consumer():
    columns = [(c.name, c.type_oid) for c in real_relations()["merchants"].columns]
    columns[7] = (columns[7][0], types.TEXT)  # risk_score
    assembler = TransactionAssembler()
    assembler.feed(1, begin_message(10, 1))
    with pytest.raises(SchemaDriftError, match="risk_score is text"):
        assembler.feed(2, relation_message(77, "merchants", columns))


def test_a_type_the_contract_does_not_map_still_stops_at_the_decoder():
    from cdc.errors import UnknownTypeError
    columns = [(c.name, c.type_oid) for c in real_relations()["merchants"].columns]
    columns[7] = (columns[7][0], 701)  # float8
    assembler = TransactionAssembler()
    assembler.feed(1, begin_message(10, 1))
    with pytest.raises(UnknownTypeError):
        assembler.feed(2, relation_message(77, "merchants", columns))
