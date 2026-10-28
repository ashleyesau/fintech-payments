"""tests/test_bronze_mapping.py: G2 step 2, the strict event to row mapping and its inverse (bronze/mapping.py).

What is held here, each with a mutation that fails it (see the commit message):
  - every event field is a Bronze column or on the dropped list, and the rows have exactly the schema's columns;
  - the round trip row_to_event(event_to_row(e)) equals e, key order included, over the contract's own examples
    and over awkward values in every column (nulls, limits, unicode, empty and null lists, the SAST boundary);
  - every kind of wrong shape is refused with a MappingError that names the table and the column, in every
    column of every table, and in the before image;
  - the mapping and the Envelope model give the same answer on every envelope level fault;
  - the inverse is as strict as the forward direction.
"""
import json
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone

import pytest

from bronze.check import exit_code
from bronze.errors import ContractError, MappingError
from bronze.mapping import DROPPED_FIELDS, ENVELOPE_FIELDS, event_to_row, row_to_event
from bronze.schema import INTEGER_WIDTHS, build_schema
from cdc.columns import EXPECTED_COLUMNS
from cdc.envelope import Envelope
from tests import bronze_contract as contract
from tests.bronze_synth import DAY, FIRST_LSN, LSN_STEP, event

DB = "fintech_cdc"
TABLES = tuple(EXPECTED_COLUMNS)
UTC = timezone.utc


def to_row(e):
    return event_to_row(e, source_db=DB)


def to_event(table, row):
    return row_to_event(table, row, source_db=DB)


def make(table="merchants", op="c", **kw):
    return event(table, op, FIRST_LSN + LSN_STEP, kw.pop("position", 0), kw.pop("size", 1), **kw)


def changed(e, path, value):
    """A copy of event `e` with the field at `path` set to `value` (or deleted when value is DELETE)."""
    copy = deepcopy(e)
    target = copy
    for key in path[:-1]:
        target = target[key]
    if value is DELETE:
        del target[path[-1]]
    else:
        target[path[-1]] = value
    return copy


DELETE = object()


def same_event(a, b):
    """Equal as values and as text, so field and key order count."""
    assert a == b
    assert json.dumps(a) == json.dumps(b)


# ---------------------------------------------------------------------------
# the contract's own examples (CDC Change Event Contract (v1) section 9)
# ---------------------------------------------------------------------------

def envelope(table, op, commit_lsn, change_lsn, xid, position, size, event_ts, event_date, flags, before, after):
    return {
        "envelope_version": 1, "op": op,
        "source": {"db": DB, "schema": "public", "table": table, "commit_lsn": commit_lsn, "change_lsn": change_lsn,
                   "commit_ts": "2026-10-27T09:41:07.481516Z"},
        "transaction": {"id": xid, "position": position, "size": size},
        "event_ts": event_ts, "event_date": event_date, "ts_ms": 1793094067490,
        "ingested_at": "2026-10-27T09:41:07.490000Z", "flags": flags, "before": before, "after": after,
    }


LOAN_AFTER = {
    "application_id": "b7e1d4a9-0c23-4f5e-a6b8-91d7c3e2f408", "merchant_id": "3f9c2a71-5b0e-4d6a-9c1d-2e8a7b4f10c5",
    "applicant_user_id": "user-3f9c2a71", "business_id": "biz-3f9c2a71", "application_channel": "web",
    "submission_ts": "2025-03-01T08:15:00.000000Z", "decision_ts": "2025-03-01T08:15:42.000000Z",
    "business_registration": "2019/123456/07", "months_trading": 38, "declared_monthly_revenue": 4500000,
    "requested_amount": 6000000, "requested_term_months": 6, "owner_credit_score": 612,
    "owner_bureau": "transunion", "actioned_by": "automated_engine", "decision": "approved", "decline_reasons": [],
    "approved_amount": 6000000, "approved_term_months": 6, "total_cost_percent": "0.0700", "disbursed_at": None,
    "disbursement_amount_minor": None, "model_version_id": "risk-v1", "fica_status": "pass",
}
DISBURSED = {**LOAN_AFTER, "disbursed_at": "2025-03-03T09:30:00.000000Z", "disbursement_amount_minor": 6000000}

EXAMPLES = {
    "merchants": envelope("merchants", "c", 26556528, 26556480, 741, 0, 1, "2025-03-01T21:59:00.000000Z", "2025-03-01", [], None, {
        "merchant_id": "3f9c2a71-5b0e-4d6a-9c1d-2e8a7b4f10c5", "archetype": "spaza_shop", "province": "gauteng",
        "is_metro": True, "onboarded_at": "2025-01-13T22:00:00.000000Z", "first_transaction_at": "2025-01-17T05:12:44.000000Z",
        "merchant_state": "growing", "risk_score": 14, "loan_count": 0, "has_active_loan": False,
        "snapshot_at": "2025-03-01T21:59:00.000000Z"}),
    "merchant_transactions": envelope("merchant_transactions", "c", 26618104, 26618032, 742, 0, 2,
                                       "2025-03-01T22:30:12.000000Z", "2025-03-02", [], None, {
        "transaction_id": "c41d8e02-77aa-4b19-8f3e-5d2a9b60e7f1", "merchant_id": "3f9c2a71-5b0e-4d6a-9c1d-2e8a7b4f10c5",
        "event_type": "payment", "channel": "card_present", "status": "completed", "entry_mode": "contactless",
        "payment_method": "visa", "bank_selected": None, "amount_minor": 12550, "currency": "ZAR",
        "interchange_fee_minor": 63, "scheme_fee_minor": 9, "acquirer_markup_minor": 120, "mcc": "5411",
        "auth_code": "K7M2QX", "decline_code": None, "risk_score": 12, "risk_action": "allow",
        "created_at": "2025-03-01T22:30:12.000000Z", "bank_selected_at": None, "login_succeeded_at": None,
        "otp_requested_at": None, "otp_entered_at": None, "authorized_at": "2025-03-01T22:30:14.000000Z",
        "completed_at": "2025-03-01T22:30:15.000000Z", "refunded_at": None, "settlement_date": "2025-03-03",
        "batch_id": "batch-2025-03-03-3f9c2a71", "device_id": "POS-3f9c2a71", "pos_version": "v3.1.0",
        "is_test": False, "correlation_id": "corr-a1b2c3d4e5f6"}),
    "risk_events": envelope("risk_events", "c", 26618104, 26618080, 742, 1, 2, "2025-03-01T22:30:12.000000Z", "2025-03-02", [], None, {
        "alert_id": "0d6b9f35-2e81-4a47-93c0-7f1ae5b8d264", "entity_type": "transaction",
        "entity_id": "c41d8e02-77aa-4b19-8f3e-5d2a9b60e7f1", "merchant_id": "3f9c2a71-5b0e-4d6a-9c1d-2e8a7b4f10c5",
        "rule_id": "rule-large-txn-flag", "rule_type": "large_txn_flag", "risk_score": 19, "risk_score_delta": 5,
        "action": "flag_review", "action_by": "system_auto", "action_ts": "2025-03-01T22:30:12.000000Z",
        "resolved_ts": None, "outcome": None}),
    "loan_applications insert": envelope("loan_applications", "c", 26700416, 26700344, 760, 0, 1,
                                          "2025-03-01T08:15:00.000000Z", "2025-03-01", [], None, LOAN_AFTER),
    "loan_applications update": envelope("loan_applications", "u", 26912768, 26912700, 801, 0, 1,
                                          "2025-03-01T08:15:00.000000Z", "2025-03-01", [], LOAN_AFTER, DISBURSED),
    "loan_applications replayed disbursement": envelope("loan_applications", "u", 26912900, 26912800, 802, 0, 1,
                                                         "2025-03-01T08:15:00.000000Z", "2025-03-01", ["noop_update"],
                                                         DISBURSED, DISBURSED),
    "loan_repayments": envelope("loan_repayments", "c", 27300224, 27300152, 830, 0, 1, "2025-04-02T06:00:00.000000Z", "2025-04-02", [], None, {
        "repayment_id": "e5a02c93-18d4-4c7b-b0f6-3a9d8e1274bc", "loan_id": "b7e1d4a9-0c23-4f5e-a6b8-91d7c3e2f408",
        "merchant_id": "3f9c2a71-5b0e-4d6a-9c1d-2e8a7b4f10c5", "installment_number": 1, "due_date": "2025-04-02",
        "payment_ts": "2025-04-02T06:00:00.000000Z", "principal_due": 1000000, "interest_due": 70000,
        "total_due": 1070000, "amount_paid": 1070000, "outstanding_before": 6420000, "outstanding_after": 5350000,
        "debit_method": "debicheck", "mandate_id": "mnd-8c2f41a0", "mandate_status": "ACCP", "collection_status": "success",
        "response_code": "00", "retry_attempt": 0, "days_past_due": 0, "par_bucket": "current", "is_cured": False,
        "write_off_flag": False}),
    "loan_repayments unpaid": envelope("loan_repayments", "c", 27300300, 27300250, 831, 0, 1, "2025-05-01T22:00:00.000000Z", "2025-05-02",
                                        ["event_time_fallback"], None, {
        "repayment_id": "f6b13da4-29e5-4d8c-c107-4bae9f2385cd", "loan_id": "b7e1d4a9-0c23-4f5e-a6b8-91d7c3e2f408",
        "merchant_id": "3f9c2a71-5b0e-4d6a-9c1d-2e8a7b4f10c5", "installment_number": 2, "due_date": "2025-05-02",
        "payment_ts": None, "principal_due": 1000000, "interest_due": 70000, "total_due": 1070000, "amount_paid": 0,
        "outstanding_before": 5350000, "outstanding_after": 5350000, "debit_method": "debicheck",
        "mandate_id": "mnd-8c2f41a0", "mandate_status": "ACCP", "collection_status": "unpaid_nsf", "response_code": "51",
        "retry_attempt": 0, "days_past_due": 0, "par_bucket": "current", "is_cured": False, "write_off_flag": False}),
}


@pytest.mark.parametrize("name", EXAMPLES)
def test_the_contract_examples_are_valid_envelopes(name):
    assert Envelope.model_validate(EXAMPLES[name])


@pytest.mark.parametrize("name", EXAMPLES)
def test_the_contract_examples_round_trip_exactly(name):
    e = EXAMPLES[name]
    table = e["source"]["table"]
    same_event(to_event(table, to_row(e)), e)


def test_a_row_has_native_types():
    row = to_row(EXAMPLES["merchants"])
    assert type(row["event_date"]) is date and row["event_date"] == date(2025, 3, 1)
    assert type(row["event_ts"]) is datetime and row["event_ts"] == datetime(2025, 3, 1, 21, 59, tzinfo=UTC)
    assert row["event_ts"].utcoffset().total_seconds() == 0
    assert row["commit_lsn"] == 26556528 and row["transaction_xid"] == 741 and row["envelope_version"] == 1
    assert row["flags"] == [] and row["is_metro"] is True and row["has_active_loan"] is False
    assert row["onboarded_at"] == datetime(2025, 1, 13, 22, 0, tzinfo=UTC)


def test_microseconds_survive():
    e = changed(EXAMPLES["merchants"], ("event_ts",), "2025-03-01T21:59:00.123457Z")
    assert to_row(e)["event_ts"].microsecond == 123457
    same_event(to_event("merchants", to_row(e)), e)


def test_numeric_stays_its_exact_text():
    assert to_row(EXAMPLES["loan_applications insert"])["total_cost_percent"] == "0.0700"


def test_the_mapping_does_not_change_its_input_and_shares_no_lists_with_it():
    e = EXAMPLES["loan_applications update"]
    e = changed(e, ("after", "decline_reasons"), ["a", None])
    snapshot = deepcopy(e)
    row = to_row(e)
    assert e == snapshot
    row["decline_reasons"].append("x")
    row["flags"].append("noop_update")
    assert e == snapshot
    back = to_event("loan_applications", to_row(e))
    back["after"]["decline_reasons"].append("y")
    assert e == snapshot


# ---------------------------------------------------------------------------
# every field is accounted for (G2 Q1)
# ---------------------------------------------------------------------------

def event_field_paths() -> set[tuple[str, ...]]:
    paths = set()
    for name, info in Envelope.model_fields.items():
        sub = getattr(info.annotation, "model_fields", None)
        if sub:
            paths |= {(name, f.alias or n) for n, f in sub.items()}
        else:
            paths.add((name,))
    return paths


def test_every_event_field_is_a_bronze_column_an_image_or_on_the_dropped_list():
    mapped = {path for _, path, _ in ENVELOPE_FIELDS}
    images = {("before",), ("after",)}
    assert event_field_paths() == mapped | set(DROPPED_FIELDS) | images
    assert not mapped & set(DROPPED_FIELDS)


def test_the_dropped_fields_are_exactly_source_db_schema_and_table():
    assert set(DROPPED_FIELDS) == {("source", "db"), ("source", "schema"), ("source", "table")}


def test_the_envelope_columns_of_the_mapping_are_the_schemas_in_order():
    schema = build_schema("merchants")
    assert [c for c, _, _ in ENVELOPE_FIELDS] == [f.name for f in schema.fields[:13]]


@pytest.mark.parametrize("name", EXAMPLES)
def test_a_row_has_exactly_the_schema_columns_in_order(name):
    e = EXAMPLES[name]
    table = e["source"]["table"]
    assert list(to_row(e)) == [f.name for f in build_schema(table).fields]


def test_the_envelope_renames():
    row = to_row(EXAMPLES["merchants"])
    e = EXAMPLES["merchants"]
    assert row["transaction_xid"] == e["transaction"]["id"]
    assert row["transaction_position"] == e["transaction"]["position"] and row["transaction_size"] == e["transaction"]["size"]
    assert (row["commit_lsn"], row["change_lsn"]) == (e["source"]["commit_lsn"], e["source"]["change_lsn"])
    assert "transaction_id" not in row


# ---------------------------------------------------------------------------
# before images (G2 Q4 point 7)
# ---------------------------------------------------------------------------

def test_an_insert_has_every_before_column_null():
    row = to_row(EXAMPLES["loan_applications insert"])
    before = [c for c in row if c.startswith("before_")]
    assert len(before) == 24 and all(row[c] is None for c in before)


def test_an_update_copies_the_before_image_into_the_before_columns():
    row = to_row(EXAMPLES["loan_applications update"])
    assert row["before_disbursed_at"] is None and row["disbursed_at"] == datetime(2025, 3, 3, 9, 30, tzinfo=UTC)
    assert row["before_disbursement_amount_minor"] is None and row["disbursement_amount_minor"] == 6000000
    assert row["before_application_id"] == row["application_id"] == LOAN_AFTER["application_id"]
    assert row["before_decline_reasons"] == []


def test_an_update_with_a_null_before_disbursed_at_is_an_update_not_an_insert_after_the_round_trip():
    row = to_row(EXAMPLES["loan_applications update"])
    assert row["before_disbursed_at"] is None and row["op"] == "u"
    back = to_event("loan_applications", row)
    assert back["op"] == "u" and back["before"] is not None and back["before"]["disbursed_at"] is None


def test_an_insert_with_all_before_columns_null_is_an_insert_after_the_round_trip():
    back = to_event("loan_applications", to_row(EXAMPLES["loan_applications insert"]))
    assert back["op"] == "c" and back["before"] is None


def test_a_replayed_disbursement_keeps_its_flag_and_equal_images():
    row = to_row(EXAMPLES["loan_applications replayed disbursement"])
    assert row["flags"] == ["noop_update"]
    assert all(row["before_" + c] == row[c] for c in EXPECTED_COLUMNS["loan_applications"])


def test_the_update_lands_in_the_month_of_its_insert():
    insert, update = to_row(EXAMPLES["loan_applications insert"]), to_row(EXAMPLES["loan_applications update"])
    assert insert["event_date"] == update["event_date"]
    late = changed(changed(EXAMPLES["loan_applications update"], ("event_ts",), "2025-06-30T21:59:59.999999Z"),
                   ("event_date",), "2025-06-30")
    assert to_row(late)["event_date"].replace(day=1) == date(2025, 6, 1)


# ---------------------------------------------------------------------------
# the round trip over awkward values
# ---------------------------------------------------------------------------

def base(table):
    op = "c"
    return make(table, op)


def optional_columns(table):
    return [c for c, _ in contract.TABLES[table]]


NULL_CASES = [(t, c) for t in TABLES for c in optional_columns(t)]


@pytest.mark.parametrize("table,column", NULL_CASES)
def test_a_null_in_any_table_column_round_trips_and_reads_back_as_null(table, column):
    e = changed(base(table), ("after", column), None)
    row = to_row(e)
    assert row[column] is None
    same_event(to_event(table, row), e)


@pytest.mark.parametrize("column", EXPECTED_COLUMNS["loan_applications"])
def test_a_null_in_any_before_column_of_an_update_round_trips(column):
    e = changed(make("loan_applications", "u"), ("before", column), None)
    row = to_row(e)
    assert row["before_" + column] is None
    same_event(to_event("loan_applications", row), e)


@pytest.mark.parametrize("table,column", sorted(INTEGER_WIDTHS))
@pytest.mark.parametrize("which", ["low", "high"])
def test_every_integer_column_round_trips_at_the_limits_of_its_width(table, column, which):
    bits = INTEGER_WIDTHS[(table, column)]
    value = -(1 << (bits - 1)) if which == "low" else (1 << (bits - 1)) - 1
    e = changed(base(table), ("after", column), value)
    row = to_row(e)
    assert row[column] == value and type(row[column]) is int
    same_event(to_event(table, row), e)


@pytest.mark.parametrize("text", ["", " ", "Zoë Müller", "日本語", "emoji \U0001F680 beyond the BMP", 'quote " and \\ backslash',
                                  "line\nbreak\tand\x01control", "x" * 20000, "\u2028\u2029\u0085", "0.0700", "null", "true"])
def test_awkward_text_round_trips(text):
    e = changed(base("merchants"), ("after", "province"), text)
    row = to_row(e)
    assert row["province"] == text
    same_event(to_event("merchants", row), e)


@pytest.mark.parametrize("reasons", [[], None, [None], ["a", None, "b"], ["", "x"], ["日本", "\U0001F680"], ["a"] * 500])
def test_list_columns_round_trip_including_null_empty_and_null_elements(reasons):
    e = changed(make("loan_applications", "u"), ("after", "decline_reasons"), reasons)
    e = changed(e, ("before", "decline_reasons"), reasons)
    row = to_row(e)
    assert row["decline_reasons"] == reasons and row["before_decline_reasons"] == reasons
    same_event(to_event("loan_applications", row), e)


@pytest.mark.parametrize("flags", [[], ["noop_update"], ["event_time_fallback"], ["noop_update", "event_time_fallback"],
                                   ["event_time_fallback", "noop_update"]])
def test_flags_round_trip_in_order(flags):
    e = changed(base("merchants"), ("flags",), flags)
    same_event(to_event("merchants", to_row(e)), e)


@pytest.mark.parametrize("event_ts,event_date", [
    ("2025-02-28T22:00:00.000000Z", "2025-03-01"),     # the first instant of 1 March in South Africa
    ("2025-02-28T21:59:59.999999Z", "2025-02-28"),     # the last microsecond of 28 February
    ("2025-03-01T00:00:00.000000Z", "2025-03-01"),
    ("2025-12-31T22:00:00.000000Z", "2026-01-01"),     # across the year
    ("2024-02-28T22:00:00.000000Z", "2024-02-29"),     # a leap day
    ("2024-02-29T22:00:00.000000Z", "2024-03-01"),
    ("0001-01-01T00:00:00.000000Z", "0001-01-01"),
    ("9999-12-31T21:59:59.999999Z", "9999-12-31"),
])
def test_the_south_african_date_boundary(event_ts, event_date):
    e = changed(changed(base("merchants"), ("event_ts",), event_ts), ("event_date",), event_date)
    row = to_row(e)
    assert row["event_date"] == date.fromisoformat(event_date)
    same_event(to_event("merchants", row), e)


@pytest.mark.parametrize("number", ["0.0700", "0", "-1", "12345.6789", "007", "-0.5", "100"])
def test_numeric_text_round_trips(number):
    e = changed(make("loan_applications", "c"), ("after", "total_cost_percent"), number)
    row = to_row(e)
    assert row["total_cost_percent"] == number
    same_event(to_event("loan_applications", row), e)


@pytest.mark.parametrize("path,value", [
    (("transaction", "id"), (1 << 63) - 1), (("transaction", "id"), 0), (("source", "commit_lsn"), (1 << 63) - 1),
    (("source", "change_lsn"), 0), (("source", "change_lsn"), (1 << 63) - 1), (("source", "commit_lsn"), 2 ** 40),
    (("source", "change_lsn"), 2 ** 40), (("transaction", "id"), 2 ** 40), (("ts_ms"), 0), (("ts_ms"), (1 << 63) - 1),
    (("transaction", "position"), 2 ** 31 - 2), (("transaction", "size"), 2 ** 31 - 1),
])
def test_envelope_integers_round_trip_at_their_limits(path, value):
    path = (path,) if isinstance(path, str) else path
    e = changed(base("merchants"), path, value)
    if path == ("transaction", "position"):
        e = changed(e, ("transaction", "size"), 2 ** 31 - 1)
    same_event(to_event("merchants", to_row(e)), e)


def test_the_source_database_is_the_argument_in_both_directions():
    e = changed(base("merchants"), ("source", "db"), "other_db")
    row = event_to_row(e, source_db="other_db")
    same_event(row_to_event("merchants", row, source_db="other_db"), e)


# ---------------------------------------------------------------------------
# wrong shapes are refused, naming the table and the column
# ---------------------------------------------------------------------------

class Text(str):
    """A str subclass: equal to its text, but not what the JSON reader produces."""


BAD = {
    "string": [5, True, ["a"], {"a": 1}, 1.5, "\ud800", Text("x")],
    "int": ["7", 7.0, 1.5, True, False, 2 ** 31, -2 ** 31 - 1, [7]],
    "long": ["7", 7.0, True, 2 ** 63, -2 ** 63 - 1, [7]],
    "boolean": ["true", "t", 1, 0, "false", [True]],
    "date": ["2025-3-1", "20250301", "2025-02-30", "2025-13-01", "2025-03-01\n", " 2025-03-01", "2025-03-01T00:00:00.000000Z",
             "２０２５-03-01", 20250301, "0000-01-01", "2025-03-01 "],
    "timestamptz": ["2025-03-01 10:00:00.000000Z", "2025-03-01T10:00:00Z", "2025-03-01T10:00:00.000Z", "2025-03-01T10:00:00.000000",
                    "2025-03-01T10:00:00.000000+00:00", "2025-03-01T10:00:00.000000z", "2025-13-01T10:00:00.000000Z",
                    "2025-03-01T24:00:00.000000Z", "2025-03-01T10:00:00.0000000Z", "2025-03-01T10:00:00.000000Z\n",
                    "２０２５-03-01T10:00:00.000000Z", 1740823200, "2025-03-01"],
    "list of string": ["a,b", "{}", ["a", 1], [["a"]], {"a": 1}, [True], [1.5], 5, ("a",), ["a", "\ud800"], [Text("a")]],
}
NUMERIC_BAD = [0.07, 7, "0,07", "abc", "", "1e3", "0.07\n", " 0.07", ".07", "1.", "+1", "1_0", "０.０７", True, ["0.07"]]


def bad_cases():
    for table in TABLES:
        for column, kind in contract.TABLES[table]:
            values = NUMERIC_BAD if column == "total_cost_percent" else BAD[kind]
            for value in values:
                yield pytest.param(table, column, value, id=f"{table}.{column}={value!r}"[:90])


@pytest.mark.parametrize("table,column,value", list(bad_cases()))
def test_a_wrong_shape_in_any_column_is_refused_and_named(table, column, value):
    e = changed(base(table), ("after", column), value)
    with pytest.raises(MappingError) as caught:
        to_row(e)
    message = str(caught.value)
    assert message.startswith(f"{table}.{column}: expected "), message
    assert caught.value.table == table and caught.value.column == column
    assert isinstance(caught.value, ContractError) and exit_code(caught.value) == 2


@pytest.mark.parametrize("column,value", [("disbursed_at", "2025-03-03 09:30:00"), ("months_trading", "38"),
                                          ("declared_monthly_revenue", 1.5), ("decline_reasons", "a"),
                                          ("total_cost_percent", 0.07), ("application_id", 5)])
def test_a_wrong_shape_in_the_before_image_is_named_with_its_before_column(column, value):
    e = changed(make("loan_applications", "u"), ("before", column), value)
    with pytest.raises(MappingError, match=f"loan_applications.before_{column}: expected .*\\(event field before.{column}\\)"):
        to_row(e)


ENVELOPE_PATHS = {
    "op": ("op",), "event_date": ("event_date",), "event_ts": ("event_ts",), "commit_lsn": ("source", "commit_lsn"),
    "change_lsn": ("source", "change_lsn"), "commit_ts": ("source", "commit_ts"),
    "transaction_position": ("transaction", "position"), "transaction_size": ("transaction", "size"),
    "transaction_xid": ("transaction", "id"), "ts_ms": ("ts_ms",), "ingested_at": ("ingested_at",), "flags": ("flags",),
    "envelope_version": ("envelope_version",),
}


def test_the_test_table_of_envelope_paths_is_the_contract_envelope():
    assert list(ENVELOPE_PATHS) == [name for name, _ in contract.ENVELOPE]


@pytest.mark.parametrize("column", ENVELOPE_PATHS)
def test_a_null_in_any_required_envelope_column_is_refused(column):
    e = changed(base("merchants"), ENVELOPE_PATHS[column], None)
    with pytest.raises(MappingError, match=f"merchants.{column}: expected .*got null"):
        to_row(e)


ENVELOPE_BAD = [
    ("op", "d"), ("op", "r"), ("op", "t"), ("op", "C"), ("op", 1), ("op", ["c"]), ("op", Text("c")),
    ("envelope_version", 2), ("envelope_version", 0), ("envelope_version", "1"), ("envelope_version", 1.0),
    ("envelope_version", True),
    ("event_date", "2025-3-1"), ("event_ts", "2025-03-01 08:00:00"), ("commit_ts", "2025-03-01T08:00:00Z"),
    ("ingested_at", 1793094067), ("event_ts", 5), ("ingested_at", "2025-03-01 08:00:00"), ("commit_ts", 1.5),
    ("event_ts", "2025-03-01"), ("event_date", "2025-03-01T00:00:00.000000Z"), ("event_date", 20250301),
    ("commit_lsn", -1), ("commit_lsn", 2 ** 63), ("commit_lsn", "26556528"), ("commit_lsn", 1.5), ("commit_lsn", True),
    ("change_lsn", -1), ("change_lsn", 2 ** 63), ("change_lsn", "1"),
    ("transaction_position", -1), ("transaction_position", 2 ** 31), ("transaction_position", "0"),
    ("transaction_size", 0), ("transaction_size", -1), ("transaction_size", 2 ** 31), ("transaction_size", 1.0),
    ("transaction_xid", -1), ("transaction_xid", 2 ** 63), ("transaction_xid", "7"),
    ("ts_ms", -1), ("ts_ms", 2 ** 63), ("ts_ms", 1.5),
    ("flags", ["surprise"]), ("flags", "noop_update"), ("flags", [None]), ("flags", [1]), ("flags", ["noop_update", "x"]),
    ("flags", ""), ("flags", ("noop_update",)), ("flags", [Text("noop_update")]), ("flags", {"noop_update": 1}),
]


@pytest.mark.parametrize("column,value", ENVELOPE_BAD, ids=[f"{c}={v!r}" for c, v in ENVELOPE_BAD])
def test_a_wrong_envelope_value_is_refused_and_named(column, value):
    e = changed(base("merchants"), ENVELOPE_PATHS[column], value)
    with pytest.raises(MappingError, match=f"merchants.{column}: "):
        to_row(e)


def test_an_envelope_error_names_the_event_field_only_when_the_names_differ():
    for column, path in ENVELOPE_PATHS.items():
        e = changed(base("merchants"), path, "bad")
        with pytest.raises(MappingError) as caught:
            to_row(e)
        shown = "(event field" in str(caught.value)
        assert shown == (path != (column,)), str(caught.value)


def test_an_envelope_field_error_names_the_event_field_when_the_names_differ():
    e = changed(base("merchants"), ("transaction", "id"), -5)
    with pytest.raises(MappingError, match=r"merchants.transaction_xid: .*\(event field transaction.id\)"):
        to_row(e)


def test_the_transaction_position_must_be_inside_the_transaction():
    e = changed(changed(base("merchants"), ("transaction", "position"), 3), ("transaction", "size"), 3)
    with pytest.raises(MappingError, match="transaction_position: 3 is not inside a transaction of 3"):
        to_row(e)


@pytest.mark.parametrize("event_ts,event_date", [
    ("2025-02-28T22:00:00.000000Z", "2025-02-28"),     # the UTC date, not the South African one
    ("2025-02-28T21:59:59.999999Z", "2025-03-01"),
    ("2025-03-01T08:00:00.000000Z", "2025-03-02"), ("2025-03-01T08:00:00.000000Z", "2025-02-28"),
    ("9999-12-31T22:00:00.000000Z", "9999-12-31"),     # its South African date would be in year 10000
])
def test_event_date_must_be_the_south_african_date_of_event_ts(event_ts, event_date):
    e = changed(changed(base("merchants"), ("event_ts",), event_ts), ("event_date",), event_date)
    with pytest.raises(MappingError, match="event_date: .* is not the South African date of event_ts"):
        to_row(e)


def test_the_source_database_must_be_the_warehouses():
    e = changed(base("merchants"), ("source", "db"), "fintech")
    with pytest.raises(MappingError, match=r"merchants.source.db: expected \"fintech_cdc\".*got \"fintech\""):
        to_row(e)
    assert event_to_row(e, source_db="fintech")["op"] == "c"


@pytest.mark.parametrize("schema", ["private", "", "PUBLIC", None, 5])
def test_the_source_schema_must_be_public(schema):
    e = changed(base("merchants"), ("source", "schema"), schema)
    with pytest.raises(MappingError, match="merchants.source.schema: expected \"public\""):
        to_row(e)


@pytest.mark.parametrize("table", ["orders", "", None, 5, "Merchants", ["merchants"]])
def test_the_source_table_must_be_one_of_the_five(table):
    e = changed(base("merchants"), ("source", "table"), table)
    with pytest.raises(MappingError, match="source.table: expected one of merchants, merchant_transactions"):
        to_row(e)


# ---------------------------------------------------------------------------
# structure: missing, extra and renamed fields
# ---------------------------------------------------------------------------

def test_a_missing_top_level_field_is_named():
    with pytest.raises(MappingError, match="merchants.event: fields differ from the contract \\(missing flags\\)"):
        to_row(changed(base("merchants"), ("flags",), DELETE))


def test_an_extra_top_level_field_is_named():
    with pytest.raises(MappingError, match="merchants.event: fields differ from the contract \\(unexpected note\\)"):
        to_row(changed(base("merchants"), ("note",), "x"))


def test_a_missing_and_an_extra_field_are_both_named():
    e = changed(changed(base("merchants"), ("flags",), DELETE), ("labels",), [])
    with pytest.raises(MappingError, match="missing flags; unexpected labels"):
        to_row(e)


@pytest.mark.parametrize("section,key", [("source", "commit_ts"), ("source", "db"), ("transaction", "size"), ("transaction", "id")])
def test_a_missing_source_or_transaction_field_is_named(section, key):
    with pytest.raises(MappingError, match=f"{section}: fields differ from the contract \\(missing {key}\\)"):
        to_row(changed(base("merchants"), (section, key), DELETE))


@pytest.mark.parametrize("section", ["source", "transaction"])
def test_an_extra_source_or_transaction_field_is_named(section):
    with pytest.raises(MappingError, match=f"{section}: fields differ from the contract \\(unexpected extra\\)"):
        to_row(changed(base("merchants"), (section, "extra"), 1))


@pytest.mark.parametrize("image", ["after", "before"])
def test_a_missing_extra_and_renamed_column_is_named(image):
    table, op = "loan_applications", "u"
    e = make(table, op)
    dropped = changed(e, (image, "fica_status"), DELETE)
    with pytest.raises(MappingError, match=f"loan_applications.{image}: fields differ from the contract \\(missing fica_status\\)"):
        to_row(dropped)
    extra = changed(e, (image, "score"), 1)
    with pytest.raises(MappingError, match=f"{image}: fields differ from the contract \\(unexpected score\\)"):
        to_row(extra)
    renamed = deepcopy(e)
    renamed[image]["kyc_status"] = renamed[image].pop("fica_status")
    with pytest.raises(MappingError, match="missing fica_status; unexpected kyc_status"):
        to_row(renamed)


@pytest.mark.parametrize("bad", [None, [], "x", 5])
def test_a_source_or_transaction_that_is_not_an_object_is_refused(bad):
    with pytest.raises(MappingError):
        to_row(changed(base("merchants"), ("source",), bad))
    with pytest.raises(MappingError, match="transaction: expected an object"):
        to_row(changed(base("merchants"), ("transaction",), bad))


@pytest.mark.parametrize("bad", [None, [], "x", 5])
def test_an_image_that_is_not_an_object_is_refused(bad):
    with pytest.raises(MappingError, match="after: expected an object"):
        to_row(changed(base("merchants"), ("after",), bad))


@pytest.mark.parametrize("bad", [None, [], "x", 5])
def test_an_event_that_is_not_an_object_is_refused(bad):
    with pytest.raises(MappingError, match="event: expected an object"):
        to_row(bad)


def test_a_dict_subclass_is_not_an_object_for_the_mapping():
    from collections import OrderedDict
    with pytest.raises(MappingError, match="after: expected an object"):
        to_row(changed(base("merchants"), ("after",), OrderedDict(base("merchants")["after"])))


def test_when_the_table_cannot_be_told_the_fields_of_source_are_reported_first():
    e = changed(changed(base("merchants"), ("source", "table"), "orders"), ("source", "commit_ts"), DELETE)
    with pytest.raises(MappingError, match="^source: fields differ from the contract \\(missing commit_ts\\)"):
        to_row(e)
    with pytest.raises(MappingError, match="^source: fields differ from the contract \\(missing table\\)"):
        to_row(changed(base("merchants"), ("source", "table"), DELETE))


def test_an_event_with_no_source_says_so_rather_than_naming_a_table():
    with pytest.raises(MappingError, match="event: fields differ from the contract \\(missing source\\)"):
        to_row(changed(base("merchants"), ("source",), DELETE))


# ---------------------------------------------------------------------------
# operations
# ---------------------------------------------------------------------------

def test_an_insert_with_a_before_image_is_refused():
    e = changed(make("loan_applications", "c"), ("before",), row_image("loan_applications"))
    with pytest.raises(MappingError, match="loan_applications.before: an insert has no before image"):
        to_row(e)


@pytest.mark.parametrize("table", ["merchants", "merchant_transactions", "loan_repayments", "risk_events"])
def test_a_before_image_on_a_table_that_is_never_updated_is_refused(table):
    e = changed(base(table), ("before",), row_image(table))
    with pytest.raises(MappingError, match=f"{table}.before: {table} has no before image"):
        to_row(e)


def test_an_update_with_no_before_image_is_refused():
    with pytest.raises(MappingError, match="loan_applications.before: expected an object, got null"):
        to_row(changed(make("loan_applications", "u"), ("before",), None))


@pytest.mark.parametrize("table", ["merchants", "merchant_transactions", "loan_repayments", "risk_events"])
def test_an_update_on_any_table_but_loan_applications_is_refused(table):
    e = changed(changed(base(table), ("op",), "u"), ("before",), row_image(table))
    with pytest.raises(MappingError, match=f"{table}.op: an update on {table}"):
        to_row(e)


def row_image(table):
    return deepcopy(make(table, "c")["after"])


# ---------------------------------------------------------------------------
# the mapping and the Envelope model agree on envelope level faults
# ---------------------------------------------------------------------------

ENVELOPE_FAULTS = [
    ("version 2", ("envelope_version",), 2),
    ("version as text", ("envelope_version",), "1"),
    ("op d", ("op",), "d"), ("op r", ("op",), "r"),
    ("event_ts with a space", ("event_ts",), "2025-03-01 08:00:00"),
    ("commit_ts without microseconds", ("source", "commit_ts"), "2025-03-01T08:00:00Z"),
    ("ingested_at a number", ("ingested_at",), 1793094067),
    ("event_date short", ("event_date",), "2025-3-1"),
    ("position past size", ("transaction", "position"), 5),
    ("size zero", ("transaction", "size"), 0),
    ("negative position", ("transaction", "position"), -1),
    ("negative xid", ("transaction", "id"), -1),
    ("negative ts_ms", ("ts_ms",), -1),
    ("negative commit_lsn", ("source", "commit_lsn"), -1),
    ("negative change_lsn", ("source", "change_lsn"), -1),
    ("unknown flag", ("flags",), ["surprise"]),
    ("flags as text", ("flags",), "noop_update"),
    ("schema other", ("source", "schema"), "other"),
    ("table unknown", ("source", "table"), "orders"),
    ("extra top level field", ("extra",), 1), ("missing flags", ("flags",), DELETE), ("missing after", ("after",), DELETE),
    ("extra source field", ("source", "extra"), 1), ("missing transaction field", ("transaction", "size"), DELETE),
]


@pytest.mark.parametrize("name", EXAMPLES)
def test_both_accept_every_valid_example(name):
    e = EXAMPLES[name]
    assert Envelope.model_validate(e)
    to_row(e)


@pytest.mark.parametrize("name,path,value", ENVELOPE_FAULTS, ids=[f[0] for f in ENVELOPE_FAULTS])
def test_both_refuse_every_envelope_level_fault(name, path, value):
    e = changed(base("merchants"), path, value)
    with pytest.raises(Exception):
        Envelope.model_validate(e)
    with pytest.raises(MappingError):
        to_row(e)


def test_both_refuse_a_before_image_on_an_insert_and_a_missing_one_on_an_update():
    insert = changed(make("loan_applications", "c"), ("before",), row_image("loan_applications"))
    update = changed(make("loan_applications", "u"), ("before",), None)
    for e in (insert, update):
        with pytest.raises(Exception):
            Envelope.model_validate(e)
        with pytest.raises(MappingError):
            to_row(e)


def test_the_mapping_is_stricter_than_the_envelope_model_in_value_shapes():
    """The Envelope model checks that before and after are objects; the mapping checks every value. The
    Envelope also accepts any database name, any date that fits the pattern and updates on any table."""
    for e in (changed(base("merchants"), ("after", "risk_score"), "14"),
              changed(base("merchants"), ("after", "snapshot_at"), "2025-03-01 21:59:00"),
              changed(base("merchants"), ("source", "db"), "fintech"),
              changed(changed(base("merchants"), ("event_ts",), "2025-03-01T08:00:00.000000Z"), ("event_date",), "2025-02-30"),
              changed(changed(base("merchants"), ("op",), "u"), ("before",), row_image("merchants"))):
        assert Envelope.model_validate(e)
        with pytest.raises(MappingError):
            to_row(e)


# ---------------------------------------------------------------------------
# the inverse is as strict
# ---------------------------------------------------------------------------

def row_of(table, op="c"):
    return to_row(make(table, op))


def test_row_to_event_refuses_an_unknown_table():
    with pytest.raises(MappingError, match="table: expected one of"):
        to_event("orders", row_of("merchants"))


@pytest.mark.parametrize("bad", [None, [], "x"])
def test_row_to_event_refuses_a_row_that_is_not_an_object(bad):
    with pytest.raises(MappingError, match="merchants.row: expected an object"):
        to_event("merchants", bad)


def test_row_to_event_refuses_a_missing_and_an_extra_column():
    row = row_of("merchants")
    del row["province"]
    with pytest.raises(MappingError, match="merchants.row: fields differ from the contract \\(missing province\\)"):
        to_event("merchants", row)
    row = row_of("merchants")
    row["surprise"] = 1
    with pytest.raises(MappingError, match="unexpected surprise"):
        to_event("merchants", row)


def test_row_to_event_refuses_before_columns_on_a_table_without_them():
    row = row_of("merchants")
    row["before_province"] = None
    with pytest.raises(MappingError, match="unexpected before_province"):
        to_event("merchants", row)


def test_row_to_event_refuses_a_before_column_set_on_an_insert():
    row = row_of("loan_applications", "c")
    row["before_months_trading"] = 5
    with pytest.raises(MappingError, match="loan_applications.before_months_trading: an insert has no before image"):
        to_event("loan_applications", row)


@pytest.mark.parametrize("column,value", [
    ("event_ts", datetime(2025, 3, 1, 8, 0)),                       # naive
    ("event_ts", "2025-03-01T08:00:00.000000Z"),                    # the event's text, not a datetime
    ("event_date", datetime(2025, 3, 1, 0, 0, tzinfo=UTC)),         # a datetime where a date goes
    ("event_date", "2025-03-01"),
    ("commit_lsn", True), ("commit_lsn", 1.0), ("commit_lsn", "5"), ("commit_lsn", 2 ** 63),
    ("transaction_position", 2 ** 31), ("flags", ["surprise"]), ("flags", None), ("op", "d"), ("envelope_version", 2),
    ("is_metro", 1), ("province", 5), ("risk_score", 2 ** 31), ("onboarded_at", "2025-01-13T22:00:00.000000Z"),
    ("onboarded_at", datetime(2025, 1, 13, 22, 0)),
])
def test_row_to_event_refuses_a_value_of_the_wrong_type(column, value):
    row = row_of("merchants")
    row[column] = value
    with pytest.raises(MappingError, match=f"merchants.{column}: expected "):
        to_event("merchants", row)


def test_row_to_event_refuses_an_event_date_that_is_not_the_south_african_date_of_event_ts():
    row = row_of("merchants")
    row["event_date"] = date(2030, 1, 1)
    with pytest.raises(MappingError, match="event_date: .* is not the South African date"):
        to_event("merchants", row)


def test_row_to_event_refuses_an_update_row_of_a_table_that_is_never_updated():
    row = row_of("merchants")
    row["op"] = "u"
    with pytest.raises(MappingError, match="merchants.op: an update on merchants"):
        to_event("merchants", row)


def test_row_to_event_turns_a_datetime_in_another_offset_into_utc():
    row = row_of("merchants")
    row["event_ts"] = datetime(2025, 3, 1, 10, 0, tzinfo=timezone(timedelta(hours=2)))
    row["event_date"] = date(2025, 3, 1)
    assert row_to_event("merchants", row, source_db=DB)["event_ts"] == "2025-03-01T08:00:00.000000Z"


def test_the_south_african_date_is_worked_out_from_utc_not_from_the_offset_the_datetime_carries():
    """01:00 at +05:00 on 1 March is 20:00 UTC on 28 February, which is 22:00 on 28 February in South Africa."""
    row = row_of("merchants")
    row["event_ts"] = datetime(2025, 3, 1, 1, 0, tzinfo=timezone(timedelta(hours=5)))
    row["event_date"] = date(2025, 2, 28)
    assert row_to_event("merchants", row, source_db=DB)["event_ts"] == "2025-02-28T20:00:00.000000Z"
    row["event_date"] = date(2025, 3, 1)
    with pytest.raises(MappingError, match="not the South African date"):
        row_to_event("merchants", row, source_db=DB)


def test_a_mapping_error_message_can_be_given_a_location():
    error = MappingError("merchants", "risk_score", "expected an integer, got \"x\"")
    error.where = "cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_a_b.ndjson.gz, event 7: "
    assert str(error) == ("cdc/cdc_r01/merchants/event_date=2025-03-01/lsn_a_b.ndjson.gz, event 7: "
                          "merchants.risk_score: expected an integer, got \"x\"")


def test_a_long_value_is_cut_in_the_message_and_text_is_shown_ascii_escaped():
    with pytest.raises(MappingError) as caught:
        to_row(changed(base("merchants"), ("after", "risk_score"), "x" * 500))
    assert "..." in str(caught.value) and len(str(caught.value)) < 200
    with pytest.raises(MappingError) as caught:
        to_row(changed(base("merchants"), ("after", "risk_score"), "日本"))
    assert str(caught.value).isascii() and "\\u65e5" in str(caught.value)


def test_a_value_that_is_not_json_is_still_shown():
    with pytest.raises(MappingError, match=r"merchants.risk_score: expected an integer from -2147483648 to 2147483647, got \{1\}"):
        to_row(changed(base("merchants"), ("after", "risk_score"), {1}))


def test_a_mapping_error_without_a_table_names_just_the_field():
    assert str(MappingError(None, "source.table", "expected x")) == "source.table: expected x"


# ---------------------------------------------------------------------------
# the synthetic landings used by the index and memory tests are shaped like the real thing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table,op", [(t, "c") for t in TABLES] + [("loan_applications", "u")])
def test_a_synthetic_event_maps_and_round_trips(table, op):
    e = make(table, op, flags=["noop_update"] if op == "u" else [])
    same_event(to_event(table, to_row(e)), e)


def test_every_line_of_the_lazy_landing_maps():
    from cdc.dedup import events_in
    from tests.bronze_synth import LazyLanding

    landing = LazyLanding(3, tx_size=30, merchants=10)
    seen = 0
    for key in landing.list_keys(landing.prefix):
        for e in events_in(landing.read(key)):
            to_row(e)
            seen += 1
    assert seen == 90
