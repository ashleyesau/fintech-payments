"""tests/test_bronze_drift.py: G2 step 3, drift stops pass 1 end to end (GCS Plan (V2) sections 10 and 15).

The landed events are changed on the way into a synthetic landing and run through the real path: the
landing reader, summarise_object, the index and, for the exit code and the message a person reads, the
bronze.check command. The five kinds the G2 done-when names: an extra column, a missing column, a renamed
column, a retyped column and a wrong envelope version. The first, second, third and fifth are structural
(EventError, bronze/events.py); a retyped column is a value of the wrong shape (MappingError, bronze/mapping.py),
which is the check pass 1 gained in step 3. Every stop is exit code 2, names the object and the event, and
happens before pass 2, so nothing can have been committed.
"""
import io
from copy import deepcopy

import pytest

from bronze.check import EXIT_CONTRACT, EXIT_OK, execute
from bronze.config import CREDENTIALS_VARIABLE, BronzeSettings
from bronze.errors import ContractError, EventError, MappingError
from bronze.index import build_index
from cdc.columns import EXPECTED_COLUMNS
from tests import bronze_contract as contract
from tests.bronze_synth import DAY, FIRST_LSN, LSN_STEP, SOURCE_DB, Landing, event

TABLES = tuple(EXPECTED_COLUMNS)


@pytest.fixture(autouse=True)
def default_settings(monkeypatch):
    for key in ("CDC_GCS_BUCKET", "CDC_SLOT_NAME", "CDC_DB", "BRONZE_READ_WORKERS", "BRONZE_BUCKET"):
        monkeypatch.delenv(key, raising=False)


def objects(table="merchants", count=3, op="c"):
    """One transaction of `count` changes of one table, as the events the consumer would write."""
    return [event(table, op, FIRST_LSN + LSN_STEP, position, count, serial=position) for position in range(count)]


def land(events, table="merchants"):
    """A landing whose only object holds `events`, and its object key."""
    landing = Landing()
    key = landing.put(table, DAY, events)
    return landing, key


def edit(events, number, change):
    """A copy of `events` in which event `number` (1 based) has been through change(event)."""
    copy = deepcopy(events)
    change(copy[number - 1])
    return copy


def stop(landing):
    """Run pass 1 and return what it raised."""
    with pytest.raises(ContractError) as caught:
        build_index(landing.reader(), 2, source_db=SOURCE_DB)
    return caught.value


def command(landing, tmp_path):
    file = tmp_path / "credentials.json"
    file.write_text("{}")
    out, err = io.StringIO(), io.StringIO()
    code = execute(BronzeSettings(_env_file=None), reader_factory=lambda settings: landing.reader(),
                   environ={CREDENTIALS_VARIABLE: str(file)}, out=out, err=err)
    return code, out.getvalue(), err.getvalue()


def test_clean_events_pass_every_table():
    for table in TABLES:
        landing, _ = land(objects(table), table)
        assert build_index(landing.reader(), 2, source_db=SOURCE_DB).summary().events_read == 3


# ---------------------------------------------------------------------------
# the five kinds
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table", TABLES)
def test_an_extra_column_stops_pass_1_and_is_named(table):
    events = edit(objects(table), 2, lambda e: e["after"].update(surprise=1))
    landing, key = land(events, table)
    error = stop(landing)
    assert isinstance(error, EventError)
    assert str(error).startswith(f"{key}, event 2: after columns differ from the {table} model")
    assert "unexpected surprise" in str(error) and "new Bronze contract version" in str(error)


@pytest.mark.parametrize("table", TABLES)
def test_a_missing_column_stops_pass_1_and_is_named(table):
    column = EXPECTED_COLUMNS[table][-1]
    events = edit(objects(table), 3, lambda e: e["after"].pop(column))
    landing, key = land(events, table)
    error = stop(landing)
    assert isinstance(error, EventError)
    assert str(error).startswith(f"{key}, event 3: after columns differ") and f"missing {column}" in str(error)


@pytest.mark.parametrize("table", TABLES)
def test_a_renamed_column_stops_pass_1_and_names_both_names(table):
    old = EXPECTED_COLUMNS[table][0]

    def rename(e):
        e["after"] = {("renamed_" + k if k == old else k): v for k, v in e["after"].items()}

    landing, key = land(edit(objects(table), 1, rename), table)
    error = stop(landing)
    assert isinstance(error, EventError)
    assert f"missing {old}" in str(error) and f"unexpected renamed_{old}" in str(error)
    assert str(error).startswith(f"{key}, event 1: ")


def test_a_renamed_column_in_the_before_image_of_an_update_is_named():
    def rename(e):
        e["before"] = {("kyc_status" if k == "fica_status" else k): v for k, v in e["before"].items()}

    landing, key = land(edit(objects("loan_applications", 1, "u"), 1, rename), "loan_applications")
    error = stop(landing)
    assert isinstance(error, EventError) and "before columns differ" in str(error)
    assert "missing fica_status" in str(error) and "unexpected kyc_status" in str(error)


@pytest.mark.parametrize("version", [2, 0, "1", 1.0, None])
def test_a_wrong_envelope_version_stops_pass_1(version):
    landing, key = land(edit(objects(), 2, lambda e: e.update(envelope_version=version)))
    error = stop(landing)
    assert isinstance(error, EventError) and str(error).startswith(f"{key}, event 2: envelope_version is")


# A retyped column: one planted fault for each kind of type the contract has. Each is a value of the wrong
# shape for its column, so the structural checks pass it and only the mapping can stop it.
RETYPES = [
    ("merchants", "risk_score", "14", "int as text"),
    ("merchants", "risk_score", 14.5, "int as a float"),
    ("merchants", "risk_score", 2 ** 31, "int outside its width"),
    ("merchants", "province", 5, "text as an integer"),
    ("merchants", "is_metro", "true", "boolean as text"),
    ("merchants", "snapshot_at", "2025-03-01 21:59:00", "timestamp with a space"),
    ("merchants", "snapshot_at", "2025-03-01T21:59:00Z", "timestamp without microseconds"),
    ("merchants", "snapshot_at", 1740866340, "timestamp as an epoch number"),
    ("merchant_transactions", "amount_minor", "12550", "long as text"),
    ("merchant_transactions", "amount_minor", 2 ** 63, "long outside its width"),
    ("merchant_transactions", "settlement_date", "2025-3-3", "short date"),
    ("merchant_transactions", "settlement_date", "2025-02-30", "impossible date"),
    ("loan_applications", "decline_reasons", "insufficient_trading_history", "list as text"),
    ("loan_applications", "decline_reasons", [1, 2], "list of numbers"),
    ("loan_applications", "total_cost_percent", 0.07, "numeric as a float"),
    ("loan_applications", "total_cost_percent", "7%", "numeric as text with a sign"),
    ("loan_repayments", "payment_ts", "2025-04-02", "timestamp as a date"),
    ("loan_repayments", "is_cured", 0, "boolean as an integer"),
    ("risk_events", "risk_score_delta", [5], "int as a list"),
]


@pytest.mark.parametrize("table,column,value,label", RETYPES, ids=[r[3] for r in RETYPES])
def test_a_retyped_column_stops_pass_1_and_names_the_object_event_and_column(table, column, value, label):
    events = edit(objects(table), 2, lambda e: e["after"].__setitem__(column, value))
    landing, key = land(events, table)
    error = stop(landing)
    assert isinstance(error, MappingError)
    assert str(error).startswith(f"{key}, event 2: {table}.{column}: expected "), str(error)


def test_a_retyped_column_in_the_before_image_stops_pass_1_and_names_the_before_column():
    events = edit(objects("loan_applications", 2, "u"), 1, lambda e: e["before"].__setitem__("months_trading", "38"))
    landing, key = land(events, "loan_applications")
    error = stop(landing)
    assert isinstance(error, MappingError)
    assert str(error).startswith(f"{key}, event 1: loan_applications.before_months_trading: expected ")
    assert "(event field before.months_trading)" in str(error)


@pytest.mark.parametrize("path,value", [
    (("flags",), None), (("flags",), ["surprise"]), (("event_ts",), "2025-03-01 08:00:00"),
    (("source", "commit_ts"), "2025-03-01T08:00:00Z"), (("transaction", "id"), "700"), (("ts_ms",), -1),
    (("source", "change_lsn"), 1.5),
])
def test_a_wrong_envelope_value_stops_pass_1(path, value):
    def change(e):
        target = e
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value

    landing, key = land(edit(objects(), 1, change))
    error = stop(landing)
    assert isinstance(error, MappingError) and str(error).startswith(f"{key}, event 1: merchants.")


def test_an_event_from_another_source_database_stops_pass_1():
    landing, key = land(edit(objects(), 1, lambda e: e["source"].update(db="fintech")))
    error = stop(landing)
    assert isinstance(error, MappingError)
    assert str(error).startswith(f"{key}, event 1: merchants.source.db: expected \"fintech_cdc\"")


def test_the_source_database_is_the_one_the_index_is_built_for():
    landing, _ = land(objects())
    assert build_index(landing.reader(), 2, source_db="fintech_cdc").summary().events_read == 3
    with pytest.raises(MappingError, match="source.db: expected \"other\""):
        build_index(landing.reader(), 2, source_db="other")


def test_an_event_that_breaks_the_south_african_date_rule_stops_pass_1():
    def change(e):
        e["event_ts"] = "2025-03-01T23:30:00.000000Z"       # 01:30 on 2 March in South Africa; the object says 1 March

    landing, key = land(edit(objects(), 1, change))
    error = stop(landing)
    assert isinstance(error, MappingError) and "is not the South African date of event_ts" in str(error)


# ---------------------------------------------------------------------------
# what a person sees, and that nothing was written
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fault", ["extra", "missing", "renamed", "retyped", "version"])
def test_every_kind_of_drift_through_the_command_is_exit_2_with_one_line_naming_the_object(tmp_path, fault):
    def change(e):
        if fault == "extra":
            e["after"]["surprise"] = 1
        elif fault == "missing":
            e["after"].pop("province")
        elif fault == "renamed":
            e["after"] = {("area" if k == "province" else k): v for k, v in e["after"].items()}
        elif fault == "retyped":
            e["after"]["risk_score"] = "14"
        else:
            e["envelope_version"] = 2

    landing, key = land(edit(objects(), 2, change))
    writes = landing.sink.listings, len(landing.sink.list_keys(landing.prefix))
    code, out, err = command(landing, tmp_path)
    assert code == EXIT_CONTRACT
    errors = [line for line in err.splitlines() if line.startswith("error:")]
    assert len(errors) == 1 and key in errors[0] and "event 2" in errors[0]
    assert "OK:" not in out
    assert len(landing.sink.list_keys(landing.prefix)) == writes[1]      # the landing has the same objects


def test_a_clean_landing_through_the_command_is_exit_0_and_says_the_events_map(tmp_path):
    landing, _ = land(objects())
    code, out, err = command(landing, tmp_path)
    assert code == EXIT_OK and "every event maps to a Bronze row" in out.splitlines()[-1]


def test_the_stop_names_the_first_bad_event_not_a_later_one(tmp_path):
    events = objects(count=4)
    events = edit(events, 4, lambda e: e["after"].__setitem__("risk_score", "x"))
    events = edit(events, 2, lambda e: e["after"].__setitem__("risk_score", "y"))
    landing, key = land(events)
    assert str(stop(landing)).startswith(f"{key}, event 2: ")


def test_the_drift_cases_use_columns_the_contract_lists():
    for table, column, _, _ in RETYPES:
        assert column in [name for name, _ in contract.TABLES[table]]


def test_the_command_maps_for_the_source_database_in_the_cdc_settings(tmp_path, monkeypatch):
    events = [deepcopy(e) for e in objects()]
    for e in events:
        e["source"]["db"] = "fintech_cdc_r02"
    landing, _ = land(events)
    code, out, err = command(landing, tmp_path)
    assert code == EXIT_CONTRACT and "source.db: expected \"fintech_cdc\"" in err
    monkeypatch.setenv("CDC_DB", "fintech_cdc_r02")
    code, out, err = command(landing, tmp_path)
    assert code == EXIT_OK and "source database fintech_cdc_r02" in out
