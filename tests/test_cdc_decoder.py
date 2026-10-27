"""tests/test_cdc_decoder.py: M2, the pgoutput decoder and the type map.

Nothing here needs a database. The messages are raw bytes captured from a real Postgres 16
slot (tests/fixtures/pgoutput.json, written by scripts/capture_pgoutput_fixtures.py), and the
expected rows in that file were written by hand, not produced by the decoder. Malformed and
unusual messages are built byte by byte below.
"""
import dataclasses
import json
import struct
from pathlib import Path

import pytest

from cdc import types
from cdc.errors import CdcError, DecodeError, UnknownTypeError, UnsupportedChange
from cdc.lsn import format_lsn, parse_lsn
from cdc.pgoutput import (
    Begin, Commit, Decoder, Delete, Insert, Relation, Truncate, Update, require_supported,
)

FIXTURES = json.loads((Path(__file__).parent / "fixtures" / "pgoutput.json").read_text())["scenarios"]

TABLE_COLUMNS = {"merchants": 11, "merchant_transactions": 32, "loan_applications": 24,
                 "loan_repayments": 22, "risk_events": 13}


def raw(name):
    return [bytes.fromhex(m["hex"]) for m in FIXTURES[name]["messages"]]


def decode(name):
    decoder = Decoder()
    return [decoder.decode(m) for m in raw(name)]


def summarise(message):
    """A decoded change as the dict the fixture's `expected` list uses."""
    if isinstance(message, Insert):
        return {"op": "I", "table": message.relation.name, "new": message.new}
    if isinstance(message, Update):
        return {"op": "U", "table": message.relation.name, "new": message.new,
                "old_kind": message.old_kind, "old": message.old}
    if isinstance(message, Delete):
        return {"op": "D", "table": message.relation.name, "old_kind": message.old_kind, "old": message.old}
    if isinstance(message, Truncate):
        return {"op": "T", "tables": [r.name for r in message.relations]}
    return None


def changes(name):
    return [s for s in map(summarise, decode(name)) if s is not None]


def expected(name):
    scenario = FIXTURES[name]
    return scenario.get("expected") or FIXTURES[scenario["expected_same_as"]]["expected"]


# ---------------------------------------------------------------------------
# Hand-built messages
# ---------------------------------------------------------------------------

NULL, UNCHANGED = b"n", b"u"


def text(value: str) -> bytes:
    data = value.encode()
    return b"t" + struct.pack(">I", len(data)) + data


def tuple_data(*values: bytes) -> bytes:
    return struct.pack(">H", len(values)) + b"".join(values)


def relation_message(oid: int, name: str, columns: list, schema="public", identity=b"d") -> bytes:
    """columns: (name, type oid) pairs; the first is the key."""
    body = struct.pack(">I", oid) + schema.encode() + b"\0" + name.encode() + b"\0" + identity
    body += struct.pack(">H", len(columns))
    for i, (column, type_oid) in enumerate(columns):
        body += bytes([1 if i == 0 else 0]) + column.encode() + b"\0" + struct.pack(">Ii", type_oid, -1)
    return b"R" + body


def insert_message(oid: int, *values: bytes) -> bytes:
    return b"I" + struct.pack(">I", oid) + b"N" + tuple_data(*values)


THING = relation_message(7, "thing", [("id", types.INT4), ("label", types.TEXT)])


def primed() -> Decoder:
    decoder = Decoder()
    decoder.decode(THING)
    return decoder


# ---------------------------------------------------------------------------
# Every message kind, from real bytes
# ---------------------------------------------------------------------------

def test_every_message_kind_is_covered_by_the_fixtures():
    seen = {type(m) for name in FIXTURES for m in _decode_if_clean(name)}
    assert seen == {Begin, Commit, Relation, Insert, Update, Delete, Truncate}


def _decode_if_clean(name):
    if "error" in FIXTURES[name]:
        return []
    return decode(name)


def test_begin_and_commit_frame_each_transaction_and_agree():
    messages = decode("inserts_utc")
    begins = [m for m in messages if isinstance(m, Begin)]
    commits = [m for m in messages if isinstance(m, Commit)]
    assert len(begins) == len(commits) == 2
    assert isinstance(messages[0], Begin) and isinstance(messages[-1], Commit)
    for begin, commit in zip(begins, commits):
        assert begin.final_lsn == commit.commit_lsn, "Begin.final_lsn is the commit LSN"
        assert commit.end_lsn > commit.commit_lsn
        assert begin.commit_ts == commit.commit_ts
        assert begin.commit_ts.tzinfo is not None and begin.commit_ts.year >= 2025
    assert begins[0].xid != begins[1].xid
    assert begins[0].xid == FIXTURES["inserts_utc"]["messages"][0]["xid"]
    assert commits[1].commit_lsn > commits[0].end_lsn, "transactions do not overlap"


def test_a_relation_message_describes_its_table():
    relations = {m.name: m for m in decode("inserts_utc") if isinstance(m, Relation)}
    assert {n: len(r.columns) for n, r in relations.items()} == TABLE_COLUMNS
    merchants = relations["merchants"]
    assert (merchants.schema, merchants.replica_identity) == ("public", "d")
    assert [c.name for c in merchants.columns if c.is_key] == ["merchant_id", "snapshot_at"]
    assert relations["loan_applications"].replica_identity == "f", "migration 003"
    numeric = next(c for c in relations["loan_applications"].columns if c.name == "total_cost_percent")
    assert numeric.type_oid == types.NUMERIC and numeric.type_modifier == ((5 << 16) | 4) + 4


@pytest.mark.parametrize("name", ["inserts_utc", "inserts_johannesburg", "inserts_kolkata", "inserts_st_johns"])
def test_inserts_into_every_table_decode_to_the_expected_rows(name):
    assert changes(name) == expected(name)


def test_rows_keep_the_tables_column_order():
    for message in decode("inserts_utc"):
        if isinstance(message, Insert):
            assert list(message.new) == [c.name for c in message.relation.columns]
            assert len(message.new) == TABLE_COLUMNS[message.relation.name]


@pytest.mark.parametrize("name, offset", [
    ("inserts_utc", b"+00"), ("inserts_johannesburg", b"+02"),
    ("inserts_kolkata", b"+05:30"), ("inserts_st_johns", b"-03:30"),
])
def test_the_fixtures_really_contain_each_offset(name, offset):
    """So the offset parsing in the test above is exercised, not assumed."""
    assert any(offset in m for m in raw(name))
    if offset != b"+00":
        assert not any(b"+00" in m for m in raw(name)), "no UTC timestamps in a non-UTC reading"


def test_a_disbursement_update_carries_the_full_old_row_and_a_replay_is_identical():
    got = changes("disbursement")
    assert got == expected("disbursement")
    first, replay = got[1], got[2]
    assert first["old_kind"] == "O" and first["old"] != first["new"]
    assert replay["old_kind"] == "O" and replay["old"] == replay["new"] == first["new"]
    assert first["old"]["disbursed_at"] is None and first["new"]["disbursed_at"] == "2025-03-03T09:30:00.000000Z"


def test_updates_on_a_default_identity_table_have_no_old_row_or_a_key_only_one():
    got = changes("default_identity_updates")
    assert got == expected("default_identity_updates")
    assert got[1]["old_kind"] is None and got[1]["old"] is None
    assert got[2]["old_kind"] == "K"
    assert got[2]["old"]["merchant_id"] == "m-001" and got[2]["old"]["risk_score"] is None


def test_a_delete_and_a_truncate_decode():
    got = changes("delete_and_truncate")
    assert got == expected("delete_and_truncate")
    assert got[1]["old_kind"] == "K" and got[2] == {"op": "T", "tables": ["risk_events"]}


def test_truncate_options():
    message = Decoder()
    for m in raw("delete_and_truncate")[:3]:
        message.decode(m)
    oid = next(r for r in message._relations.values()).oid
    plain = message.decode(b"T" + struct.pack(">IB", 1, 0) + struct.pack(">I", oid))
    cascade = message.decode(b"T" + struct.pack(">IB", 1, 3) + struct.pack(">I", oid))
    assert (plain.cascade, plain.restart_identity) == (False, False)
    assert (cascade.cascade, cascade.restart_identity) == (True, True)


# ---------------------------------------------------------------------------
# What stops the consumer
# ---------------------------------------------------------------------------

def test_a_relation_with_an_unmapped_type_stops_at_the_relation_message():
    decoder = Decoder()
    messages = raw("unknown_type")
    assert isinstance(decoder.decode(messages[0]), Begin)
    with pytest.raises(UnknownTypeError, match=r"scratch_uuid\.u.*2950"):
        decoder.decode(messages[1])


def test_an_unchanged_toast_marker_stops_the_consumer():
    decoder = Decoder()
    decoder.decode(raw("unchanged_toast")[0])
    decoder.decode(raw("unchanged_toast")[1])
    with pytest.raises(DecodeError, match="unchanged TOAST"):
        decoder.decode(raw("unchanged_toast")[2])


@pytest.mark.parametrize("message, pattern", [
    (b"", "empty"),
    (b"Z" + b"\0" * 20, "unknown message type"),
    (b"O" + b"\0" * 20, "unknown message type"),   # origin: not requested, refused
    (b"M" + b"\0" * 20, "unknown message type"),   # logical message: not requested, refused
    (insert_message(99, text("1"), text("x")), "no Relation message"),
])
def test_unknown_or_unplanned_messages_are_refused(message, pattern):
    with pytest.raises(DecodeError, match=pattern):
        primed().decode(message)


@pytest.mark.parametrize("values, pattern", [
    ((text("1"), UNCHANGED), "unchanged TOAST"),
    ((text("1"), b"b" + struct.pack(">I", 1) + b"x"), "binary value"),
    ((text("1"), b"?"), "unknown tuple column kind"),
    ((text("1"),), "1 columns"),
    ((text("1"), text("a"), text("b")), "3 columns"),
    ((text("abc"), text("x")), "thing.id"),
])
def test_bad_tuples_stop_the_consumer(values, pattern):
    with pytest.raises(DecodeError, match=pattern):
        primed().decode(insert_message(7, *values))


def test_a_value_that_runs_past_the_end_of_the_message_is_refused():
    message = insert_message(7, text("1"), b"t" + struct.pack(">I", 50) + b"short")
    with pytest.raises(DecodeError, match="past the end"):
        primed().decode(message)


def test_trailing_bytes_are_refused():
    with pytest.raises(DecodeError, match="trailing"):
        primed().decode(insert_message(7, text("1"), text("x")) + b"\0")


@pytest.mark.parametrize("name", [n for n in FIXTURES if "error" not in FIXTURES[n]])
def test_a_truncated_or_extended_real_message_is_a_decode_error_never_a_crash(name):
    decoder = Decoder()
    for message in raw(name):
        decoder.decode(message)
    for message in raw(name):
        for broken in (message[:-1], message[: len(message) // 2], message + b"\0"):
            with pytest.raises(DecodeError):
                decoder.decode(broken)


def test_every_error_the_consumer_stops_on_is_a_cdc_error():
    assert issubclass(UnknownTypeError, DecodeError) and issubclass(DecodeError, CdcError)
    assert issubclass(UnsupportedChange, CdcError) and not issubclass(UnsupportedChange, DecodeError)


# ---------------------------------------------------------------------------
# Relation messages are state, and can come at any time
# ---------------------------------------------------------------------------

def test_a_repeated_relation_message_replaces_what_was_known():
    decoder = primed()
    assert decoder.decode(insert_message(7, text("1"), text("x"))).new == {"id": 1, "label": "x"}
    wider = relation_message(7, "thing", [("id", types.INT4), ("label", types.TEXT), ("flag", types.BOOL)])
    decoder.decode(wider)
    assert decoder.decode(insert_message(7, text("2"), text("y"), text("t"))).new == {"id": 2, "label": "y", "flag": True}
    with pytest.raises(DecodeError, match="2 columns"):
        decoder.decode(insert_message(7, text("3"), text("z")))


def test_a_relation_message_in_the_middle_of_a_transaction_is_accepted():
    """Relation messages repeat inside one decode call (seen on a real slot); each is accepted
    and the rows after it still decode."""
    sequence = []
    for message in raw("inserts_utc"):
        sequence.append(message)
        if message[:1] == b"R":
            sequence.append(message)  # the same Relation message again, straight after
    decoder = Decoder()
    inserts = [m for m in map(decoder.decode, sequence) if isinstance(m, Insert)]
    assert [i.relation.name for i in inserts] == [e["table"] for e in expected("inserts_utc")]


def test_a_failed_relation_message_leaves_the_earlier_state_alone():
    decoder = primed()
    bad = relation_message(7, "thing", [("id", types.INT4), ("u", 2950)])
    with pytest.raises(UnknownTypeError):
        decoder.decode(bad)
    assert decoder.decode(insert_message(7, text("1"), text("x"))).new == {"id": 1, "label": "x"}


# ---------------------------------------------------------------------------
# require_supported (contract section 8)
# ---------------------------------------------------------------------------

def test_inserts_and_full_row_updates_on_loan_applications_are_supported():
    for name in ("inserts_utc", "disbursement"):
        for message in decode(name):
            require_supported(message)


def test_a_delete_and_a_truncate_are_unsupported():
    messages = decode("delete_and_truncate")
    with pytest.raises(UnsupportedChange, match="delete on risk_events"):
        require_supported(next(m for m in messages if isinstance(m, Delete)))
    with pytest.raises(UnsupportedChange, match="truncate of risk_events"):
        require_supported(next(m for m in messages if isinstance(m, Truncate)))


def test_an_update_on_any_other_table_is_unsupported():
    updates = [m for m in decode("default_identity_updates") if isinstance(m, Update)]
    assert len(updates) == 2
    for update in updates:
        with pytest.raises(UnsupportedChange, match="only loan_applications"):
            require_supported(update)


@pytest.mark.parametrize("old_kind, old", [(None, None), ("K", {"application_id": "app-1"})])
def test_an_update_without_the_full_old_row_is_unsupported(old_kind, old):
    update = next(m for m in decode("disbursement") if isinstance(m, Update))
    weakened = dataclasses.replace(update, old_kind=old_kind, old=old)
    with pytest.raises(UnsupportedChange, match="REPLICA IDENTITY FULL"):
        require_supported(weakened)


# ---------------------------------------------------------------------------
# The type map (contract section 4)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value, utc", [
    ("2025-03-01 22:30:12+00", "2025-03-01T22:30:12.000000Z"),
    ("2025-03-01 22:30:12+02", "2025-03-01T20:30:12.000000Z"),
    ("2025-03-01 22:30:12-05", "2025-03-02T03:30:12.000000Z"),
    ("2025-03-02 00:30:12+02", "2025-03-01T22:30:12.000000Z"),   # the contract's SAST example
    ("2025-01-01 00:00:00+02", "2024-12-31T22:00:00.000000Z"),   # across a year boundary
    ("2024-03-01 00:30:00+02", "2024-02-29T22:30:00.000000Z"),   # across a leap day
    ("2025-03-01 22:30:12+05:30", "2025-03-01T17:00:12.000000Z"),
    ("2025-03-01 22:30:12-03:30", "2025-03-02T02:00:12.000000Z"),
    ("2025-03-01 22:30:12+01:00:30", "2025-03-01T21:29:42.000000Z"),
    ("2025-03-01 22:30:12.1+00", "2025-03-01T22:30:12.100000Z"),
    ("2025-03-01 22:30:12.12+00", "2025-03-01T22:30:12.120000Z"),
    ("2025-03-01 22:30:12.123456+00", "2025-03-01T22:30:12.123456Z"),
    ("2025-03-01 22:30:12.000001+00", "2025-03-01T22:30:12.000001Z"),
    ("0999-03-01 22:30:12+00", "0999-03-01T22:30:12.000000Z"),
])
def test_timestamps_become_utc_with_six_fractional_digits(value, utc):
    assert types.to_timestamptz(value) == utc


@pytest.mark.parametrize("value", [
    "infinity", "-infinity", "2025-03-01 22:30:12", "2025-03-01T22:30:12Z", "2025-03-01 22:30:12 BC",
    "2025-03-01 22:30:12.1234567+00", "2025-13-01 22:30:12+00", "2025-02-30 22:30:12+00",
    "12025-03-01 22:30:12+00", "2025-03-01 24:00:00+00", "", "not a time",
])
def test_unparseable_timestamps_are_refused(value):
    with pytest.raises(ValueError):
        types.to_timestamptz(value)


@pytest.mark.parametrize("value, parsed", [
    ("{}", []),
    ("{a}", ["a"]),
    ("{a,b}", ["a", "b"]),
    ("{NULL}", [None]),
    ('{"NULL"}', ["NULL"]),
    ('{"",a}', ["", "a"]),
    ('{"a b"," c"}', ["a b", " c"]),
    ('{"with, comma"}', ["with, comma"]),
    ('{"say \\"hi\\""}', ['say "hi"']),
    ('{"back\\\\slash"}', ["back\\slash"]),
    ('{"{braces}"}', ["{braces}"]),
    ("{caf\u00e9,\u4e2d}", ["caf\u00e9", "\u4e2d"]),
    ('{"with, comma","with \\"quote\\"",NULL,"NULL","back\\\\slash"," lead space",""}',
     ["with, comma", 'with "quote"', None, "NULL", "back\\slash", " lead space", ""]),
])
def test_text_arrays(value, parsed):
    assert types.to_text_array(value) == parsed


@pytest.mark.parametrize("value", [
    "", "a", "{a", "a}", "{a,}", "{,a}", "{a,,b}", "{{a},{b}}", '{"a}', '{"a\\', '{"a"b}', "[1:2]={a,b}",
    "{a b}x", '{a"b}',
])
def test_malformed_or_unsupported_arrays_are_refused(value):
    with pytest.raises(ValueError):
        types.to_text_array(value)


@pytest.mark.parametrize("value", ["0", "-1", "12550", "9007199254740991", "-9007199254740991"])
def test_integers_inside_the_safe_range(value):
    assert types.to_int(value) == int(value)


@pytest.mark.parametrize("value", ["9007199254740992", "-9007199254740992", "9223372036854775807",
                                   "1_000", " 5", "+5", "5.0", "", "-", "abc"])
def test_integers_outside_the_safe_range_or_not_plain_are_refused(value):
    with pytest.raises(ValueError):
        types.to_int(value)


def test_booleans_dates_numerics_and_text():
    assert (types.to_bool("t"), types.to_bool("f")) == (True, False)
    for bad in ("true", "T", "1", ""):
        with pytest.raises(ValueError):
            types.to_bool(bad)
    assert types.to_date("2025-03-03") == "2025-03-03"
    for bad in ("infinity", "2025-3-3", "2025-W01-1", "2025-02-30", "0001-01-01 BC"):
        with pytest.raises(ValueError):
            types.to_date(bad)
    assert [types.to_numeric(v) for v in ("0.0700", "1.0000", "0.0000", "9.9999")] == ["0.0700", "1.0000", "0.0000", "9.9999"]
    assert types.to_text("") == "" and types.to_text("caf\u00e9 \u4e2d") == "caf\u00e9 \u4e2d"


def test_the_map_holds_exactly_the_eight_types_of_the_five_tables():
    assert set(types.CONVERTERS) == {16, 20, 23, 25, 1082, 1184, 1700, 1009}
    for oid in (21, 700, 701, 1114, 2950, 3802, 1043):   # int2, float4, float8, timestamp, uuid, jsonb, varchar
        with pytest.raises(UnknownTypeError):
            types.converter_for(oid)


def test_the_five_tables_use_only_mapped_types():
    for name in ("inserts_utc",):
        for message in decode(name):
            if isinstance(message, Relation):
                assert {c.type_oid for c in message.columns} <= set(types.CONVERTERS)


# ---------------------------------------------------------------------------
# LSNs
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text_form, number", [
    ("0/0", 0), ("0/1953870", 26556528), ("0/FFFFFFFF", 4294967295), ("1/0", 4294967296),
    ("16/B374D848", (0x16 << 32) + 0xB374D848),
])
def test_lsn_round_trip(text_form, number):
    assert parse_lsn(text_form) == number
    assert format_lsn(number) == text_form


@pytest.mark.parametrize("bad", ["", "1953870", "0/", "/1", "0/G", "0/1/2", "123456789/0", "-1/0", "+1/0", "1_0/0", " 0/0"])
def test_a_malformed_lsn_is_refused(bad):
    with pytest.raises(DecodeError):
        parse_lsn(bad)


def test_lsns_sort_numerically_not_as_text():
    assert parse_lsn("0/9") < parse_lsn("0/10") < parse_lsn("1/0")
    assert "0/9" > "0/10"  # as text they would not
