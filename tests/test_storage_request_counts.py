"""tests/test_storage_request_counts.py: scripts/storage_request_counts.py, P2, against a fake Monitoring API.

The first real call to the Monitoring API is on the Air; everything else (the request, the pages, the sums, the classes,
the self-check against the first P2, the delta, the text and the exit codes) is tested here."""
import io
import json
import sys
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import storage_request_counts as src  # noqa: E402

L, B, T = "cdc_landing", "fintech_bronze", "payments-512910-tfstate"


def series(bucket, method, code, *values):
    labels = {"method": method} if code is None else {"method": method, "response_code": code}
    return {"resource": {"type": "gcs_bucket", "labels": {"bucket_name": bucket}}, "metric": {"type": src.METRIC, "labels": labels},
            "points": [{"interval": {}, "value": {"int64Value": str(v)}} for v in values]}


def query(url):
    return urllib.parse.parse_qs(urllib.parse.urlparse(url).query, keep_blank_values=True)


class FakeApi:
    """get(url, token): answers from `windows`, a dict of interval.endTime -> a list of pages (each a list of series)."""

    def __init__(self, windows):
        self.windows, self.calls, self.tokens = windows, [], []

    def __call__(self, url, token):
        q = query(url)
        self.calls.append(url)
        self.tokens.append(token)
        pages = self.windows[q["interval.endTime"][0]]
        number = int(q["pageToken"][0]) if "pageToken" in q else 0
        body = {"timeSeries": pages[number]}
        if number + 1 < len(pages):
            body["nextPageToken"] = str(number + 1)
        return body


# the first P2's raw counts (DECISION_LOG, "P2 done"): 6,709 requests
FIRST = [
    series(L, "WriteObject", "OK", 1_000, 341), series(L, "WriteObject", "FAILED_PRECONDITION", 166),
    series(L, "ReadObject", "OK", 3_822), series(L, "ListObjects", "OK", 511), series(L, "ListObjects", "NOT_FOUND", 1),
    series(L, "DeleteObject", "OK", 433), series(L, "GetObjectMetadata", "OK", 1), series(L, "GetObjectMetadata", "NOT_FOUND", 23),
    series(L, "GetBucketMetadata", "OK", 19), series(L, "GetBucketMetadata", "PERMISSION_DENIED", 10),
    series(L, "GetIamPolicy", "OK", 28), series(L, "GetBucketStorageLayout", "OK", 4),
    series(L, "TestIamPermissions", "OK", 4), series(L, "TestIamPermissions", "INVALID_ARGUMENT", 1),
    series(L, "SetIamPolicy", "OK", 3),
    series(B, "WriteObject", "OK", 35), series(B, "ReadObject", "OK", 47), series(B, "ReadObject", "CANCELLED", 1),
    series(B, "GetObjectMetadata", "OK", 80), series(B, "ListObjects", "OK", 18), series(B, "DeleteObject", "OK", 35),
    series(B, "GetBucketMetadata", "OK", 20), series(B, "GetIamPolicy", "OK", 18),
    series(T, "WriteObject", "OK", 15), series(T, "ListObjects", "OK", 4), series(T, "UpdateBucketMetadata", "OK", 1),
    series(T, "ReadObject", "OK", 29), series(T, "GetObjectMetadata", "OK", 27), series(T, "DeleteObject", "OK", 11),
    series(T, "GetBucketMetadata", "OK", 1),
]


def test_the_fixture_is_the_first_p2s_raw_counts_and_totals():
    counts = src.counts_between("p", "s", "e", "t", FakeApi({"e": [FIRST]}))
    assert src.by_bucket(counts) == {B: 254, L: 6_367, T: 88} == dict(sorted(src.FIRST_P2_TOTALS.items()))
    assert sum(counts.values()) == 6_709


# ---------------------------------------------------------------------------
# the request and the pages
# ---------------------------------------------------------------------------

def test_the_request_asks_for_the_request_count_of_buckets_summed_per_bucket_method_and_code():
    url = src.request_url("my-project", "2026-10-01T07:00:00Z", "2026-10-29T11:03:00Z")
    assert url.startswith("https://monitoring.googleapis.com/v3/projects/my-project/timeSeries?")
    q = query(url)
    assert q["filter"] == ['metric.type = "storage.googleapis.com/api/request_count" AND resource.type = "gcs_bucket"']
    assert src.METRIC == "storage.googleapis.com/api/request_count"
    assert (q["interval.startTime"], q["interval.endTime"]) == (["2026-10-01T07:00:00Z"], ["2026-10-29T11:03:00Z"])
    assert q["aggregation.alignmentPeriod"] == ["86400s"] and q["aggregation.perSeriesAligner"] == ["ALIGN_SUM"]
    assert q["aggregation.crossSeriesReducer"] == ["REDUCE_SUM"]
    assert q["aggregation.groupByFields"] == ["resource.label.bucket_name", "metric.label.method", "metric.label.response_code"]
    assert q["view"] == ["FULL"] and "pageToken" not in q


def test_a_page_token_is_added_only_when_given():
    assert query(src.request_url("p", "a", "b", "TOKEN"))["pageToken"] == ["TOKEN"]
    assert "pageToken" not in query(src.request_url("p", "a", "b", None))


def test_points_and_series_with_the_same_key_are_added_and_pages_are_followed():
    pages = [[series(L, "ReadObject", "OK", 1, 2, 3)], [series(L, "ReadObject", "OK", 10), series(B, "ListObjects", "OK", 4)],
             [series(L, "ReadObject", "NOT_FOUND", 100)]]
    api = FakeApi({"e": pages})
    counts = src.counts_between("p", "s", "e", "TOKEN", api)
    assert counts == {(L, "ReadObject", "OK"): 16, (B, "ListObjects", "OK"): 4, (L, "ReadObject", "NOT_FOUND"): 100}
    assert len(api.calls) == 3 and api.tokens == ["TOKEN"] * 3
    assert "pageToken" not in query(api.calls[0]) and query(api.calls[1])["pageToken"] == ["1"] and query(api.calls[2])["pageToken"] == ["2"]


def test_a_series_without_a_response_code_or_points_and_an_empty_answer():
    assert src.counts_between("p", "s", "e", "t", FakeApi({"e": [[series(L, "ReadObject", None, 5)]]})) == {(L, "ReadObject", ""): 5}
    empty = series(L, "ReadObject", "OK")
    assert src.counts_between("p", "s", "e", "t", FakeApi({"e": [[empty]]})) == {}
    assert src.counts_between("p", "s", "e", "t", lambda url, token: {}) == {}


# ---------------------------------------------------------------------------
# the real getters, with the network and gcloud replaced
# ---------------------------------------------------------------------------

class Answer:
    def __init__(self, body):
        self.body = body

    def read(self):
        return self.body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_http_get_sends_the_token_and_reads_json(monkeypatch):
    seen = {}

    def urlopen(request, timeout):
        seen["header"], seen["url"], seen["timeout"] = request.get_header("Authorization"), request.full_url, timeout
        return Answer(b'{"timeSeries": []}')

    monkeypatch.setattr(src.urllib.request, "urlopen", urlopen)
    assert src.http_get("https://example/x", "SECRET") == {"timeSeries": []}
    assert seen == {"header": "Bearer SECRET", "url": "https://example/x", "timeout": 60}


def test_an_api_error_carries_the_status_and_googles_message(monkeypatch):
    def urlopen(request, timeout):
        raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", {}, io.BytesIO(b'{"error": {"message": "no permission"}}'))

    monkeypatch.setattr(src.urllib.request, "urlopen", urlopen)
    with pytest.raises(RuntimeError) as stop:
        src.http_get("https://example/x", "t")
    assert str(stop.value).startswith("the Monitoring API answered 403: ") and "no permission" in str(stop.value)


def test_a_network_error_is_a_runtime_error(monkeypatch):
    def urlopen(request, timeout):
        raise urllib.error.URLError("name not known")

    monkeypatch.setattr(src.urllib.request, "urlopen", urlopen)
    with pytest.raises(RuntimeError, match="could not reach the Monitoring API: name not known"):
        src.http_get("https://example/x", "t")


class Ran:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


def test_the_token_comes_from_gcloud_for_the_project(monkeypatch):
    seen = {}

    def run(command, **kwargs):
        seen["command"], seen["kwargs"] = command, kwargs
        return Ran(0, "  ya29.TOKEN\n")

    monkeypatch.setattr(src.subprocess, "run", run)
    assert src.gcloud_token() == "ya29.TOKEN"
    assert seen["command"] == ["gcloud", "auth", "print-access-token", "--project=payments-512910"]
    assert seen["kwargs"]["capture_output"] is True and seen["kwargs"]["text"] is True and seen["kwargs"]["timeout"] == 60


@pytest.mark.parametrize("ran", [Ran(1, "", "not logged in"), Ran(0, "  \n", ""), Ran(1, "ya29.LOOKS-LIKE-A-TOKEN", "failed")])
def test_no_token_is_a_runtime_error_and_never_prints_one(monkeypatch, ran):
    monkeypatch.setattr(src.subprocess, "run", lambda command, **kwargs: ran)
    with pytest.raises(RuntimeError, match="gcloud could not give a token"):
        src.gcloud_token()


def test_a_missing_gcloud_is_a_runtime_error(monkeypatch):
    def run(command, **kwargs):
        raise FileNotFoundError("gcloud")

    monkeypatch.setattr(src.subprocess, "run", run)
    with pytest.raises(RuntimeError, match="could not run gcloud for a token"):
        src.gcloud_token()


# ---------------------------------------------------------------------------
# what the counts mean
# ---------------------------------------------------------------------------

def test_totals_by_bucket_are_sorted():
    assert src.by_bucket({(T, "a", ""): 1, (B, "a", ""): 2, (B, "b", "x"): 3, (L, "a", ""): 4}) == {B: 5, L: 4, T: 1}
    assert list(src.by_bucket({(T, "a", ""): 1, (B, "a", ""): 2})) == [B, T]


def test_the_first_p2s_class_a_for_the_landing_bucket_is_reproduced_with_and_without_the_refused_creates():
    counts = src.counts_between("p", "s", "e", "t", FakeApi({"e": [FIRST]}))
    c = src.classify(counts)[L]
    assert c["a_high"] == 1_341 + 166 + 512 + 3 == 2_022 and c["a_low"] == 1_856
    assert c["b"] == 3_822 + 24 + 29 + 28 + 4 + 5 and c["free"] == 433 and c["unmapped"] == {}


def test_each_method_is_in_exactly_one_class_and_only_a_refused_create_is_split_out():
    counts = {(L, m, "OK"): 1 for m in src.CLASS_A + src.CLASS_B + src.FREE}
    c = src.classify(counts)["all"]
    assert (c["a_low"], c["a_high"], c["b"], c["free"], c["unmapped"]) == (4, 4, 6, 1, {})
    assert set(src.CLASS_A).isdisjoint(src.CLASS_B) and set(src.CLASS_A + src.CLASS_B).isdisjoint(src.FREE)
    refused = {(L, "WriteObject", "FAILED_PRECONDITION"): 7, (L, "WriteObject", "OK"): 3, (L, "ListObjects", "FAILED_PRECONDITION"): 2,
               (L, "ReadObject", "FAILED_PRECONDITION"): 5}
    c = src.classify(refused)["all"]
    assert (c["a_low"], c["a_high"], c["b"]) == (5, 12, 5)         # only WriteObject's refusal is a "refused create"


def test_a_method_that_is_not_mapped_is_listed_apart_and_counted_nowhere_else():
    counts = {(L, "ComposeObject", "OK"): 4, (B, "ComposeObject", "OK"): 1, (L, "ReadObject", "OK"): 9, (L, "RewriteObject", "OK"): 2}
    classes = src.classify(counts)
    assert classes["all"]["unmapped"] == {"ComposeObject": 5, "RewriteObject": 2}
    assert classes[L]["unmapped"] == {"ComposeObject": 4, "RewriteObject": 2} and classes[B]["unmapped"] == {"ComposeObject": 1}
    assert (classes["all"]["a_high"], classes["all"]["b"]) == (0, 9)


def test_classes_are_given_for_all_buckets_and_for_each_separately():
    counts = {(L, "ReadObject", "OK"): 5, (B, "ReadObject", "OK"): 7, (B, "WriteObject", "OK"): 2}
    classes = src.classify(counts)
    assert list(classes) == ["all", L, B]
    assert classes["all"]["b"] == 12 and classes[B] == {"a_low": 2, "a_high": 2, "b": 7, "free": 0, "unmapped": {}}
    assert classes[L]["b"] == 5 and classes[L]["a_low"] == 0


def test_the_difference_keeps_what_moved_in_either_direction_and_drops_what_did_not():
    before = {(L, "ReadObject", "OK"): 10, (L, "ListObjects", "OK"): 5, (B, "WriteObject", "OK"): 3}
    now = {(L, "ReadObject", "OK"): 1_826, (L, "ListObjects", "OK"): 5, (B, "ReadObject", "OK"): 2}
    assert src.difference(now, before) == {(B, "ReadObject", "OK"): 2, (B, "WriteObject", "OK"): -3, (L, "ReadObject", "OK"): 1_816}
    assert list(src.difference(now, before)) == sorted(src.difference(now, before))
    assert src.difference(now, now) == {}


def test_the_self_check_passes_on_the_first_p2s_totals_and_names_each_bucket_that_differs():
    counts = src.counts_between("p", "s", "e", "t", FakeApi({"e": [FIRST]}))
    ok, text = src.reproduces_first_p2(counts)
    assert ok and text.startswith("the first window reproduces the first P2 (6,709 requests")
    exact = {(L, "ReadObject", "OK"): 6_367, (B, "ReadObject", "OK"): 254, (T, "ReadObject", "OK"): 88}
    ok, text = src.reproduces_first_p2(exact)
    assert ok and text == ("the first window reproduces the first P2 (6,709 requests: cdc_landing 6,367, fintech_bronze 254, "
                           "payments-512910-tfstate 88)")
    ok, text = src.reproduces_first_p2({**exact, (L, "ReadObject", "OK"): 6_368})
    assert not ok and "does NOT reproduce" in text and "cdc_landing: found 6,368, first P2 6,367" in text and "fintech_bronze" not in text
    ok, text = src.reproduces_first_p2({(L, "ReadObject", "OK"): 6_367, (B, "ReadObject", "OK"): 254})
    assert not ok and "payments-512910-tfstate: found 0, first P2 88" in text
    ok, text = src.reproduces_first_p2({**exact, ("another_bucket", "ReadObject", "OK"): 1})
    assert not ok and "another_bucket: found 1, first P2 0" in text


# ---------------------------------------------------------------------------
# the text
# ---------------------------------------------------------------------------

def test_the_tables_are_sorted_by_bucket_method_and_response_code_whatever_order_they_arrive_in():
    counts = {(T, "ReadObject", "OK"): 1, (B, "WriteObject", "OK"): 2, (B, "ListObjects", "OK"): 3, (B, "ListObjects", "A_CODE"): 4}
    rows = [line.split() for line in src.table(counts, "title")[2:-1]]
    assert [(r[0], r[1], r[2]) for r in rows] == [(B, "ListObjects", "A_CODE"), (B, "ListObjects", "OK"), (B, "WriteObject", "OK"),
                                                   (T, "ReadObject", "OK")]
    assert src.table(counts, "title")[0] == "title" and src.table(counts, "title")[-1] == "  total 10 requests"


def test_the_report_names_the_windows_the_check_the_tables_the_delta_the_classes_and_the_headroom():
    first = {(L, "WriteObject", "OK"): 1_000, (L, "WriteObject", "FAILED_PRECONDITION"): 100, (L, "ReadObject", "OK"): 3_000}
    now = {**first, (L, "ReadObject", "OK"): 4_816, (L, "ListObjects", "OK"): 2, (L, "ComposeObject", "OK"): 3}
    text = src.render(first, now, "FIRSTEND", "NOWEND", "START", "THE CHECK LINE")
    lines = text.splitlines()
    assert lines[:4] == ["window 1: START to FIRSTEND", "window 2: START to NOWEND", "", "THE CHECK LINE"]
    assert "window 1 (FIRSTEND)" in text and "window 2 (NOWEND)" in text and "since window 1 (window 2 less window 1)" in text
    assert "  cdc_landing                ReadObject               OK                       1,816" in text
    assert "  cdc_landing                ListObjects              OK                           2" in text
    assert "  total 4,100 requests" in text and "  total 5,921 requests" in text and "  total 1,821 requests" in text
    assert "NOT MAPPED: ComposeObject 3 (not counted in A or B)" in text
    assert "Class A   1,002 to 1,102   Class B   4,816" in text
    assert "Class A 20.0% to 22.0% of 5,000; Class B 9.6% of 50,000" in text
    assert "headroom: 3,898 Class A left at the high count" in text
    assert "Google's billed estimate (successful reads, writes and listings only): Class A 1,002   Class B 4,816" in text
    assert "NOT ENOUGH" not in text


def test_a_window_with_no_change_says_so_and_a_single_class_a_figure_has_no_range():
    same = {(L, "WriteObject", "OK"): 10}
    text = src.render(same, same, "A", "B", "S", "c")
    assert "since window 1: no request" in text
    classes = text.split("classes (my mapping")[1].split("against the second-hand")[0]
    assert "  all                        Class A               10   Class B       0" in classes and " to " not in classes


def test_the_headroom_line_shows_room_and_planned_a_when_given():
    def headroom(a, planned=0):
        return src.render({}, {(L, "WriteObject", "OK"): a}, "A", "B", "S", "c", planned_a=planned).splitlines()[-1]
    assert headroom(5_000 - 135) == "headroom: 135 Class A left at the high count"
    assert headroom(5_000 - 135, planned=100) == "headroom: 135 Class A left at the high count; with 100 planned: 35 remaining"
    assert headroom(5_000 - 135, planned=135) == "headroom: 135 Class A left at the high count; with 135 planned: 0 remaining"
    assert "(NOT ENOUGH)" in headroom(5_000 - 135, planned=136)
    assert "NOT ENOUGH" not in headroom(5_000 - 135)    # no planned: no NOT ENOUGH however low the room


# ---------------------------------------------------------------------------
# the command
# ---------------------------------------------------------------------------

END1, END2 = "2026-10-29T11:03:00Z", "2026-10-30T19:30:00Z"
GOOD_FIRST = [series(L, "ReadObject", "OK", 6_367), series(B, "ReadObject", "OK", 254), series(T, "ReadObject", "OK", 88)]


def run_main(windows, argv=(), token=lambda: "TOKEN", clock=lambda: datetime(2026, 10, 30, 19, 30, 45, tzinfo=timezone.utc)):
    out, err, api = io.StringIO(), io.StringIO(), FakeApi(windows)
    code = src.main(list(argv), token_source=token, get=api, clock=clock, out=out, err=err)
    return code, out.getvalue(), err.getvalue(), api


def test_the_command_exits_zero_when_the_first_window_reproduces_the_first_p2():
    code, out, err, api = run_main({END1: [GOOD_FIRST], END2: [GOOD_FIRST + [series(L, "ReadObject", "OK", 1_816)]]})
    assert code == 0 and err == "" and "the first window reproduces the first P2 (6,709 requests" in out
    assert "since window 1" in out and "1,816" in out
    ends = [query(u)["interval.endTime"][0] for u in api.calls]
    expected_start = src.default_month_start(datetime(2026, 10, 30, 19, 30, 45, tzinfo=timezone.utc))
    assert ends == [END1, END2] and {query(u)["interval.startTime"][0] for u in api.calls} == {expected_start}
    assert api.tokens == ["TOKEN", "TOKEN"] and f"/projects/{src.PROJECT}/" in api.calls[0]
    assert f"window 2: {expected_start} to {END2}" in out                   # now, to the minute


def test_the_command_still_prints_everything_but_exits_two_when_the_check_fails():
    wrong = [series(L, "ReadObject", "OK", 6_000), series(B, "ReadObject", "OK", 254), series(T, "ReadObject", "OK", 88)]
    code, out, err, _ = run_main({END1: [wrong], END2: [wrong]})
    assert code == 2 and "does NOT reproduce" in out and "window 2" in out and err == ""


def test_the_options_reach_the_requests():
    code, out, err, api = run_main({"B": [GOOD_FIRST], "C": [GOOD_FIRST]},
                                   ["--project", "other", "--start", "A", "--first-end", "B", "--end", "C"])
    assert code == 0
    assert [(query(u)["interval.startTime"][0], query(u)["interval.endTime"][0]) for u in api.calls] == [("A", "B"), ("A", "C")]
    assert all("/projects/other/" in u for u in api.calls)


def test_the_default_end_is_now_to_the_minute_in_utc():
    # Use an October clock so FIRST_P2_END (Oct 29) is not skipped; the test checks --end defaulting to now.
    code, out, err, api = run_main({END1: [GOOD_FIRST], "2026-10-31T23:59:00Z": [GOOD_FIRST]},
                                   clock=lambda: datetime(2026, 10, 31, 23, 59, 59, tzinfo=timezone.utc))
    assert code == 0 and query(api.calls[1])["interval.endTime"] == ["2026-10-31T23:59:00Z"]


def test_a_token_failure_exits_one_with_one_error_line_and_nothing_on_stdout():
    def no_token():
        raise RuntimeError("gcloud could not give a token (exit 1): not logged in")

    code, out, err, api = run_main({}, token=no_token)
    assert code == 1 and out == "" and err == "error: gcloud could not give a token (exit 1): not logged in\n" and api.calls == []


def test_an_api_failure_exits_one_and_prints_nothing_of_the_figures():
    def broken(url, token):
        raise RuntimeError("the Monitoring API answered 403: denied")

    out, err = io.StringIO(), io.StringIO()
    assert src.main([], token_source=lambda: "t", get=broken, out=out, err=err) == 1
    assert out.getvalue() == "" and err.getvalue() == "error: the Monitoring API answered 403: denied\n"


def test_the_token_is_never_printed():
    code, out, err, _ = run_main({END1: [GOOD_FIRST], END2: [GOOD_FIRST]}, token=lambda: "SUPER-SECRET-TOKEN")
    assert "SUPER-SECRET" not in out + err


# ---------------------------------------------------------------------------
# default_month_start (step 6)
# ---------------------------------------------------------------------------

def test_default_month_start_returns_the_first_of_the_month_at_07_utc():
    assert src.default_month_start(datetime(2026, 10, 30, 19, 0, 0, tzinfo=timezone.utc)) == "2026-10-01T07:00:00Z"
    assert src.default_month_start(datetime(2026, 11, 15, 12, 0, 0, tzinfo=timezone.utc)) == "2026-11-01T07:00:00Z"
    assert src.default_month_start(datetime(2026, 1, 1, 7, 0, 0, tzinfo=timezone.utc)) == "2026-01-01T07:00:00Z"


def test_default_month_start_uses_utc_not_local_time(monkeypatch):
    # A datetime that is Oct 31 in UTC+2 but still Oct 30 in UTC should give November (UTC month)
    import datetime as dt_mod
    late_oct = datetime(2026, 10, 31, 0, 30, 0, tzinfo=timezone.utc)      # Oct 31 UTC
    assert src.default_month_start(late_oct).startswith("2026-10-01")


# mutation 1: change day=1 to day=2
def test_default_month_start_day_must_be_1():
    result = src.default_month_start(datetime(2026, 10, 15, 9, 0, 0, tzinfo=timezone.utc))
    assert result[8:10] == "01"


# mutation 2: change hour=7 to hour=8
def test_default_month_start_hour_must_be_07():
    result = src.default_month_start(datetime(2026, 10, 15, 9, 0, 0, tzinfo=timezone.utc))
    assert result[11:13] == "07"


# ---------------------------------------------------------------------------
# google_estimate (step 6)
# ---------------------------------------------------------------------------

def test_google_estimate_counts_only_ok_for_the_billed_methods():
    counts = {
        (L, "WriteObject", "OK"): 100,
        (L, "WriteObject", "FAILED_PRECONDITION"): 20,      # not billed
        (L, "ListObjects", "OK"): 5,
        (L, "ListObjects", "NOT_FOUND"): 3,                 # not billed
        (L, "ReadObject", "OK"): 900,
        (L, "ReadObject", "CANCELLED"): 10,                 # not billed
        (L, "GetObjectMetadata", "OK"): 50,
        (L, "GetObjectMetadata", "NOT_FOUND"): 8,           # not billed
        (L, "GetBucketMetadata", "OK"): 4,
        (L, "GetBucketMetadata", "PERMISSION_DENIED"): 7,   # not billed
        (L, "SetIamPolicy", "OK"): 3,                       # not in Google's rule
        (L, "GetIamPolicy", "OK"): 6,                       # not in Google's rule
        (L, "DeleteObject", "OK"): 15,                      # not billed
    }
    ga, gb = src.google_estimate(counts)
    assert ga == 100 + 5                      # WriteObject OK + ListObjects OK only
    assert gb == 900 + 50 + 4                 # ReadObject OK + GetObjectMetadata OK + GetBucketMetadata OK only


# mutation 3: google_estimate must exclude non-OK responses
def test_google_estimate_excluding_permission_denied_is_not_optional():
    counts = {(L, "GetBucketMetadata", "PERMISSION_DENIED"): 7, (L, "GetBucketMetadata", "OK"): 3}
    _, gb = src.google_estimate(counts)
    assert gb == 3                            # only OK


# mutation 4: google_estimate methods are specific
def test_google_estimate_does_not_count_set_iam_or_get_iam():
    counts = {(L, "SetIamPolicy", "OK"): 5, (L, "GetIamPolicy", "OK"): 3}
    ga, gb = src.google_estimate(counts)
    assert ga == 0 and gb == 0


def test_google_estimate_on_the_first_p2_fixture():
    counts = src.counts_between("p", "s", "e", "t", FakeApi({"e": [FIRST]}))
    ga, gb = src.google_estimate(counts)
    # WriteObject OK: L=1341, B=35, T=15 = 1391; ListObjects OK: L=511, B=18, T=4 = 533; total = 1924
    assert ga == (1_000 + 341) + 35 + 15 + 511 + 18 + 4
    # ReadObject OK: L=3822, B=47, T=29 = 3898; GetObjectMetadata OK: L=1, B=80, T=27 = 108;
    # GetBucketMetadata OK: L=19, B=20, T=1 = 40; total = 4046
    assert gb == (3_822 + 47 + 29) + (1 + 80 + 27) + (19 + 20 + 1)


# ---------------------------------------------------------------------------
# render with planned_a (step 6) - mutation tests covered by headroom test above
# ---------------------------------------------------------------------------

def test_the_google_estimate_line_is_in_the_report():
    text = src.render({}, {(L, "WriteObject", "OK"): 10, (L, "ReadObject", "OK"): 20}, "A", "B", "S", "c")
    assert "Google's billed estimate (successful reads, writes and listings only): Class A 10   Class B 20" in text


# mutation 5: google estimate line must be present
def test_the_report_contains_google_billed_estimate_not_just_the_safe_count():
    text = src.render({}, {}, "A", "B", "S", "c")
    assert "Google's billed estimate" in text


# ---------------------------------------------------------------------------
# dynamic --start default and --planned-a flag (step 6)
# ---------------------------------------------------------------------------

def test_the_default_start_comes_from_the_clock():
    nov_clock = lambda: datetime(2026, 11, 15, 10, 0, 0, tzinfo=timezone.utc)
    nov_start = src.default_month_start(datetime(2026, 11, 15, 10, 0, 0, tzinfo=timezone.utc))
    nov_end = "2026-11-15T10:00:00Z"
    # With a November clock and first-end still in October, the first window is skipped.
    code, out, err, api = run_main({nov_end: [GOOD_FIRST]}, clock=nov_clock)
    assert code == 0 and "first-window check skipped" in out
    assert {query(u)["interval.startTime"][0] for u in api.calls} == {nov_start}
    assert f"window 2: {nov_start}" in out


# mutation 6: dynamic start must be used (not None or a hardcoded value)
def test_an_explicit_start_overrides_the_clock():
    explicit_start = "2026-09-01T07:00:00Z"
    explicit_end = "2026-09-30T10:00:00Z"
    code, out, _, api = run_main({END1: [GOOD_FIRST], explicit_end: [GOOD_FIRST]},
                                 ["--start", explicit_start, "--first-end", END1, "--end", explicit_end])
    assert code == 0
    assert {query(u)["interval.startTime"][0] for u in api.calls} == {explicit_start}


def test_planned_a_flag_reaches_render_and_shows_remaining():
    windows = {END1: [GOOD_FIRST], END2: [GOOD_FIRST]}
    code, out, err, _ = run_main(windows, ["--planned-a", "5"])
    assert code == 0 and "with 5 planned" in out


def test_planned_a_zero_shows_no_planned_line():
    windows = {END1: [GOOD_FIRST], END2: [GOOD_FIRST]}
    code, out, err, _ = run_main(windows)
    assert "planned" not in out


# ---------------------------------------------------------------------------
# first-window skip when first-end is at or before start (step 6)
# ---------------------------------------------------------------------------

def test_first_window_is_skipped_when_first_end_is_at_or_before_start():
    nov_start = "2026-11-01T07:00:00Z"
    nov_end = "2026-11-10T12:00:00Z"
    # FIRST_P2_END (Oct 29) < nov_start (Nov 1): first window should be skipped
    code, out, err, api = run_main({nov_end: [GOOD_FIRST]},
                                   ["--start", nov_start, "--end", nov_end])
    assert code == 0
    assert "first-window check skipped" in out
    assert len(api.calls) == 1                          # only one request, not two
    assert query(api.calls[0])["interval.endTime"][0] == nov_end


def test_first_window_is_also_skipped_when_first_end_equals_start():
    equal = "2026-11-01T07:00:00Z"
    nov_end = "2026-11-10T12:00:00Z"
    code, out, err, api = run_main({nov_end: [GOOD_FIRST]},
                                   ["--start", equal, "--first-end", equal, "--end", nov_end])
    assert code == 0 and "first-window check skipped" in out and len(api.calls) == 1


# mutation 7: the skip condition must use <= not <
def test_first_window_runs_when_first_end_is_strictly_after_start():
    before_end = "2026-10-28T11:03:00Z"        # strictly before FIRST_P2_END
    code, out, err, api = run_main({before_end: [GOOD_FIRST], END2: [GOOD_FIRST]},
                                   ["--first-end", before_end])
    assert len(api.calls) == 2                  # both windows queried, not skipped
    assert "first-window check skipped" not in out
