"""
scripts/storage_request_counts.py: P2, the month's Cloud Storage request counts from Cloud Monitoring, read only.

    PYTHONPATH=. python scripts/storage_request_counts.py

It asks the Cloud Monitoring API (metric storage.googleapis.com/api/request_count, grouped by bucket, method and response
code) how many requests the project made from the start of the billing month, with YOUR gcloud login (it runs
`gcloud auth print-access-token --project=payments-512910` itself; run it with GOOGLE_APPLICATION_CREDENTIALS unset, so
it is not the bronze-writer or cdc-consumer file). It makes GET requests to Monitoring, which are not Cloud Storage
operations: it spends no Class A or Class B operation.

Two windows from the month start. The first ends at 2026-10-29 11:03 UTC and must reproduce the first P2 (6,709
requests: cdc_landing 6,367, fintech_bronze 254, payments-512910-tfstate 88): that is the check that this script and
that one read the same thing. The second ends now. The difference is what happened between: after the P5 run it should be
that run's two listings and 1,816 reads of cdc_landing and nothing else; after the first Bronze write it is the write.

Class A and B are MY mapping from the method names (Google's operations table recalled, NOT verified against its page);
Class A is given with and without the refused creates (WriteObject answered FAILED_PRECONDITION), because whether Google
bills a refused create is not known. A method that is not in the mapping is listed apart, never dropped. These are
request counts against the second-hand free-tier limits (5,000 Class A and 50,000 Class B a month), not money, and the
billing month's start in Pacific time (07:00 UTC while daylight saving lasts) is an assumption.

Exit codes: 0 done and the first window reproduces the first P2; 2 done but it does not (the printed figures cannot then be
compared with the earlier ones); 1 the token or the API failed (Google's message is printed in full).
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Callable, Optional

PROJECT = "payments-512910"
METRIC = "storage.googleapis.com/api/request_count"
API = "https://monitoring.googleapis.com/v3/projects/{project}/timeSeries"
MONTH_START = "2026-10-01T07:00:00Z"
FIRST_P2_END = "2026-10-29T11:03:00Z"
FIRST_P2_TOTALS = {"cdc_landing": 6_367, "fintech_bronze": 254, "payments-512910-tfstate": 88}   # 6,709 in all
CLASS_A = ("WriteObject", "ListObjects", "SetIamPolicy", "UpdateBucketMetadata")
CLASS_B = ("ReadObject", "GetObjectMetadata", "GetBucketMetadata", "GetIamPolicy", "TestIamPermissions",
           "GetBucketStorageLayout")
FREE = ("DeleteObject",)
REFUSED = ("WriteObject", "FAILED_PRECONDITION")      # a create the bucket refused because the name was taken
LIMIT_A, LIMIT_B = 5_000, 50_000
FIRST_WRITE_A = (110, 125)                            # GCS Plan (V2) section 12, plus the marker, the catalog copy and listings

Counts = dict   # (bucket, method, response_code) -> requests


# ---------------------------------------------------------------------------
# reading Monitoring
# ---------------------------------------------------------------------------

def request_url(project: str, start: str, end: str, page_token: Optional[str] = None) -> str:
    params = [
        ("filter", f'metric.type = "{METRIC}" AND resource.type = "gcs_bucket"'),
        ("interval.startTime", start), ("interval.endTime", end),
        ("aggregation.alignmentPeriod", "86400s"), ("aggregation.perSeriesAligner", "ALIGN_SUM"),
        ("aggregation.crossSeriesReducer", "REDUCE_SUM"),
        ("aggregation.groupByFields", "resource.label.bucket_name"),
        ("aggregation.groupByFields", "metric.label.method"),
        ("aggregation.groupByFields", "metric.label.response_code"),
        ("view", "FULL"),
    ]
    if page_token:
        params.append(("pageToken", page_token))
    return API.format(project=project) + "?" + urllib.parse.urlencode(params)


def counts_between(project: str, start: str, end: str, token: str, get: Callable[[str, str], dict]) -> Counts:
    """Every request in [start, end) as (bucket, method, response_code) -> count, following the pages."""
    counts: Counts = {}
    page = None
    while True:
        body = get(request_url(project, start, end, page), token)
        for series in body.get("timeSeries", []):
            key = (series["resource"]["labels"]["bucket_name"], series["metric"]["labels"]["method"],
                   series["metric"]["labels"].get("response_code", ""))
            for point in series.get("points", []):
                counts[key] = counts.get(key, 0) + int(point["value"]["int64Value"])
        page = body.get("nextPageToken")
        if not page:
            return counts


def http_get(url: str, token: str) -> dict:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"the Monitoring API answered {error.code}: {error.read().decode(errors='replace')[:1500]}") from error
    except urllib.error.URLError as error:
        raise RuntimeError(f"could not reach the Monitoring API: {error.reason}") from error


def gcloud_token() -> str:
    try:
        run = subprocess.run(["gcloud", "auth", "print-access-token", f"--project={PROJECT}"], capture_output=True,
                             text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RuntimeError(f"could not run gcloud for a token: {error}") from error
    if run.returncode != 0 or not run.stdout.strip():
        raise RuntimeError(f"gcloud could not give a token (exit {run.returncode}): {run.stderr.strip()[:500]}")
    return run.stdout.strip()


# ---------------------------------------------------------------------------
# what the counts mean
# ---------------------------------------------------------------------------

def by_bucket(counts: Counts) -> dict[str, int]:
    totals: dict[str, int] = {}
    for (bucket, _, _), n in counts.items():
        totals[bucket] = totals.get(bucket, 0) + n
    return dict(sorted(totals.items()))


def classify(counts: Counts) -> dict:
    """Class A (without and with the refused creates), Class B, free and unmapped requests, for all buckets and each."""
    out: dict = {}
    for scope in ["all"] + sorted({b for b, _, _ in counts}):
        a_kept = a_refused = b = free = 0
        unmapped: dict[str, int] = {}
        for (bucket, method, code), n in counts.items():
            if scope != "all" and bucket != scope:
                continue
            if method in CLASS_A:
                if (method, code) == REFUSED:
                    a_refused += n
                else:
                    a_kept += n
            elif method in CLASS_B:
                b += n
            elif method in FREE:
                free += n
            else:
                unmapped[method] = unmapped.get(method, 0) + n
        out[scope] = {"a_low": a_kept, "a_high": a_kept + a_refused, "b": b, "free": free, "unmapped": dict(sorted(unmapped.items()))}
    return out


def difference(now: Counts, before: Counts) -> Counts:
    """Requests in `now` that are not in `before`, key by key (a key that went down is shown negative)."""
    out = {key: now.get(key, 0) - before.get(key, 0) for key in set(now) | set(before)}
    return {key: n for key, n in sorted(out.items()) if n != 0}


def reproduces_first_p2(counts: Counts) -> tuple[bool, str]:
    found = by_bucket(counts)
    if found == dict(sorted(FIRST_P2_TOTALS.items())):
        return True, f"the first window reproduces the first P2 ({sum(found.values()):,} requests: " + ", ".join(
            f"{b} {n:,}" for b, n in found.items()) + ")"
    parts = [f"{b}: found {found.get(b, 0):,}, first P2 {FIRST_P2_TOTALS.get(b, 0):,}" for b in sorted(set(found) | set(FIRST_P2_TOTALS))
             if found.get(b, 0) != FIRST_P2_TOTALS.get(b, 0)]
    return False, ("the first window does NOT reproduce the first P2 (" + "; ".join(parts) + "): the figures below cannot be "
                   "compared with it until that is understood (late data, a different window, or a different reading)")


def table(counts: Counts, title: str) -> list[str]:
    lines = [title, f"  {'bucket':<26} {'method':<24} {'response':<20} {'requests':>9}"]
    for (bucket, method, code), n in sorted(counts.items()):
        lines.append(f"  {bucket:<26} {method:<24} {code:<20} {n:>9,}")
    lines.append(f"  total {sum(counts.values()):,} requests")
    return lines


def render(first: Counts, now: Counts, first_end: str, end: str, start: str, check: str) -> str:
    lines = [f"window 1: {start} to {first_end}", f"window 2: {start} to {end}", "", check, ""]
    lines += table(first, f"window 1 ({first_end})") + [""] + table(now, f"window 2 ({end})") + [""]
    moved = difference(now, first)
    lines += table(moved, "since window 1 (window 2 less window 1)") if moved else ["since window 1: no request"]
    lines += ["", "classes (my mapping, not verified against Google's table; request counts, not billing)"]
    classes = classify(now)
    for scope, c in classes.items():
        a = f"{c['a_low']:,}" if c["a_low"] == c["a_high"] else f"{c['a_low']:,} to {c['a_high']:,}"
        lines.append(f"  {scope:<26} Class A {a:>16}   Class B {c['b']:>7,}   deletes (free) {c['free']:>6,}")
        for method, n in c["unmapped"].items():
            lines.append(f"    NOT MAPPED: {method} {n:,} (not counted in A or B)")
    total = classes["all"]
    lines += ["", f"against the second-hand free-tier limits: Class A {total['a_low'] / LIMIT_A:.1%} to {total['a_high'] / LIMIT_A:.1%} of {LIMIT_A:,}; "
                  f"Class B {total['b'] / LIMIT_B:.1%} of {LIMIT_B:,}"]
    low, high = FIRST_WRITE_A
    room = LIMIT_A - total["a_high"]
    lines.append(f"headroom for the first write (about {low} to {high} Class A, plus one marker, one catalog copy and a few listings): "
                 f"{room:,} Class A left at the high count" + ("" if room >= high + 10 else "  (NOT ENOUGH)"))
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None, *, token_source: Callable[[], str] = gcloud_token,
         get: Callable[[str, str], dict] = http_get, clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
         out=None, err=None) -> int:
    out, err = out or sys.stdout, err or sys.stderr
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--project", default=PROJECT)
    parser.add_argument("--start", default=MONTH_START, help="the start of the billing month, UTC (an assumption: 07:00 UTC)")
    parser.add_argument("--first-end", default=FIRST_P2_END, help="the end of the first window: when the first P2 was read")
    parser.add_argument("--end", default=None, help="the end of the second window (default: now)")
    args = parser.parse_args(argv)
    end = args.end or clock().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:00Z")
    try:
        token = token_source()
        first = counts_between(args.project, args.start, args.first_end, token, get)
        now = counts_between(args.project, args.start, end, token, get)
    except RuntimeError as error:
        print(f"error: {error}", file=err)
        return 1
    matched, check = reproduces_first_p2(first)
    print(render(first, now, args.first_end, end, args.start, check), file=out)
    return 0 if matched else 2


if __name__ == "__main__":
    sys.exit(main())
