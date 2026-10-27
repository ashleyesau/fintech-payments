"""
measure_upload_latency.py: how long one small object takes to reach the GCS bucket.

The 50 MiB upload test in M0 measured bandwidth. The consumer will upload many objects of
tens of KB, and each pays a round trip from South Africa to us-east1, so this measures the
per-object cost. Batch size, poll interval and upload concurrency are fixed from the result.

It uploads about 30 objects (sizes drawn to look like the real gzipped objects: a few hundred
bytes up to about 400 KB, mean about 60 KB) twice, one at a time and then with a thread pool,
reads a few back, and deletes everything it created. It uses the same call the consumer's
writer will use: an upload with content type application/gzip and the precondition
if_generation_match=0, with the library's own retry switched off so a failure shows up
instead of hiding as extra latency.

    PYTHONPATH=. python scripts/measure_upload_latency.py

Cost: one warm-up upload plus two uploads per object, so 61 of the 5,000 monthly Class A
operations at the default 30 objects. Reads are Class B (50,000 a month). Deletes are free.

Safety: it only writes under the prefix _latency_test/ and only deletes the object names it
created itself. It never lists the bucket (a listing is a Class A operation). The bucket
comes from CDC_GCS_BUCKET (default cdc_landing). Authentication is the application-default
login; nothing is read from or written to the repo.

Needs the google-cloud-storage package: pip install "google-cloud-storage>=3"
"""
from __future__ import annotations

import math
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import click

from cdc.config import settings

SAFE_PREFIX = "_latency_test/"
CONTENT_TYPE = "application/gzip"
TIMEOUT_SECONDS = 60

# Object size, in bytes after gzip. The real sample (Air data, 194,361 events) gave 317 objects,
# mean 55 KB, largest 350 KB, smallest 0.4 KB. Sizes are evenly spaced quantiles of a log-normal
# (so a small run has the right spread, with no sampling noise), clipped to the observed range;
# 30 objects give a mean of about 55 KB. Shuffled with a fixed seed, so the same run order
# and sizes are used every time.
MIN_BYTES, MAX_BYTES = 400, 400_000
LOG_MEDIAN, LOG_SPREAD = 10.157, 1.3

# 194,361 events landed in 317 objects, so about 613 events an object. Used only for the
# projection at the end, to turn per-object times into a time for one batch.
EVENTS_PER_OBJECT = 613


def make_payloads(count: int, seed: int = 42) -> list[bytes]:
    """`count` byte strings with realistic object sizes. Random bytes: the size is what matters
    to the upload, and these stand in for data that is already gzipped."""
    rng = random.Random(seed)
    normal = statistics.NormalDist()
    sizes = [int(min(MAX_BYTES, max(MIN_BYTES, math.exp(LOG_MEDIAN + LOG_SPREAD * normal.inv_cdf((i + 0.5) / count)))))
             for i in range(count)]
    rng.shuffle(sizes)
    return [rng.randbytes(size) for size in sizes]


def upload(bucket, name: str, data: bytes) -> tuple[float, str | None]:
    """One upload as the consumer will make it. Returns (seconds, error text or None)."""
    blob = bucket.blob(name)
    started = time.perf_counter()
    try:
        blob.upload_from_string(data, content_type=CONTENT_TYPE, if_generation_match=0,
                                timeout=TIMEOUT_SECONDS, retry=None)
    except Exception as error:  # report every kind of failure, do not hide any
        return time.perf_counter() - started, f"{type(error).__name__}: {error}"
    return time.perf_counter() - started, None


def read(bucket, name: str) -> tuple[float, bytes | None, str | None]:
    blob = bucket.blob(name)
    started = time.perf_counter()
    try:
        data = blob.download_as_bytes(timeout=TIMEOUT_SECONDS, retry=None)
    except Exception as error:
        return time.perf_counter() - started, None, f"{type(error).__name__}: {error}"
    return time.perf_counter() - started, data, None


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(fraction * len(ordered)))]


def run_phase(bucket, names: list[str], payloads: list[bytes], workers: int) -> dict:
    """Upload every payload; one at a time when workers is 1, else with a thread pool."""
    started = time.perf_counter()
    if workers == 1:
        results = [upload(bucket, n, d) for n, d in zip(names, payloads)]
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(lambda pair: upload(bucket, *pair), zip(names, payloads)))
    wall = time.perf_counter() - started
    good = [(seconds, len(data)) for (seconds, error), data in zip(results, payloads) if error is None]
    errors = [error for _, error in results if error is not None]
    return {"wall": wall, "latencies": [s for s, _ in good], "sizes": [b for _, b in good],
            "total_bytes": sum(b for _, b in good), "errors": errors}


def report_phase(title: str, phase: dict) -> None:
    latencies = phase["latencies"]
    click.echo(f"{title}: {len(latencies)} ok, {len(phase['errors'])} failed, "
               f"{phase['total_bytes'] / 1e6:.1f} MB in {phase['wall']:.2f} s")
    if latencies:
        click.echo(f"  per object: min {min(latencies) * 1000:.0f} ms, median {statistics.median(latencies) * 1000:.0f} ms, "
                   f"mean {statistics.fmean(latencies) * 1000:.0f} ms, p90 {percentile(latencies, 0.9) * 1000:.0f} ms, "
                   f"max {max(latencies) * 1000:.0f} ms")
        click.echo(f"  throughput: {len(latencies) / phase['wall']:.2f} objects/s, "
                   f"{phase['total_bytes'] / phase['wall'] / 1e6:.2f} MB/s")
    for error in sorted(set(phase["errors"])):
        click.echo(f"  ERROR ({phase['errors'].count(error)}x): {error}")


def fixed_cost_and_rate(phase: dict) -> tuple[float, float] | None:
    """Least-squares fit of latency = fixed + size / rate over the one-at-a-time uploads:
    the per-object cost that does not depend on size, and the transfer rate. None if the
    data cannot support a fit (too few points, or no upward trend with size)."""
    if len(phase["latencies"]) < 8 or len(set(phase["sizes"])) < 3:
        return None
    fit = statistics.linear_regression(phase["sizes"], phase["latencies"])
    if fit.slope <= 0:
        return None
    return fit.intercept, 1.0 / fit.slope


def delete_all(bucket, names: list[str], workers: int) -> tuple[int, list[str]]:
    """Delete exactly the names given. Deletes are free. A missing object counts as deleted."""
    from google.api_core.exceptions import NotFound

    def one(name: str) -> str | None:
        try:
            bucket.blob(name).delete(timeout=TIMEOUT_SECONDS, retry=None)
        except NotFound:
            return None
        except Exception as error:
            return f"{name}: {type(error).__name__}: {error}"
        return None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        problems = [p for p in pool.map(one, names) if p is not None]
    return len(names) - len(problems), problems


def measure(bucket, prefix: str, objects: int, workers: int, reads: int, attempted: list[str]) -> None:
    """Run the whole measurement against `bucket`. Every object name is appended to `attempted`
    before it is written, so the caller can delete them even if this is interrupted."""
    if not prefix.startswith(SAFE_PREFIX) or not prefix.endswith("/"):
        raise click.ClickException(f"prefix must start with {SAFE_PREFIX} and end with /, got {prefix!r}")
    payloads = make_payloads(objects)
    sizes = [len(p) for p in payloads]
    names_seq = [f"{prefix}seq/object_{i:03d}.ndjson.gz" for i in range(objects)]
    names_par = [f"{prefix}par/object_{i:03d}.ndjson.gz" for i in range(objects)]
    warmup = f"{prefix}warmup.ndjson.gz"
    attempted.append(warmup)

    click.echo(f"bucket {bucket.name}, prefix {prefix}")
    click.echo(f"{objects} objects, {sum(sizes) / 1e6:.1f} MB: min {min(sizes):,} B, median {statistics.median(sizes):,.0f} B, "
               f"mean {statistics.fmean(sizes):,.0f} B, max {max(sizes):,} B")
    click.echo(f"class A operations: {1 + 2 * objects}, reads (class B): {reads}")

    # The first upload on a new client pays for the TLS handshake and token fetch. Time it on
    # its own, so it does not distort the one-at-a-time figures.
    cold_seconds, cold_error = upload(bucket, warmup, payloads[0])
    click.echo(f"first upload on a new client (cold, 1 object of {sizes[0]:,} B): {cold_seconds * 1000:.0f} ms"
               + (f"  ERROR: {cold_error}" if cold_error else ""))
    if cold_error:
        return

    attempted.extend(names_seq)
    sequential = run_phase(bucket, names_seq, payloads, workers=1)
    report_phase("one at a time", sequential)

    attempted.extend(names_par)
    parallel = run_phase(bucket, names_par, payloads, workers=workers)
    report_phase(f"{workers} at a time", parallel)

    fit = fixed_cost_and_rate(sequential)
    if fit:
        fixed, rate = fit
        click.echo(f"fit over the one-at-a-time uploads: about {max(fixed, 0) * 1000:.0f} ms fixed per object "
                   f"plus {rate / 1e6:.2f} MB/s of transfer (indicative: {len(sequential['latencies'])} points)")
    else:
        click.echo("fit: not enough spread in the data to separate fixed cost from transfer rate")

    if reads and sequential["latencies"]:
        timings, mismatches, failures = [], 0, []
        for name, payload in zip(names_seq[:reads], payloads[:reads]):
            seconds, data, error = read(bucket, name)
            if error:
                failures.append(error)
            else:
                timings.append(seconds)
                mismatches += data != payload
        if timings:
            click.echo(f"read back {len(timings)} objects (the replay compare): median {statistics.median(timings) * 1000:.0f} ms, "
                       f"max {max(timings) * 1000:.0f} ms, {mismatches} differing from what was written")
        for error in sorted(set(failures)):
            click.echo(f"  ERROR reading ({failures.count(error)}x): {error}")

    if sequential["latencies"] and parallel["latencies"]:
        # One flush of the default 10,000 changes is about this many objects of mean size.
        per_batch = 10_000 / EVENTS_PER_OBJECT
        seq_batch = per_batch * statistics.fmean(sequential["latencies"])
        par_batch = per_batch * parallel["wall"] / len(parallel["latencies"])
        click.echo(f"projection for one flush of 10,000 changes (about {per_batch:.0f} objects of mean size):")
        click.echo(f"  one at a time: {seq_batch:.1f} s, so uploads alone sustain about {10_000 / seq_batch:,.0f} changes/s")
        click.echo(f"  {workers} at a time: {par_batch:.1f} s, so about {10_000 / par_batch:,.0f} changes/s")
        click.echo("  for comparison the Air generates about 1,400 rows/s end to end, and the CPU work "
                   "(decode, envelope, gzip, fetch) takes about 2.5 s a flush")


@click.command()
@click.option("--bucket", "bucket_name", default=settings.gcs_bucket, show_default=True)
@click.option("--prefix", default=None, help=f"Must start with {SAFE_PREFIX}. Default: a timestamped one.")
@click.option("--objects", default=30, show_default=True, help="Objects per phase.")
@click.option("--workers", default=4, show_default=True, help="Threads in the parallel phase.")
@click.option("--reads", default=5, show_default=True, help="Objects to read back (0 to skip).")
@click.option("--project", default=None, help="GCP project, if the client cannot work it out from the login.")
def main(bucket_name: str, prefix: str | None, objects: int, workers: int, reads: int, project: str | None) -> None:
    try:
        from google.cloud import storage
    except ImportError:
        raise click.ClickException('google-cloud-storage is not installed: pip install "google-cloud-storage>=3"')
    if objects < 1 or workers < 1 or reads < 0:
        raise click.ClickException("objects and workers must be at least 1, reads at least 0")
    prefix = prefix or f"{SAFE_PREFIX}{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}/"

    client = storage.Client(project=project) if project else storage.Client()
    bucket = client.bucket(bucket_name)
    attempted: list[str] = []
    try:
        measure(bucket, prefix, objects, workers, reads, attempted)
    finally:
        if attempted:
            deleted, problems = delete_all(bucket, attempted, workers)
            click.echo(f"deleted {deleted} of {len(attempted)} objects")
            for problem in problems:
                click.echo(f"  COULD NOT DELETE {problem}")
            if problems:
                click.echo(f"  delete the rest by hand: gcloud storage rm --recursive gs://{bucket_name}/{prefix}")


if __name__ == "__main__":
    main()
