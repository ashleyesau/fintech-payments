# Cost and performance

The pipeline runs on GCP's Always Free tier for GCS: 5 GB storage, 5,000 Class A operations and 50,000 Class B operations per month in `us-east1`.

All GCS figures come from Google Cloud Monitoring billing estimates and `scripts/storage_request_counts.py`.

BigQuery costs for the dbt layers come from dbt run results and BigQuery job statistics. They are summarised in [BigQuery cost (dbt layers)](#bigquery-cost-dbt-layers) below and measured in full in the warehouse cost study (kept outside the repo). The BigQuery free tier is 1 TiB of queries and 10 GiB of storage per month.

- **Class A**: writes, listings, object creates
- **Class B**: reads

---

## GCS operation costs by step

### CDC consumer

The consumer creates one object per table per event date.

For the 1,000-merchant run (2,610 objects):
- **2,610 Class A** (one per object written)
- **Class B**: minimal (a `_runs/` listing per batch, reads for content-hash comparisons on collisions)

A replay that finds all objects already present writes nothing and costs about 1 Class A.

The monthly 5,000 Class A limit matters for `--follow` mode. At the default 3,600x streaming speed, the consumer lands about 315 objects an hour. A long streaming session uses the month's budget in about 16 hours. Use `--once` for a backfill.

### Bronze write

For the 1,000-merchant run (8,537,090 events):
- **~190 to 220 Class A**: one `_runs/` listing, one object per Parquet file, the run marker, and the catalog copy
- **~2,610 Class B**: one read per NDJSON object in the landing

The partition-by-month design keeps the Class A count low. Without it, a per-day partition would produce roughly 10x more data files.

A no-op write (all events already in Bronze) cost about 2,610 Class B plus one `_runs/` listing. Since 12 November 2026 a write run that finds nothing new since the newest run marker reads no landed object (see below); the figures in this document for a no-op run were measured before that change. The catalog rebuild that precedes every DAG write is separate: it scans the Bronze data files and costs Class B against `fintech_bronze`. The earlier estimate of about 300 per run is withdrawn, because the counts of 13 November suggest it is higher; one nothing-new run has not yet been measured with before-and-after counts.

On 10 November 2026 a failed and a passing `bronze_write` attempt together raised the month's Class B count by about 3,600, which is consistent with that figure for the passing run.

### `bronze.verify` (full proof)

Each run:
- **~1 Class A** (the `_runs/` listing)
- **One Class B per landed object, plus Bronze metadata reads.** The 1,000-merchant landing has 2,610 objects, so a run costs roughly 2,610 Class B. A smaller earlier run with 908 landed objects cost about 908.

### `register_bronze_bq.py`

Five BigQuery metadata API calls (one DDL statement per table). BigQuery DDL is free. The only GCS cost is five Class B reads that BigQuery performs when validating the external table schema.

### Month totals (GCS phase, October 2026)

By the end of the GCS phase (G0 to G5), the October billing month had used:
- **~2,337 Class A** (46.7% of 5,000)
- **~19,558 Class B** (39.1% of 50,000)

This covered the G0 BigQuery compatibility probe, all check and verify runs, the full Bronze write, four verify runs, two crash-test bucket operations, and several measurement scripts.

### Month totals (November 2026, to the 10th)

By the 10th of November, before the first end-to-end Airflow run, the month had used:
- **~3,350 to 3,560 Class A** (67% to 71% of 5,000)
- **~32,300 to 35,700 Class B** (65% to 72% of 50,000)

`cdc_landing` is the largest source (about 23,000 Class B, almost all object reads). The script's cumulative counts shifted slightly between runs, so these are ranges. They are request counts, not invoice amounts.

### Month totals (November 2026, to the 13th)

Measured on 13 November 2026 at 18:18 UTC, month to date:
- **3,415 to 3,416 Class A** (68.3% of 5,000; 3,408 by Google's billable estimate)
- **49,085 Class B** (98.2% of 50,000; 48,894 by Google's billable estimate)

By bucket, Class B was `cdc_landing` 32,532, `fintech_bronze` 16,500 and the Terraform state bucket 53. Class B rose by 7,718 between 10 and 13 November, which fits one full `bronze_verify` plus many catalog rebuilds and task tests, but the split was not measured. No pipeline runs are planned until the new month. The script's class mapping is its own and is not verified against Google's table.

---

## Timing (measured)

### Generator

| Run | Merchants onboarded | Duration |
|---|---|---|
| Sandbox (Python 3.11) | 100, 365 days | 183 seconds |
| MacBook Air (Python 3.13, Intel, 2017) | 100, 365 days | 476 seconds |
| MacBook Air | 1,000, 455 days | 3,337 seconds (~56 min) |

Throughput:
- Backfill: ~22,000 rows per second (batch commits of 2,000 rows)
- Streaming: ~900 rows per second (one commit per event)

### CDC consumer

Decode speed on the MacBook Air (Intel): ~10,142 events per second. This is about 2.8x headroom over the ~1,400 rows per second the Air generates at 3,600x speed.

The 1,000-merchant run consumed 8.95 million changes in two runs (a WiFi interruption mid-way through the first) and produced 2,610 objects in `gs://cdc_landing/cdc/cdc_r03/`.

### Bronze write

| Run | Events | Objects read | Duration |
|---|---|---|---|
| 1,000-merchant run, fresh bucket (Airflow `bronze_write` task) | 8,537,090 | 2,610 | 124 min |
| Same Bronze, nothing to append (Airflow, 10 November 2026) | 0 new | about 2,610 (one pass) | about 40 min |
| Local scaling run (warehouse cost study, section 8) | 1.1M | not recorded | 6.2 min |
| Local scaling run | 3.3M | not recorded | 18.5 min |
| Local scaling run | 11.0M | not recorded | 55.8 min |

Bronze was reset before the 124-minute run. Before 12 November 2026 a rerun that appended nothing still read every landed object (pass 1). In Airflow it took about 40 minutes, about a third of the fresh write. The local scaling runs found 47% to 53%. That matches the per-event cost of pass 1 measured on the Air (about 271 microseconds an event, 8.54M events, about 39 minutes).

A write run now skips pass 1 when the landing is unchanged since the newest run marker (tables at the marker's snapshots, the marker's object count, the marker's highest LSN). It lists the landing once and reads the marker, so it should take well under a minute and about 1 to 10 Class B. On the real bucket, the write step took 32 s and read zero landed objects (12 November 2026, three tests). A run with new data still does the full two passes.

The 124-minute run is the 7 November fresh write of 8,537,090 events: pass 1 read all 2,610 objects in about 42 minutes (about 3,400 events per second) and pass 2 took about 82 minutes (about 1,700 events per second) in 87 commits. A 100,000-row batch of `merchant_transactions` took about 56 s on average, and the three small tables took 37 to 44 s per commit, which suggests a large fixed cost per commit. Whether that cost is network delay or compression is unmeasured. A local 400,000-event run on 13 November reached about 2,000 events per second natively and about 1,830 in Docker, so Docker costs about 10% and is not the main source of slowness.

### Airflow DAG

Before 10 November 2026 (1,000-merchant run, one `dbt_marts` task running `dbt run --full-refresh`; the separate `dbt_test` step was not timed):

| Task | Duration |
|---|---|
| `bronze_write` | 124 min |
| `register_bq` | 23 sec |
| `dbt_marts` | 65 sec |

Measured full DAG runs (1,000-merchant run, nothing new landed):

| Run | Total | Notes |
|---|---|---|
| 10 November 2026 | 1 h 35 min | `bronze_write` about 53 min, `bronze_verify` about 38 min |
| 12 November 2026 | 1 h 7 min 34 s | After the `bronze_write` skip; `bronze_verify` 56 min 36 s |
| 12 November 2026, 21:25 UTC | 11 min 33 s | After both skips |

A run with new data was last measured at about 2 h 6 min (7 November, `bronze_write` about 124 min). A small increment is estimated at about 1 hour; that has not been measured.

After the DAG became a layered, test-gated build. Each task was run on its own with `airflow tasks test` on 10 November 2026. Durations come from task-log timestamps and are approximate:

| Task | Duration |
|---|---|
| `bronze_write` | about 40 min (nothing to append) |
| `register_bq` | 19 sec |
| `check_bq_schema` | about 11 sec |
| `dbt_build_staging` | about 87 sec (5 views, 42 tests) |
| `dbt_build_dimensions` | about 50 sec (3 tables, 29 tests) |
| `dbt_build_intermediate` | about 56 sec (2 incremental models, 4 tables, 43 tests) |
| `dbt_build_marts` | about 70 sec (11 incremental models, 54 tests) |
| `bronze_verify` | 56 min 36 s on 12 November 2026 (2,610 objects, 8.54M events), about 84% of that full DAG run; skipped when the landing is unchanged since a verified run marker, or when `bronze_write` just read every object itself and recorded its marker as verified (`force_verify` overrides) |

The four dbt layers total about 4.4 minutes. That includes about 168 data tests and a `dbt deps` per task, so it is not comparable with the 65 seconds for `dbt run --full-refresh` alone.

---

## BigQuery cost (dbt layers)

Measured on synthetic data at 1,000 merchants (8.3M transactions). Bytes billed is the cost metric; BigQuery bills at least 10 MiB per table queried. The full method, estimates and limitations are in the warehouse cost study (kept outside the repo). Every figure is a single run.

- A full rebuild of the 25-model graph billed 5.115 GB (38.2 s wall clock). Four models account for 94% of it.
- The two large intermediates and two large marts were then made incremental, partitioned and bounded by a literal date. Output was checked identical against copies of the old tables.

| Model | Full refresh (before) | Steady-state incremental | Recovery of 30 deleted days |
|---|--:|--:|--:|
| `int_payments__transaction_enriched` | 2,533 MB | 453 MB (-82%) | 1,253 MB (-51%) |
| `int_payments__merchant_daily_gpv` | 840 MB | 73 MB (-91%) | 188 MB (-78%) |
| `fct_settlement_daily` | 991 MB | not attributed | 223 MB (-77%) |
| `fct_revenue_daily` | 459 MB | not attributed | 105 MB (-77%) |

- A whole run of the four dbt layers in Airflow, models and tests together, billed 4.379 GB across 205 jobs, about 0.4% of the monthly free allowance. The 168 data tests billed 3.002 GB (68.6%) and the 37 model and other jobs billed 1.377 GB (31.4%), about 73% below the 5.115 GB full-refresh baseline. The baseline covered the 25 models only and did not include the tests.
- The enriched model cost about three times its dry-run estimate. The cause has not been investigated.

---

## Optimisations

### Month partitioning instead of day partitioning

The Bronze tables are partitioned by `month(event_date)`.

Day partitioning would produce up to 365 Parquet files per table per year (1,825 total for five tables). Month partitioning produces 12 per table per year (60 total). This reduces Class A costs by roughly 14x on the Bronze write.

Measured on the 100-merchant development run: 65 Parquet data files. Estimated for day partitioning of the same run: ~857 files. At 1,000 merchants the file count scales proportionally.

### Two-pass streaming reader

The Bronze writer reads the landing in two passes.

- **Pass 1**: builds an in-memory index across all 2,610 objects. The index holds about 12 bytes per event (~100 MB for 8.5M events).
- **Pass 2**: reads each object again and converts events to Arrow.

The alternative (single-pass with materialised rows) would have required 5.8 KB per row. For 8.5 million events, that is over 49 GB.

### Batch size (100,000 rows per commit)

Each Bronze write batch is 100,000 rows, converted to Arrow before committing to Iceberg.

Measured peak memory at this batch size: 669 to 673 MB above the start. At the whole-table size (all events in one batch), memory was 5.0 GB. 100,000 was chosen to stay under 1,000 MB.

### Isolated virtualenv for Airflow tasks

SQLAlchemy 1.4.x (Airflow) and SQLAlchemy 2.x (PyIceberg) cannot coexist. Rather than pinning Airflow's SQLAlchemy version or modifying the base image, the pipeline dependencies are installed into `/opt/pipeline-venv` at Docker image build time. Each task calls that venv's Python directly.

### WAL retention cap

`max_slot_wal_keep_size` in `docker-compose.yml` caps how much WAL an unread slot can pin.

The initial cap of 3 GB was too small. The 1,000-merchant backfill produced roughly 11 GB of WAL and invalidated slot `cdc_r02` mid-run. The cap was raised to 16 GB (commit `6c50189`). At 750 bytes per row, 16 GB is roughly 21 million rows of headroom.

### Incremental intermediates with literal date bounds

`int_payments__transaction_enriched` and `int_payments__merchant_daily_gpv` are incremental, partitioned by `event_date`, with a 9-day lookback (a 6-day maximum settlement lag plus the 3-day mart lookback). The cutoff is a literal date, computed with `run_query` on the target table, because BigQuery prunes partitions for a literal bound but not for a `MAX(...)` subquery. It is not based on the current date, because the synthetic event dates are 311 to 673 days old. The lag bound is a synthetic-data figure; dbt tests assert it (see the study, section 7.5).

---

## Free tier headroom

Before November the billing cycle was not exhausted in any month.

In November the binding constraint is Class B. By 13 November the month had used 98.2% of the 50,000 Class B allowance (49,085) and 68.3% of Class A. `bronze_verify` reads every landed object, about 2,610 reads, but only when `force_verify` is set or `bronze_write` could not record its marker as verified. `bronze_write` reads no landed object when nothing is new, but the catalog rebuild before it still costs Class B reads against `fintech_bronze`, and the cost of one run is unmeasured. A run with new data is estimated at about 2,610 for the write's first pass plus the new objects again, with `bronze_verify` skipped if the write records its marker as verified (not yet measured on a run with new data).

Any of these would exceed the allowance this month. At the unconfirmed overage rate of about $0.0004 per 1,000 Class B the excess would cost under a cent, but the decision is to stay inside the free tier, so no pipeline runs are planned until the new month. Class A, dominated by the CDC consumer in `--follow` mode, remains the other limit: use `--once` for a backfill and reserve `--follow` for short streaming demos.

BigQuery has plenty of room: 108.71 GiB of the 1 TiB query allowance (10.6%) and 3.38 GiB of the 10 GiB storage allowance were used by 13 November, and one run of the dbt layers bills about 4.4 GB.

---

*For the DAG that runs these steps, see [Orchestration](orchestration.md). For the data layout that determines how many files are written, see [Data model](data-model.md). For the BigQuery measurements, see the warehouse cost study (kept outside the repo).*
