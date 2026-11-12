# Cost and performance

The pipeline runs on GCP's Always Free tier for GCS: 5 GB storage, 5,000 Class A operations and 50,000 Class B operations per month in `us-east1`.

All GCS figures come from Google Cloud Monitoring billing estimates and `scripts/storage_request_counts.py`.

BigQuery costs for the dbt layers come from dbt run results and BigQuery job statistics. They are summarised in [BigQuery cost (dbt layers)](#bigquery-cost-dbt-layers) below and measured in full in the [warehouse cost study](warehouse-cost-study.md). The BigQuery free tier is 1 TiB of queries and 10 GiB of storage per month.

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

A no-op write (all events already in Bronze) cost about 2,610 Class B plus one `_runs/` listing. Since 12 November 2026 a write run that finds nothing new since the newest run marker reads no landed object (see below); the figures in this document for a no-op run were measured before that change.

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

A write run now skips pass 1 when the landing is unchanged since the newest run marker (tables at the marker's snapshots, the marker's object count, the marker's highest LSN). It lists the landing once and reads the marker, so it should take well under a minute and about 1 to 10 Class B. This has been tested on the local warehouse only; the time on the real bucket has not been measured. A run with new data still does the full two passes.

The 124-minute run (about 1,150 events per second) and the local 11.0M-event run (about 3,300 events per second) have not been reconciled. They differ in run, environment (Airflow in Docker against the host) and size.

### Airflow DAG

Before 10 November 2026 (1,000-merchant run, one `dbt_marts` task running `dbt run --full-refresh`; the separate `dbt_test` step was not timed):

| Task | Duration |
|---|---|
| `bronze_write` | 124 min |
| `register_bq` | 23 sec |
| `dbt_marts` | 65 sec |

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
| `bronze_verify` | not yet measured |

The four dbt layers total about 4.4 minutes. That includes about 168 data tests and a `dbt deps` per task, so it is not comparable with the 65 seconds for `dbt run --full-refresh` alone.

---

## BigQuery cost (dbt layers)

Measured on synthetic data at 1,000 merchants (8.3M transactions). Bytes billed is the cost metric; BigQuery bills at least 10 MiB per table queried. The full method, estimates and limitations are in the [warehouse cost study](warehouse-cost-study.md). Every figure is a single run.

- A full rebuild of the 25-model graph billed 5.115 GB (38.2 s wall clock). Four models account for 94% of it.
- The two large intermediates and two large marts were then made incremental, partitioned and bounded by a literal date. Output was checked identical against copies of the old tables.

| Model | Full refresh (before) | Steady-state incremental | Recovery of 30 deleted days |
|---|--:|--:|--:|
| `int_payments__transaction_enriched` | 2,533 MB | 453 MB (-82%) | 1,253 MB (-51%) |
| `int_payments__merchant_daily_gpv` | 840 MB | 73 MB (-91%) | 188 MB (-78%) |
| `fct_settlement_daily` | 991 MB | not attributed | 223 MB (-77%) |
| `fct_revenue_daily` | 459 MB | not attributed | 105 MB (-77%) |

- A whole run of the four dbt layers in Airflow, models and tests together, billed 4.379 GB across 200 jobs, about 0.4% of the monthly free allowance. A model-only estimate is about 1.2 GB. The rest is probably the roughly 168 data tests, each of which bills at least 10 MiB, but the split has not been measured.
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

The billing cycle was not exhausted in any month where careful operation was maintained.

The key constraint is Class A operations, dominated by the CDC consumer in `--follow` mode. The main rule: use `--once` for a production backfill. Reserve `--follow` for short streaming demos only.

In November the constraint to watch is Class B. `bronze_write` and `bronze_verify` each read every landed object, about 2,610 reads apiece, so one full DAG run costs roughly 5,000 Class B (an estimate). With the month already at about 72% of Class B by the 10th, that leaves room for only a few more full runs.

BigQuery has plenty of room: about 0.09 TiB of the 1 TiB allowance was used by the 10th of November, and one run of the dbt layers bills about 4.4 GB.

---

*For the DAG that runs these steps, see [Orchestration](orchestration.md). For the data layout that determines how many files are written, see [Data model](data-model.md). For the BigQuery measurements, see the [warehouse cost study](warehouse-cost-study.md).*
