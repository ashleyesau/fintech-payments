# Cost and performance

The pipeline runs on GCP's Always Free tier for GCS: 5 GB storage, 5,000 Class A operations and 50,000 Class B operations per month in `us-east1`.

All GCS figures come from Google Cloud Monitoring billing estimates and `scripts/storage_request_counts.py`.

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

A no-op write (all events already in Bronze) costs about 2,610 Class B plus one `_runs/` listing.

### `bronze.verify` (full proof)

Each run:
- **~1 Class A** (the `_runs/` listing)
- **~908 Class B** (one read per landed object, plus Bronze metadata reads)

The 1,000-merchant run with ~2,610 objects costs roughly 2,610 Class B.

### `register_bronze_bq.py`

Five BigQuery metadata API calls (one DDL statement per table). BigQuery DDL is free. The only GCS cost is five Class B reads that BigQuery performs when validating the external table schema.

### Month totals (GCS phase, October 2026)

By the end of the GCS phase (G0 to G5), the October billing month had used:
- **~2,337 Class A** (46.7% of 5,000)
- **~19,558 Class B** (39.1% of 50,000)

This covered the G0 BigQuery compatibility probe, all check and verify runs, the full Bronze write, four verify runs, two crash-test bucket operations, and several measurement scripts.

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
| 1,000-merchant run | 8,537,090 | 2,610 | 124 min (Airflow bronze_write task) |
| 1,000-merchant run | 8.5M | 2,610 | 124 min (full write, fresh bucket) |

Bronze was reset before this run. A write that appends only new events on top of an existing Bronze table would be proportionally shorter.

### Airflow DAG (1,000-merchant run)

| Task | Duration |
|---|---|
| `bronze_write` | 124 min |
| `register_bq` | 23 sec |
| `dbt_marts` | 65 sec |

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

---

## Free tier headroom

The billing cycle was not exhausted in any month where careful operation was maintained.

The key constraint is Class A operations, dominated by the CDC consumer in `--follow` mode. The main rule: use `--once` for a production backfill. Reserve `--follow` for short streaming demos only.

---

*For the DAG that runs these steps, see [Orchestration](orchestration.md). For the data layout that determines how many files are written, see [Data model](data-model.md).*
