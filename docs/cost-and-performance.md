# Cost and performance

The pipeline runs on GCP's Always Free tier for GCS (5 GB storage, 5,000 Class A operations, 50,000 Class B operations per month in `us-east1`). This document records measured costs and timings, the design choices that keep operations within the free tier, and the performance figures from the largest run.

All GCS figures are from Google's Cloud Monitoring billing estimates and the `scripts/storage_request_counts.py` script. Class A operations are writes, listings, and object creates. Class B operations are reads.

---

## GCS operation costs by step

### CDC consumer

The consumer creates one object per table per event date. For the 100-merchant year (908 objects total):
- **908 Class A** (one per object written)
- **Class B**: minimal (a listing of `_runs/` per batch, reads for content-hash comparisons on collisions)

A replay that finds all objects already present writes nothing and costs about 1 Class A (the `_runs/` listing).

The monthly 5,000 Class A limit matters for `--follow` mode. At the default 3,600x streaming speed, the consumer lands about 315 objects an hour. A long streaming session uses the month's budget in about 16 hours. `--once` drains in a single batch and is the right mode for a backfill scenario.

### Bronze write

For the 100-merchant year (1,017,902 events):
- **110 to 130 Class A**: one `_runs/` listing, one object per Parquet file, the run marker, and the catalog copy. The partition-by-month design is what keeps this low: without it, a per-event-date partition would have produced about 857 objects instead of 65 data files.
- **~908 Class B**: one read per NDJSON object in the landing.

A no-op write (all events already in Bronze) costs about 908 Class B plus one `_runs/` listing and reads of the existing metadata files.

### `bronze.verify` (full proof)

Each run of the full verify: about **1 Class A** (the `_runs/` listing) and **~908 Class B** (one read per landed object, plus Bronze metadata reads). The 1,000-merchant run with ~2,610 objects costs roughly 2,610 Class B.

Four verify runs were completed through the development of the GCS phase, all within the free tier.

### `register_bronze_bq.py`

Five BigQuery metadata API calls (one DDL statement per table). BigQuery DDL is free. The only GCS cost is the five Class B reads that BigQuery performs when it validates the external table schema.

### Month totals (GCS phase, October 2026)

By the end of the GCS phase (G0 to G5), the October billing month had used approximately 2,337 Class A (46.7% of 5,000) and 19,558 Class B (39.1% of 50,000). This included the G0 BigQuery compatibility probe, all check and verify runs, the full 100-merchant write, four verify runs, two crash-test bucket operations, and several measurement scripts.

---

## Timing (measured)

### Generator

| Run | Merchants onboarded | Duration |
|---|---|---|
| Sandbox (Python 3.11) | 100, 365 days | 183 seconds |
| MacBook Air (Python 3.13, Intel, 2017) | 100, 365 days | 476 seconds |
| MacBook Air | 1,000, 455 days | 3,337 seconds (~56 min) |

Throughput: backfill at roughly 22,000 rows per second (batch commits of 2,000 rows). Streaming at roughly 900 rows per second (one commit per event).

### CDC consumer

The pipeline decode speed was measured at about 10,142 events per second on the MacBook Air (Intel), giving roughly 2.8x headroom over the ~1,400 rows per second that the Air generates at 3,600x speed.

The 1,000-merchant run consumed 8.95 million changes in two runs (a WiFi interruption mid-way through the first run) and produced 2,610 objects in `gs://cdc_landing/cdc/cdc_r03/`.

### Bronze write

| Run | Events | Objects read | Duration | Note |
|---|---|---|---|---|
| 100-merchant year | 1,017,902 | 908 | ~6 min | bronze.check only |
| 1,000-merchant run | 8.5M | 2,610 | 124 min | Full write, fresh bucket |

The 124-minute figure is the full write from scratch (Bronze was reset before this run). A write that appends only the new events on top of an existing Bronze table would be proportionally shorter.

### Airflow DAG (1,000-merchant run)

| Task | Duration |
|---|---|
| `bronze_write` | 124 min |
| `register_bq` | 23 sec |
| `dbt_marts` | 65 sec |

---

## Optimisations and their effects

### Month partitioning instead of day partitioning

The Bronze tables are partitioned by `month(event_date)`. The alternative was partitioning by day.

Day partitioning would produce one Parquet file per day per table (up to 365 per year per table, 1,825 total for five tables). Month partitioning produces one per month per table (up to 12 per year per table, 60 total). This reduces Class A costs by roughly 14x on the Bronze write and keeps the external table registration step fast (BigQuery reads fewer metadata files).

The measured cost with month partitioning: 65 Parquet data files for the 100-merchant year. The estimated cost with day partitioning: about 857 files.

### Two-pass streaming reader

The Bronze writer reads the landing in two passes. Pass 1 builds an in-memory index of every key across all 908 objects (about 253 to 261 MB above the starting process RSS on the MacBook Air). Pass 2 reads each object again and converts events to Arrow. The index holds about 12 bytes per event, so a 10-million-event run would take roughly 120 MB for the index alone.

The alternative (single-pass with materialised rows) would have required 5.8 KB per row for dict representation, pushing peak memory to roughly 5 GB for 1 million events, well beyond the MacBook Air's 8 GB.

### Batch size (100,000 rows per commit)

Each Bronze write batch is 100,000 rows, converted to Arrow before committing to Iceberg. Measured peak memory at this batch size: 669 to 673 MB above the start, with the index live. At the whole-table size (all events in one batch), memory was 5.0 GB, which is too large for the Air. 100,000 was chosen to stay under 1,000 MB.

### Isolated virtualenv for Airflow tasks

SQLAlchemy 1.4.x (Airflow) and SQLAlchemy 2.x (PyIceberg) cannot coexist. Rather than pinning Airflow's SQLAlchemy version or using a custom Airflow image, the pipeline dependencies are installed into `/opt/pipeline-venv` during the Docker image build. Each BashOperator task calls the venv's Python directly. This keeps the Airflow base image unmodified.

### WAL retention cap

`max_slot_wal_keep_size=3GB` in `docker-compose.yml`. An unread replication slot pins WAL indefinitely without this cap. The 1,000-merchant backfill produced roughly 11 GB of WAL. The initial cap of 3 GB caused slot `cdc_r02` to be invalidated mid-run. The cap was raised to 16 GB in commit `6c50189` to cover the 1,000-merchant case. At 750 bytes per row, 16 GB is roughly 21 million rows of headroom.

---

## Free tier headroom

After completing the V1 pipeline (all phases through session 38), the billing cycle had not been exhausted in any month where careful operation was maintained. The key constraint is Class A operations, which are dominated by the CDC consumer in `--follow` mode. The main practical rule: always use `--once` for a production backfill, and reserve `--follow` for short streaming demos.

---

*For the DAG that runs these steps, see [Orchestration](orchestration.md). For the data layout that determines how many files are written, see [Data model](data-model.md).*
