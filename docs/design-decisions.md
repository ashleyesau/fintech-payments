# Design decisions

Each entry follows the same structure: what the choice was, what went wrong or what problem forced the decision, what alternatives were considered, what was chosen, and the trade-off that comes with it. The source for most of these is the DECISION_LOG in the project's Notion workspace, cross-checked against the code.

---

## Data generation

### OLTP store: self-hosted Postgres in Docker vs a managed database

**What went wrong first:** nothing. This was a day-one decision.

**Problem:** the pipeline needs a source of truth for the generator that also demonstrates CDC capability (logical replication, WAL-based change capture). Managed databases were candidates.

**Options:** AlloyDB (GCP managed Postgres, full CDC support), Cloud SQL Postgres, self-hosted Postgres 16 in Docker.

**Choice:** self-hosted Postgres 16 in Docker.

**Why:** AlloyDB is outside GCP's always-free tier. Cloud SQL Postgres has CDC support but adds cost and network complexity that obscures the mechanism. Self-hosted Postgres demonstrates `wal_level=logical`, `pgoutput`, replication slots and WAL retention directly, which is the interview-relevant signal. `wal_level=logical` was set from day one at no meaningful cost, so CDC could be added later without a Postgres restart.

**Trade-off:** no managed failover, no automatic backups. For a learning project running on a laptop, those are non-issues.

---

### WAL retention cap: 3 GB vs 16 GB

**What went wrong first:** the initial cap of 3 GB was set based on an estimate of 750 bytes per WAL row. The 1,000-merchant backfill produced roughly 11 GB of WAL, invalidating slot `cdc_r02` mid-run.

**Choice:** raised to 16 GB in `docker-compose.yml` (commit `6c50189`).

**Why:** 16 GB covers the 1,000-merchant case with headroom. Beyond that the disk on the MacBook Air becomes the constraint.

**Trade-off:** a slot left unread for a full 1,000-merchant backfill can consume 11 GB of disk before hitting the cap. Run `python -m cdc.lag` before large operations.

---

### Transition probabilities: daily roll vs daily hazard

**What went wrong first:** Causal Rules specifies merchant state transitions as per-window probabilities (a 30-day window for most conditions). The original implementation rolled each probability once per daily evaluation. The first Phase 8 measurement found that 30 to 33% of merchants changed state in the week of 2026-01-19, the state-snapping failure mode. A per-day roll of a 60% probability gives a 7-day hit rate of 1 - (1-0.60)^7 = 97.2%. The condition applied to many merchants simultaneously (January GPV drop), so nearly all of them transitioned in the same week.

**Choice:** convert each probability to a daily hazard: `1 - (1 - p) ** (1 / 30)`. A condition that holds for a 30-day window then produces the transition with the Causal Rules probability. After the fix, the busiest week moved 8 to 9% of merchants (down from 30 to 33%).

**Trade-off:** transitions now take longer to materialise. A merchant meeting a Stressed condition needs days or weeks to transition rather than hours. This matches economic reality better but makes the simulator slower to reach steady-state distributions.

---

### EFT funnel band amendment

**What went wrong first:** the design documents specified EFT funnel completion at 70 to 83% and OTP share of failures at 40 to 60%. The sourced parameters from Simulator Design (channel splits, success rates) produce 66% completion and 38% OTP share. These cannot be reached with the documented inputs.

**Choice:** amend the validation bands to 62 to 70% (completion) and 34 to 42% (OTP share), with a note in the config and a guarding test.

**Why:** the same approach was taken for the gross take rate band (amended from 2.5 to 3.2% to 1.9 to 2.4% when the sourced fee schedule could not produce the original band) and the write-off rate band (amended from 3 to 7% to 15 to 25% gross when the sourced PAR matrix implied a 20% annual flow). In all three cases, the sourced parameters and the original bands could not coexist. The sourced parameters were kept and the bands were amended.

**Trade-off:** the validation targets are weaker in these areas. The amended bands are documented and guarded by tests, so a future parameter change that breaks them fails loudly.

---

## CDC consumer

### Postgres client: psycopg3 (SQL-level peek) vs streaming replication

**What went wrong first:** psycopg 3.3.6 has no replication API. The streaming `START_REPLICATION` path requires psycopg2 (a separate dependency) or another client entirely.

**Choice:** SQL-level slot functions over an ordinary psycopg3 connection: `pg_logical_slot_peek_binary_changes` to read, `pg_replication_slot_advance` to confirm. No streaming replication protocol, no pg_hba complications.

**Why:** the SQL approach handles the full set of change types (insert, update, truncate, delete) and does not require a replication role or separate connection configuration. It has slightly higher per-call overhead than the streaming protocol, but the measured throughput (10,142 events per second on the Air) was about 2.8x the generation rate, which is sufficient headroom.

**Trade-off:** `pg_logical_slot_peek_binary_changes` calls are blocking and return in batches. The streaming protocol would allow sub-second latency. For this use case (periodic batch landing to GCS), latency is not a constraint.

---

### Landing format: gzipped NDJSON vs Parquet or Avro

**Choice:** gzipped NDJSON (newline-delimited JSON, one line per change event).

**Why:** NDJSON is append-safe, human-readable, and needs no schema registry. Each line is a self-contained change event: table name, operation, envelope metadata, and the full before/after row. Gzip compresses roughly 14x on this data. The landing is a raw, immutable change log. The Bronze writer handles the conversion to columnar format.

**Trade-off:** NDJSON is larger than Parquet or Avro for the same data, and read-back requires JSON parsing. For a landing zone read by one downstream writer, this is acceptable. The alternative (landing Parquet directly) would couple the CDC consumer to the Bronze schema, which changes the failure modes when the schema drifts.

---

## Bronze layer

### Table format: Apache Iceberg vs plain Parquet or Delta Lake

**Choice:** Apache Iceberg (PyIceberg 0.12.0).

**Why:** Iceberg provides snapshot isolation (BigQuery always reads a consistent table state), schema evolution, and the ability to point BigQuery at a specific metadata file without touching the data files. Plain Parquet gives none of these without custom metadata management. Delta Lake was considered but Databricks dependency management and cost were factors; Iceberg is vendor-neutral.

**Trade-off:** PyIceberg 0.12.0 has limitations: it cannot stream appends into a partitioned table (each batch is a new commit), it has no built-in orphan file removal, and it ignores declared sort orders on write. These required custom solutions (the two-pass writer, `bronze.orphans`, no sort order on write).

---

### Catalog: SQLite local file vs managed Iceberg catalog (BigLake)

**Choice:** SQLite local file, rebuilt from the GCS run marker after each write.

**Why:** BigLake (the GCP-managed Iceberg catalog) charges per Data Catalog Unit hour. For a laptop-scale project, that cost is unnecessary. The SQLite catalog is a cache of the state already recorded in the GCS run marker. If the catalog file is lost, `--rebuild-catalog` reconstructs it in seconds without reading the landing.

**Trade-off:** the catalog is a local file on the machine running the Bronze writer. In a multi-machine setup, this would not work without a shared catalog. For this project, the Airflow container bind-mounts the catalog path, which is recreated on each run from the GCS marker.

---

### Partition: month vs day

**Choice:** `month(event_date)` partitioning.

**Why:** day partitioning would produce up to 365 Parquet files per table per year (1,825 total), increasing Class A GCS costs and BigQuery metadata reads. Month partitioning produces 12 files per table per year (60 total). The measured Bronze write for the 100-merchant year produced 65 data files vs an estimated 857 for day partitioning. See [Cost and performance](cost-and-performance.md).

**Trade-off:** month partitioning allows dbt to prune by month, not day. For the query patterns in the mart layer (daily grain fact tables), this is acceptable. A query on a single day reads the whole month's partition.

---

## Semantic layer (dbt)

### Rolling GPV model: table vs BigQuery materialized view

**What went wrong first:** `int_payments__merchant_gpv_rolling` was first written as a BigQuery materialized view using `RANGE BETWEEN INTERVAL 29 DAY PRECEDING AND CURRENT ROW`. BigQuery materialized views do not support `RANGE BETWEEN` with offset boundaries.

**Choice:** changed to a table using `ORDER BY UNIX_DATE(txn_date) RANGE BETWEEN 29 PRECEDING AND CURRENT ROW`. `UNIX_DATE` returns integer days since the epoch, so 29 means exactly 29 calendar days preceding, preserving the intended window semantics.

**Trade-off:** a table is refreshed on each `dbt run` rather than maintained incrementally by BigQuery. For an 11-model mart rebuild that takes 65 seconds total, this is not a constraint.

---

### `merchants_scd`: dbt snapshot vs custom SQL model

**What went wrong first:** the first implementation used a dbt snapshot (strategy: check). BigQuery MERGE requires exactly one source row per unique key. The `merchants` Bronze table is a daily snapshot: up to 20,000 rows per day, 20,000+ merchants in total. The snapshot query needed deduplication to one row per merchant before the MERGE, and each run would detect only the most recent state transition, not the full history.

**Choice:** custom SQL model in `dbt/models/dimensions/merchants_scd.sql` using LAG window functions over all daily Bronze rows. Detects transitions in 9 merchant attribute columns and emits one SCD2 row per state-change event per merchant. Full historical transition history is built on the first run.

**Why:** LAG over all rows is a single BigQuery scan. The result (431 rows on the 100-merchant dataset, average 3.7 SCD2 records per merchant) correctly captures the full history of merchant attribute changes.

**Trade-off:** the LAG model re-reads all Bronze merchant rows on each `dbt run --full-refresh`. For the 100-merchant dataset (20,000 rows), this is instant. At 1,000 merchants (200,000 rows), it remains fast. The dbt snapshot approach would have been cheaper incrementally, but it would not have produced the correct history without the deduplication complexity.

---

## Airflow

### Executor: LocalExecutor vs CeleryExecutor

**Choice:** LocalExecutor (single-node, tasks run as subprocesses of the scheduler).

**Why:** CeleryExecutor requires a message broker (Redis or RabbitMQ) and worker nodes. For a three-task DAG running on a laptop, this is unnecessary complexity and cost. LocalExecutor handles the DAG with no additional services.

**Trade-off:** no task parallelism across workers. For this pipeline, the three tasks are strictly sequential, so parallelism is not needed.

---

### Credentials: bind-mounted files vs Airflow Connections

**Choice:** bind-mounted credentials files, referenced via environment variables passed to each BashOperator task.

**Why:** Airflow Connections store credentials in the Airflow metadata database. For GCP service account impersonation (which produces a refresh-token file, not a JSON key), the standard Airflow GCP connection type does not cover the use case cleanly. Bind-mounting the existing credential files keeps the credential management consistent with how the same credentials are used outside Airflow.

**Trade-off:** credentials are on the host filesystem and must be maintained there. The paths in `airflow/.env` must be absolute paths on the host. A moved credential file breaks the DAG without an Airflow UI error.

---

### Schedule: `None` (manual) vs cron

**Choice:** `schedule=None` by default. The DAG is paused at creation.

**Why:** in V1, new data is produced by running the simulator and CDC consumer manually. There is no continuous data source that would make a cron schedule meaningful. Setting the schedule to `None` prevents the DAG from running on stale data or attempting to run with an empty landing.

**Trade-off:** the user must remember to trigger the DAG after each CDC consumer run. To enable the cron schedule: set `schedule="0 6 * * *"` in `dags/fintech_pipeline.py` and unpause the DAG.

---

## Infrastructure

### Terraform for GCP resources

**Choice:** Terraform 1.17.1 with the Google provider, managing the two GCS buckets, three service accounts (`bronze-writer`, `cdc-consumer`, `lightdash-reader`), their IAM bindings, and the BigQuery dataset.

**Why:** infrastructure changes (new service account, new IAM binding) are code-reviewed and planned before applying. The first `terraform plan` confirmed that all existing resources could be imported with no changes required. CI validates formatting, provider resolution, and config structure on every push without credentials.

**Trade-off:** Terraform state must be maintained. Remote state lives in `gs://payments-512910-tfstate`. A `terraform apply` without the state bucket fails. The `infra/scratch.tf` file contains a G4 throwaway bucket stub (now destroyed) that shows as "to add" in a plain `terraform plan`. Always use `-target` for production resources.

See `docs/decisions/0001-terraform-for-the-gcp-infrastructure.md` for the full ADR.

---

## Tools tried and not used

**Redpanda:** listed in the original platform design as the streaming message broker for a future streaming feature path. Not built. Remains out of scope for V1. All CDC work uses the SQL-level Postgres slot functions instead.

**Lightdash self-hosted (Docker):** considered for cost reasons. The CLI version at the time (v3.121.1) had a missing dependency that blocked `lightdash download`. The newer CLI (3.167+) was only available from Lightdash Cloud. Cloud was chosen because it avoided running another Docker service and the CLI issues were resolved.

**BigLake managed Iceberg catalog:** considered as the catalog backend for PyIceberg. Not used because of per-DCU-hour costs. The SQLite local catalog with GCS-backed marker was used instead.
