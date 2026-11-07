# Architecture

The pipeline has six layers. Each one has a clear entry point, a clear output format, and a failure mode that stops it without corrupting the next layer. This document describes each component, how they connect, and what format data takes at each boundary.

---

## Overview

```
Postgres 16 (OLTP)
  |-- pgoutput WAL
CDC consumer (cdc/)
  |-- gzipped NDJSON --> GCS: gs://cdc_landing/cdc/<slot>/
Bronze writer (bronze/)
  |-- Parquet / Iceberg --> GCS: gs://fintech_bronze/
BigQuery (external tables)
  |-- CREATE OR REPLACE EXTERNAL TABLE
dbt (staging / intermediate / marts)
  |-- BigQuery native tables --> BigQuery: marts.*
Lightdash Cloud
  |-- queries BigQuery directly
```

Airflow (DAG `fintech_pipeline`) runs the last three steps in sequence: `bronze_write >> register_bq >> dbt_marts`. The generator and CDC consumer run independently before Airflow is triggered.

---

## Layer 1: Generator (`simulator/`)

The generator is a Python process that simulates a merchant payments and lending business. It writes five streams to a Postgres 16 database (`fintech_cdc`).

**Five streams:**
- `merchants`: daily snapshot per merchant (INSERT-only, composite PK `merchant_id + snapshot_at`)
- `merchant_transactions`: payment events (INSERT-only)
- `loan_applications`: one INSERT at decision, one UPDATE at disbursement
- `loan_repayments`: one row per collection attempt (INSERT-only)
- `risk_events`: one row per rule firing (INSERT-only)

The generator runs in two modes. Backfill advances one simulated day at a time and writes each day in one transaction, as fast as the machine allows. Stream replays the same day in time order, sleeping the gap between events divided by a speed factor (default 3,600 simulated seconds per real second).

State is deterministic given a seed. A checkpoint file holds every merchant, numpy's RNG state, and the run flags. A resumed run writes the same rows an uninterrupted one would, row for row.

**Why Postgres:** logical replication (`wal_level=logical`) from day one, with no restart needed when CDC was added. The five-table OLTP design models a real payments company at a simplified grain. See [Design decisions](design-decisions.md) for the choice of Postgres over a managed database.

**Config files** in `config/`: `archetypes.yaml` (11 merchant archetypes), `calibration.yaml` (lifecycle parameters, archetype mix), `causal_rules.yaml` (state transition probabilities, lending gates), `payments.yaml` (channel mix, fees), `lending.yaml` (term structure, cost parameters).

---

## Layer 2: CDC consumer (`cdc/`)

The CDC consumer reads Postgres WAL via the `pgoutput` logical replication plugin, using the SQL-level peek functions over an ordinary psycopg3 connection. No streaming replication protocol is required, which sidesteps pg_hba complications.

**What it produces:** gzipped NDJSON files, one per table per event date, written to `gs://cdc_landing/cdc/<slot>/`. Each line is one change event. NDJSON (newline-delimited JSON) is chosen here because it is append-safe and human-readable: each line is a complete, self-contained record. Gzip reduces file size by roughly 14x on this data.

**The change-event envelope** (`cdc/envelope.py`): every event carries the source table, commit LSN, the change's own LSN, commit timestamp, event timestamp, event date in SAST, position in the transaction, and a version field. `loan_applications` is the only updated table; its update event carries the full old row because `REPLICA IDENTITY FULL` is set (migration 003). The other four tables are insert-only.

**Guarantees:** at-least-once delivery. The slot is confirmed only after all objects in a batch have landed. Dedup downstream uses `(source.commit_lsn, transaction.position)`. A transaction is never split across two objects.

**The decoder** (`cdc/pgoutput.py`): turns binary pgoutput messages into typed Python values. Column types are mapped by `cdc/types.py`. An unmapped type, an unchanged TOAST marker, or any message the decoder does not understand raises an error that stops the consumer without confirming the batch.

**Schema drift guard** (`cdc/columns.py`): on every Relation message (the per-table column list pgoutput sends before each change), the consumer checks the column names, their order and each column's Postgres type against the Pydantic model in `simulator/models.py`. A schema change stops the consumer before any events from that table are landed.

**Service account:** the consumer runs as `cdc-consumer@payments-512910.iam.gserviceaccount.com`, which may create and read objects in `gs://cdc_landing` and nothing else. It cannot overwrite, delete or access the Bronze bucket. It refuses to start without `GOOGLE_APPLICATION_CREDENTIALS` set to its impersonated credentials file.

---

## Layer 3: Bronze writer (`bronze/`)

The Bronze writer converts the landed NDJSON events into Apache Iceberg tables on GCS.

**What Iceberg gives here:** snapshot isolation (a reader always sees a consistent table state), schema evolution, and the ability to point BigQuery at a specific metadata file without touching the data. See [Design decisions](design-decisions.md) for why Iceberg was chosen over plain Parquet.

**What Parquet gives here:** Parquet is the columnar file format inside the Iceberg tables. It compresses well and BigQuery reads it natively via the external Iceberg table API. The Iceberg layer adds the transaction log and schema contract on top.

**Two passes:** Pass 1 builds a streaming index of the landing: for each object, it records the event count, the first-copy object number per key, and a content hash. Pass 2 reads the same objects again, converts each event to an Arrow `RecordBatch` (PyArrow's in-memory columnar representation), and commits in batches of 100,000 rows. Arrow is used here because it matches Iceberg's columnar write path: building a full batch in Arrow before writing avoids per-row overhead and keeps peak memory bounded.

**Catalog:** PyIceberg uses a SQLite file as a local catalog. The catalog is rebuilt from the newest run marker in `gs://fintech_bronze/_runs/` after each write. If the catalog file is lost, `--rebuild-catalog` reconstructs it from the marker without reading any landed objects.

**Idempotency:** a second `--write` over the same landing writes nothing. A refused object creation (HTTP 412, already exists) causes a content comparison: equal means success, different stops the writer.

**Crash recovery:** three commit points inside a Parquet write are tested under injected failure (data files written, manifest written, manifest list written). After any crash, `bronze.orphans` finds and removes unused files. See `tests/test_bronze_crash.py`.

**Service account:** the writer runs as `bronze-writer@payments-512910.iam.gserviceaccount.com`, which has object read on `gs://cdc_landing` and full object access on `gs://fintech_bronze`.

---

## Layer 4: BigQuery registration (`scripts/register_bronze_bq.py`)

After each Bronze write, the script reads the newest run marker from `gs://fintech_bronze/_runs/`, extracts the Iceberg metadata file location for each table, and executes `CREATE OR REPLACE EXTERNAL TABLE ... WITH CONNECTION ... OPTIONS (format = 'ICEBERG', uris = [...])` for all five tables.

BigQuery external Iceberg tables are a fixed snapshot until re-pointed. Without this step, dbt reads the previous Bronze state.

The BigQuery Cloud Resource Connection (`fintech-bronze`) is a GCP construct that gives BigQuery a service account to read the GCS bucket. That service account holds `roles/storage.objectViewer` on `gs://fintech_bronze`.

---

## Layer 5: dbt (`dbt/`)

dbt transforms the Bronze external tables through three layers in BigQuery:

- **Staging** (`dbt/models/staging/`): views over the five Bronze source tables. Applies `WHERE op = 'c'` for insert-only tables, deduplicates `loan_applications` to the latest row per `application_id` by `(commit_lsn DESC, transaction_position DESC)`, drops envelope columns, and renames fields.
- **Intermediate** (`dbt/models/intermediate/`): materialized tables. Enriches transactions, computes daily GPV, builds rolling windows, generates loan daily snapshots and fee schedules, and derives merchant risk profiles.
- **Dimensions** (`dbt/models/dimensions/`): `merchants_scd` (SCD2 history via LAG window functions), `dim_merchants` (current state), `dim_date` (date spine with SA public holidays from the seed).
- **Marts** (`dbt/models/marts/`): 11 incremental fact tables, all targeting the `marts` dataset in BigQuery.

The dbt package `dbt-utils` (from dbt Hub) provides `date_spine` for `dim_date` and `generate_surrogate_key` for `merchants_scd`.

---

## Layer 6: Lightdash Cloud

Lightdash Cloud connects directly to BigQuery and reads the `marts` dataset. It uses a dedicated read-only service account (`lightdash-reader@payments-512910.iam.gserviceaccount.com`) with `roles/bigquery.jobUser` at the project level and `roles/bigquery.dataViewer` on the `marts` dataset only.

Dashboard content is version-controlled under `lightdash/` as the content-as-code source of truth, committed after each `lightdash download`.

Eight dashboards: GPV Overview, Portfolio Quality, Settlement Reconciliation, Revenue, Merchant Health, Merchant Monthly Statement, Lending Operations, Risk and Fraud.

---

## Airflow (`airflow/`, `dags/`)

Airflow 2.9.3 runs on Docker Compose with LocalExecutor. Three services: a Postgres container for Airflow metadata, the webserver, and the scheduler.

The DAG `fintech_pipeline` has three tasks in sequence:

```
bronze_write >> register_bq >> dbt_marts
```

Each task is a `BashOperator` that runs inside an isolated Python virtualenv (`/opt/pipeline-venv`) to avoid a SQLAlchemy version conflict between Airflow (1.4.x) and PyIceberg (requires 2.x). Credential files are bind-mounted into the container at paths set in `airflow/.env`.

The DAG is paused at creation and manual-trigger only. To run on a cron schedule, set `schedule="0 6 * * *"` in `dags/fintech_pipeline.py` and unpause from the UI.

---

## Infrastructure as code (`infra/`)

Terraform 1.17.1 manages the GCP resources: the two GCS buckets, three service accounts (`bronze-writer`, `cdc-consumer`, `lightdash-reader`), their IAM bindings, and the BigQuery dataset. Remote state lives in `gs://payments-512910-tfstate`.

CI runs `terraform fmt -check`, `terraform init -backend=false -lockfile=readonly`, and `terraform validate` on every push to `main`, without credentials and without touching GCP.

---

## CI (`github/workflows/ci.yml`)

Three jobs run in parallel on every push to `main`:

- **pytest**: starts Postgres 16 with `wal_level=logical` via `docker run`, applies migrations, installs all dependencies, and runs the full test suite including slow tests.
- **dbt**: installs dbt-bigquery 1.9.2 and runs `dbt deps` with a CI profile (no BigQuery connection needed for this check).
- **terraform**: validates formatting, provider resolution from the lock file, and config structure.

---

*For schemas, table definitions and lineage, see [Data model](data-model.md). For the DAG design and failure handling, see [Orchestration](orchestration.md).*
