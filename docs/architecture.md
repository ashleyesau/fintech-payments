# Architecture

The pipeline has six layers. Each has a clear entry point, a clear output format, and a failure mode that stops it without corrupting the next layer.

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

Airflow (DAG `fintech_pipeline`) runs the last five steps in sequence: `bronze_write >> bronze_verify >> register_bq >> dbt_marts >> dbt_test`. The generator and CDC consumer run independently before Airflow is triggered.

---

## Layer 1: Generator (`simulator/`)

A Python process that simulates a merchant payments and lending business. It writes five streams to a Postgres 16 database (`fintech_cdc`).

**Five streams:**
- `merchants`: daily snapshot per merchant (INSERT-only, composite PK `merchant_id + snapshot_at`)
- `merchant_transactions`: payment events (INSERT-only)
- `loan_applications`: one INSERT at decision, one UPDATE at disbursement
- `loan_repayments`: one row per collection attempt (INSERT-only)
- `risk_events`: one row per rule firing (INSERT-only)

**Two modes.** Backfill advances one simulated day at a time and writes each day in one transaction, as fast as the machine allows. Stream replays the same day in time order, sleeping the gap between events divided by a speed factor (default 3,600 simulated seconds per real second).

**Determinism.** State is fully deterministic given a seed. A checkpoint file holds every merchant, numpy's RNG state, and the run flags. A resumed run writes the same rows an uninterrupted one would.

**Why Postgres.** `wal_level=logical` was set from day one at no meaningful cost. No restart was needed when CDC was added. See [Design decisions](design-decisions.md) for the full reasoning.

**Config files** in `config/`:

| File | Contents |
|---|---|
| `archetypes.yaml` | 11 merchant archetypes |
| `calibration.yaml` | Lifecycle parameters, archetype mix |
| `causal_rules.yaml` | State transition probabilities, lending gates |
| `payments.yaml` | Channel mix, fees |
| `lending.yaml` | Term structure, cost parameters |

---

## Layer 2: CDC consumer (`cdc/`)

Reads Postgres WAL via the `pgoutput` logical replication plugin. Uses the SQL-level peek functions over an ordinary psycopg3 connection. No streaming replication protocol required.

**Output.** Gzipped NDJSON files, written to `gs://cdc_landing/cdc/<slot>/`. One file per table per event date. Each line is one self-contained change event. Gzip reduces file size by roughly 14x on this data.

**The change-event envelope** (`cdc/envelope.py`). Every event carries:
- Source table, commit LSN, change LSN, commit timestamp
- Event timestamp and event date in SAST
- Position within the transaction and transaction size
- Envelope version

`loan_applications` is the only updated table. Its update events carry the full old row because `REPLICA IDENTITY FULL` is set (migration 003). The other four tables are insert-only.

**Delivery guarantee.** At-least-once. The slot is confirmed only after all objects in a batch have landed. A transaction is never split across two objects. Dedup downstream uses `(source.commit_lsn, transaction.position)`.

**The decoder** (`cdc/pgoutput.py`). Turns binary pgoutput messages into typed Python values. Column types are mapped by `cdc/types.py`. An unmapped type, an unchanged TOAST marker, or any unrecognised message raises an error that stops the consumer before confirming the batch.

**Schema drift guard** (`cdc/columns.py`). On every Relation message, the consumer checks column names, order and Postgres types against the Pydantic model in `simulator/models.py`. A schema change stops the consumer before any events land.

**Service account.** The consumer runs as `cdc-consumer@payments-512910.iam.gserviceaccount.com`. It may create and read objects in `gs://cdc_landing` only. It refuses to start without `GOOGLE_APPLICATION_CREDENTIALS` set.

---

## Layer 3: Bronze writer (`bronze/`)

Converts the landed NDJSON events into Apache Iceberg tables on GCS.

**Why Iceberg.** Snapshot isolation (a reader always sees a consistent table state), schema evolution, and the ability to point BigQuery at a specific metadata file without touching data files. See [Design decisions](design-decisions.md).

**Why Parquet.** Parquet is the columnar file format inside the Iceberg tables. It compresses well and BigQuery reads it natively via the external Iceberg table API.

**Two passes.**
- Pass 1: builds a streaming index of the landing. Records the event count, first-copy object number per key, and a content hash for each object.
- Pass 2: reads the same objects again. Converts each event to an Arrow `RecordBatch` and commits in batches of 100,000 rows. Arrow matches Iceberg's columnar write path and keeps peak memory bounded.

**Catalog.** PyIceberg uses a SQLite file as a local catalog. After each write, the catalog is rebuilt from the newest run marker in `gs://fintech_bronze/_runs/`. If the catalog file is lost, `--rebuild-catalog` reconstructs it without reading any landed objects.

**Idempotency.** A second `--write` over the same landing writes nothing. A refused object creation (HTTP 412) triggers a content comparison. Equal means success. Different stops the writer.

**Crash recovery.** Three commit points inside a Parquet write are tested under injected failure: data files written, manifest written, manifest list written. After any crash, `bronze.orphans` finds and removes unused files. See `tests/test_bronze_crash.py`.

**Service account.** Runs as `bronze-writer@payments-512910.iam.gserviceaccount.com`. Object read on `gs://cdc_landing`, full object access on `gs://fintech_bronze`.

---

## Layer 4: BigQuery registration (`scripts/register_bronze_bq.py`)

After each Bronze write, the script reads the newest run marker from `gs://fintech_bronze/_runs/`. It extracts the Iceberg metadata file location for each table and executes `CREATE OR REPLACE EXTERNAL TABLE ... WITH CONNECTION ... OPTIONS (format = 'ICEBERG', uris = [...])` for all five tables.

BigQuery external Iceberg tables point at a fixed snapshot. They are not updated automatically when the Iceberg table changes. Without this step, dbt reads the previous Bronze state.

The BigQuery Cloud Resource Connection (`fintech-bronze`) gives BigQuery a service account to read the GCS bucket. That service account holds `roles/storage.objectViewer` on `gs://fintech_bronze`.

---

## Layer 5: dbt (`dbt/`)

Transforms the Bronze external tables through four layers in BigQuery:

| Layer | Path | What it does |
|---|---|---|
| Staging | `dbt/models/staging/` | Views over Bronze. Applies `WHERE op = 'c'`, deduplicates `loan_applications`, drops envelope columns. |
| Intermediate | `dbt/models/intermediate/` | Materialized tables. Enriches transactions, computes GPV, builds rolling windows, generates loan snapshots. |
| Dimensions | `dbt/models/dimensions/` | `merchants_scd` (SCD2), `dim_merchants` (current state), `dim_date` (date spine with SA holidays). |
| Marts | `dbt/models/marts/` | 11 incremental fact tables in the `marts` BigQuery dataset. |

The dbt package `dbt-utils` provides `date_spine` for `dim_date` and `generate_surrogate_key` for `merchants_scd`.

---

## Layer 6: Lightdash Cloud

Connects directly to BigQuery and reads the `marts` dataset. Uses a dedicated read-only service account (`lightdash-reader@payments-512910.iam.gserviceaccount.com`) with `roles/bigquery.jobUser` at the project level and `roles/bigquery.dataViewer` on `marts` only.

Dashboard content lives under `lightdash/` as the content-as-code source of truth, committed after each `lightdash download`.

Eight dashboards: GPV Overview, Portfolio Quality, Settlement Reconciliation, Revenue, Merchant Health, Merchant Monthly Statement, Lending Operations, Risk and Fraud.

---

## Airflow (`airflow/`, `dags/`)

Airflow 2.9.3 on Docker Compose with LocalExecutor. Three services: a Postgres container for Airflow metadata, the webserver, and the scheduler.

The DAG `fintech_pipeline` runs five tasks in sequence:

```
bronze_write >> bronze_verify >> register_bq >> dbt_marts >> dbt_test
```

Each task is a `BashOperator` running inside an isolated Python virtualenv (`/opt/pipeline-venv`). This isolates the pipeline from a SQLAlchemy version conflict between Airflow (1.4.x) and PyIceberg (requires 2.x). Credential files are bind-mounted into the container at paths set in `airflow/.env`.

The DAG is paused at creation and manual-trigger only.

---

## Infrastructure as code (`infra/`)

Terraform 1.17.1 manages:
- Two GCS buckets
- Three service accounts (`bronze-writer`, `cdc-consumer`, `lightdash-reader`)
- Their IAM bindings
- The BigQuery datasets

Remote state lives in `gs://payments-512910-tfstate`.

CI runs `terraform fmt -check`, `terraform init -backend=false -lockfile=readonly`, and `terraform validate` on every push to `main`, without credentials.

---

## CI (`.github/workflows/ci.yml`)

Three jobs run in parallel on every push to `main`:

| Job | What it does |
|---|---|
| **pytest** | Starts Postgres 16 with `wal_level=logical`, applies migrations, runs the full test suite including slow tests. |
| **dbt** | Installs dbt-bigquery 1.9.2, runs `dbt deps` with a CI profile. No BigQuery connection needed. |
| **terraform** | Validates formatting, provider resolution from the lock file, and config structure. |

---

*For schemas, table definitions and lineage, see [Data model](data-model.md). For the DAG design and failure handling, see [Orchestration](orchestration.md).*
