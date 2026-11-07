# Orchestration

The pipeline uses Apache Airflow 2.9.3 running on Docker Compose with a LocalExecutor. This document covers the DAG design, the dependency chain, scheduling, failure handling, and what happens at each task boundary.

---

## Overview

The DAG `fintech_pipeline` contains three tasks:

```
bronze_write  >>  register_bq  >>  dbt_marts
```

Each task is a `BashOperator`. Each depends on the previous one succeeding. If any task fails, the DAG run stops and downstream tasks do not execute.

The generator and CDC consumer are not Airflow tasks. They run separately to load new data before the DAG is triggered. The DAG assumes data is already in the GCS landing bucket when it runs.

---

## DAG configuration

```python
DAG(
    dag_id="fintech_pipeline",
    schedule=None,          # manual trigger only by default
    start_date=datetime(2026, 11, 6, tzinfo=timezone.utc),
    catchup=False,
    tags=["fintech", "bronze", "dbt"],
)
```

`schedule=None` means the DAG only runs when triggered manually. To run on a cron schedule, set `schedule="0 6 * * *"` (06:00 UTC, 08:00 SAST) in `dags/fintech_pipeline.py` and unpause from the Airflow UI or:

```bash
docker compose -f airflow/docker-compose.yml exec scheduler airflow dags unpause fintech_pipeline
```

`catchup=False` means Airflow will not try to run past missed intervals if the schedule is re-enabled.

---

## Task 1: bronze_write

```bash
rm -f /home/airflow/.config/fintech_bronze/catalog.db \
  && (python -m bronze.main --rebuild-catalog || true) \
  && python -m bronze.main --write
```

Three steps in one shell command:

1. Delete the local catalog file. The Airflow container filesystem is ephemeral. If the catalog from a previous run exists, `--rebuild-catalog` refuses to overwrite it. Deleting it first is safe because the catalog is a cache, not the source of truth. The source of truth is the run marker in GCS.
2. Rebuild the catalog from the newest run marker (`--rebuild-catalog`). The `|| true` means a fresh Bronze bucket with no marker does not fail the task.
3. Write: read the landing, append new events to the Iceberg tables, write the run marker and catalog copy.

Runs with `GOOGLE_APPLICATION_CREDENTIALS` set to the bind-mounted `bronze_writer.json` file, so the task runs as the `bronze-writer` service account.

**What can go wrong:** a landing bucket with no new objects since the last run writes nothing and exits 0. A missing or inaccessible credentials file causes an exit 3 before any GCS operation. A schema drift in the landing (column added or renamed) stops the write before any rows are appended.

**On the 1,000-merchant run:** this task read 2,610 objects from `gs://cdc_landing/cdc/cdc_r03/`, wrote 8.5M events to five Iceberg tables from scratch, and took 124 minutes.

---

## Task 2: register_bq

```bash
python scripts/register_bronze_bq.py \
  --project payments-512910 \
  --location us-east1 \
  --connection fintech-bronze
```

Reads the newest run marker from `gs://fintech_bronze/_runs/`, extracts the Iceberg metadata file path for each of the five tables, and executes `CREATE OR REPLACE EXTERNAL TABLE` for each in BigQuery.

This step is required after every Bronze write. Without it, dbt in the next task reads the previous Bronze snapshot. BigQuery external Iceberg tables point at a specific metadata file; they are not automatically updated when the Iceberg table changes.

Runs with `GOOGLE_APPLICATION_CREDENTIALS` set to the bind-mounted `adc.json` file (Application Default Credentials for the workspace owner, used for BigQuery operations that `bronze-writer` cannot perform).

**What can go wrong:** a BigQuery metadata API failure on one table leaves the others registered and that table pointing at the old snapshot. The task exits non-zero and `dbt_marts` does not run.

**On the 1,000-merchant run:** this task took 23 seconds.

---

## Task 3: dbt_marts

```bash
cd /opt/repo/dbt && dbt deps && dbt run --full-refresh
```

Installs dbt packages (`dbt_utils` from dbt Hub) and runs all 11 mart models with `--full-refresh`, which forces incremental models to rebuild from scratch rather than appending to existing rows.

`--full-refresh` is used here because the Bronze source is append-only but the mart tables need to reflect the complete current state. Without it, an incremental run after a Bronze reset would add new rows on top of rows from the previous dataset.

Runs with the `adc.json` credentials, which give BigQuery access to run queries and write to the `marts` dataset.

**What can go wrong:** a dbt model compilation error or a BigQuery query failure stops the run. `dbt deps` failure (network issue reaching dbt Hub) would prevent packages from resolving, though the packages were already installed in the container image.

**On the 1,000-merchant run:** this task rebuilt all 11 mart tables in 65 seconds.

---

## Virtualenv isolation

Airflow 2.9.3 uses SQLAlchemy 1.4.x. PyIceberg 0.12.0 requires SQLAlchemy 2.x. These two cannot coexist in the same Python environment.

The `airflow/Dockerfile` builds an isolated virtualenv at `/opt/pipeline-venv` that contains the pipeline dependencies from `airflow/requirements-pipeline.txt` (PyIceberg, PyArrow, dbt-bigquery), independent of the Airflow base image's packages. All three BashOperator tasks use `${VENV}/python` and `${VENV}/dbt`, where `VENV=/opt/pipeline-venv/bin`, to run from that isolated environment.

---

## Credentials and bind mounts

The Airflow Docker Compose stack bind-mounts three paths from the host into every container:

| `airflow/.env` variable | Mounted at | Used by |
|---|---|---|
| `BRONZE_WRITER_CREDS_PATH` | `/opt/credentials/bronze_writer.json` | `bronze_write` task |
| `ADC_CREDS_PATH` | `/opt/credentials/adc.json` | `register_bq` and `dbt_marts` tasks |
| `DBT_PROFILES_DIR` | `/home/airflow/.dbt/` | `dbt_marts` task (`profiles.yml`) |

The `airflow/.env` file is gitignored. Copy `airflow/.env.example` and fill in the three absolute paths on the host before starting the stack.

---

## Starting Airflow

First-time setup (run once):

```bash
cp airflow/.env.example airflow/.env
# Edit airflow/.env: fill in BRONZE_WRITER_CREDS_PATH, ADC_CREDS_PATH, DBT_PROFILES_DIR
docker compose -f airflow/docker-compose.yml up airflow-init
```

Start the stack:

```bash
docker compose -f airflow/docker-compose.yml up -d webserver scheduler
```

UI: `http://localhost:8080` (username: `admin`, password: `admin`).

Trigger a run manually:

```bash
docker compose -f airflow/docker-compose.yml exec scheduler airflow dags trigger fintech_pipeline
```

---

## Failure handling

**A failed task stops the DAG run.** Downstream tasks do not execute. The landing data, Bronze tables, and BigQuery registrations are left in whatever state they were in when the task failed. Because each step is idempotent, a re-trigger of the DAG after the problem is fixed will:
- `bronze_write`: skip events already written (content-hash idempotency), write any new ones.
- `register_bq`: re-point all five external tables to the current Bronze state.
- `dbt_marts`: rebuild all 11 mart tables with `--full-refresh`.

**Lightdash** requires no Airflow step. It queries BigQuery directly and picks up the new mart data on the next dashboard load.

**Slot management** is outside the DAG. `CDC_SLOT_NAME` in `airflow/.env` tells the Bronze writer which landing prefix to read from. After generating new data with a new slot (e.g., `cdc_r04`), update `CDC_SLOT_NAME` in `airflow/.env` and restart the scheduler before triggering.

---

## Monitoring

Check task logs:

```bash
docker compose -f airflow/docker-compose.yml logs -f scheduler
```

Check the replication slot lag before running a large CDC consumer session:

```bash
python -m cdc.lag
```

Check how many GCS Class A and B operations have been used in the current billing month:

```bash
python scripts/storage_request_counts.py
```

---

*For the DAG's place in the full pipeline, see [Architecture](architecture.md). For the costs of each step, see [Cost and performance](cost-and-performance.md).*
