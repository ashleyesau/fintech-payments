# Orchestration

The pipeline uses Apache Airflow 2.9.3 on Docker Compose with a LocalExecutor. This document covers the DAG design, scheduling, failure handling, and what happens at each task boundary.

---

## Overview

The DAG `fintech_pipeline` has three tasks:

```
bronze_write  >>  register_bq  >>  dbt_marts
```

Each task is a `BashOperator`. Each depends on the previous one succeeding. If a task fails, the DAG run stops and downstream tasks do not execute.

The generator and CDC consumer are not Airflow tasks. They run separately to load new data before the DAG is triggered. The DAG assumes the GCS landing bucket already has data when it runs.

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

`schedule=None` means the DAG only runs when triggered manually. To enable a cron schedule, set `schedule="0 6 * * *"` (06:00 UTC, 08:00 SAST) in `dags/fintech_pipeline.py` and unpause from the UI:

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

1. **Delete the local catalog file.** The Airflow container filesystem is ephemeral. If a catalog from a previous run exists, `--rebuild-catalog` refuses to overwrite it. Deleting it first is safe because the catalog is a cache, not the source of truth.
2. **Rebuild the catalog** from the newest run marker in GCS. The `|| true` means a fresh Bronze bucket with no marker does not fail the task.
3. **Write.** Read the landing, append new events to the Iceberg tables, write the run marker and catalog copy.

Runs with `GOOGLE_APPLICATION_CREDENTIALS` set to the bind-mounted `bronze_writer.json` file.

**What can go wrong:**

| Failure | Behaviour |
|---|---|
| No new objects in the landing since the last run | Writes nothing. Exits 0. |
| Missing or inaccessible credentials file | Exits 3 before any GCS operation. |
| Schema drift in the landing | Stops before any rows are appended. |

---

## Task 2: register_bq

```bash
python scripts/register_bronze_bq.py \
  --project payments-512910 \
  --location us-east1 \
  --connection fintech-bronze
```

Reads the newest run marker from `gs://fintech_bronze/_runs/`. Extracts the Iceberg metadata file path for each of the five tables. Executes `CREATE OR REPLACE EXTERNAL TABLE` for each in BigQuery.

This step is required after every Bronze write. BigQuery external Iceberg tables point at a specific metadata file. They are not updated automatically when the Iceberg table changes. Without this step, dbt reads the previous Bronze snapshot.

Runs with the bind-mounted `adc.json` (Application Default Credentials), used for BigQuery operations that `bronze-writer` cannot perform.

**What can go wrong:** a BigQuery metadata API failure on one table leaves the others registered and that table pointing at the old snapshot. The task exits non-zero and `dbt_marts` does not run.

---

## Task 3: dbt_marts

```bash
cd /opt/repo/dbt && dbt deps && dbt run --full-refresh
```

Installs dbt packages (`dbt_utils` from dbt Hub) then rebuilds all models with `--full-refresh`. This forces every incremental model to rebuild from scratch rather than appending to existing rows.

`--full-refresh` is used because the Bronze source is append-only. Without it, an incremental run after a Bronze reset would add new rows on top of rows from the previous dataset.

Runs with the `adc.json` credentials.

**What can go wrong:** a dbt model compilation error or a BigQuery query failure stops the run. `dbt deps` failure (network issue reaching dbt Hub) would prevent packages from resolving, though they were already installed in the container image.

---

## Virtualenv isolation

Airflow 2.9.3 uses SQLAlchemy 1.4.x. PyIceberg 0.12.0 requires SQLAlchemy 2.x. These two cannot coexist in the same Python environment.

The solution: an isolated virtualenv at `/opt/pipeline-venv`, built during the Docker image build from `airflow/requirements-pipeline.txt`. It contains PyIceberg, PyArrow, and dbt-bigquery, independent of Airflow's base packages. All three tasks call `${VENV}/python` and `${VENV}/dbt` directly, where `VENV=/opt/pipeline-venv/bin`.

---

## Credentials and bind mounts

Three paths are bind-mounted from the host into every container:

| `airflow/.env` variable | Mounted at | Used by |
|---|---|---|
| `BRONZE_WRITER_CREDS_PATH` | `/opt/credentials/bronze_writer.json` | `bronze_write` task |
| `ADC_CREDS_PATH` | `/opt/credentials/adc.json` | `register_bq` and `dbt_marts` tasks |
| `DBT_PROFILES_DIR` | `/home/airflow/.dbt/` | `dbt_marts` task |

`airflow/.env` is gitignored. Copy `airflow/.env.example` and fill in the three absolute host paths before starting the stack.

---

## Starting Airflow

First-time setup (run once):

```bash
cp airflow/.env.example airflow/.env
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

A failed task stops the DAG run. The landing data, Bronze tables, and BigQuery registrations are left in whatever state they were in when the task failed.

Each step is idempotent. A re-trigger after the problem is fixed will:

| Task | Behaviour on re-trigger |
|---|---|
| `bronze_write` | Skips events already written (content-hash idempotency). Writes any new ones. |
| `register_bq` | Re-points all five external tables to the current Bronze state. |
| `dbt_marts` | Rebuilds all 11 mart tables with `--full-refresh`. |

**Lightdash** requires no Airflow step. It queries BigQuery directly and picks up new mart data on the next dashboard load.

**Slot management** is outside the DAG. `CDC_SLOT_NAME` in `airflow/.env` tells the Bronze writer which landing prefix to read. After producing new data with a new slot (e.g., `cdc_r04`), update `CDC_SLOT_NAME` and restart the scheduler before triggering.

---

## Monitoring

```bash
docker compose -f airflow/docker-compose.yml logs -f scheduler
```

```bash
python -m cdc.lag
```

```bash
python scripts/storage_request_counts.py
```

---

*For the DAG's place in the full pipeline, see [Architecture](architecture.md). For the costs of each step, see [Cost and performance](cost-and-performance.md).*
