# Orchestration

The pipeline uses Apache Airflow 2.9.3 on Docker Compose with a LocalExecutor. This document covers the DAG design, scheduling, failure handling, and what happens at each task boundary.

---

## Overview

The DAG `fintech_pipeline` has five tasks:

```
bronze_write >> bronze_verify >> register_bq >> dbt_marts >> dbt_test
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

## Task 2: bronze_verify

```bash
bash scripts/verify_unless_unchanged.sh   # runs python -m bronze.check unless the landing is unchanged
```

Reads every landed object, validates every event against the change-event contract, and proves each one maps correctly to its Bronze row. Writes nothing. Never reads the Bronze tables, so it stays within container memory limits at any dataset size.

`bronze.verify` (run manually outside the DAG) performs the full digest comparison between Bronze and the landing. That is not done here because PyIceberg loads the full Bronze tables into memory: at 8.3M rows in `merchant_transactions` alone, the process is killed by the OS before it finishes inside a container.

Runs with `bronze_writer.json` credentials.

**When it is skipped.** The check takes about an hour at 8.5M events (56 min 36 s on 12 November 2026) and reads every landed object, so it is skipped when the landing has not changed since a run marker that already passed it. The script reads the newest run marker (`python -m bronze.check --newest-marker`: one listing and one read, no landed object) and compares its id with the Airflow Variable `bronze_last_verified_marker`. If they are equal the task exits 99 and Airflow shows it as skipped. Otherwise it runs the check, and when the check passes it records the newest marker's id in the Variable. `bronze_write` runs first, so anything new in the landing has already produced a newer marker by the time this task starts. The skip trusts that landed objects are never overwritten, as the `bronze_write` skip does. If the Variable is lost or empty, the check simply runs.

`register_bq` has `trigger_rule="none_failed"`, so it runs after a skipped `bronze_verify` but still stops if `bronze_verify` fails.

**Forcing a check.** Trigger the DAG with the `force_verify` param set to true (`--conf '{"force_verify": true}'`, or the checkbox in the trigger form) to check every landed object whatever is recorded. To record a marker as verified by hand, for example after a manual `python -m bronze.check` passed: `airflow variables set bronze_last_verified_marker <run id>`.

**What can go wrong:** a contract violation in a landing event (bad shape, unmappable value) exits 2 and stops the pipeline. A bucket failure exits 3.

---

## Task 3: register_bq

```bash
python scripts/register_bronze_bq.py \
  --project payments-512910 \
  --location us-east1 \
  --connection fintech-bronze
```

Reads the newest run marker from `gs://fintech_bronze/_runs/`. Extracts the Iceberg metadata file path for each of the five tables. Executes `CREATE OR REPLACE EXTERNAL TABLE` for each in BigQuery.

This step is required after every Bronze write. BigQuery external Iceberg tables point at a specific metadata file. They are not updated automatically when the Iceberg table changes. Without this step, dbt reads the previous Bronze snapshot.

Runs with the bind-mounted `pipeline_runner.json` credentials.

**What can go wrong:** a BigQuery metadata API failure on one table leaves the others registered and that table pointing at the old snapshot. The task exits non-zero and `dbt_marts` does not run.

---

## Task 4: dbt_marts

```bash
cd /opt/repo/dbt && dbt deps && dbt run --full-refresh
```

Installs dbt packages (`dbt_utils` from dbt Hub) then rebuilds all models with `--full-refresh`. This forces every incremental model to rebuild from scratch rather than appending to existing rows.

`--full-refresh` is used because the Bronze source is append-only. Without it, an incremental run after a Bronze reset would add new rows on top of rows from the previous dataset.

Runs with `pipeline_runner.json` credentials.

**What can go wrong:** a dbt model compilation error or a BigQuery query failure stops the run. `dbt deps` failure (network issue reaching dbt Hub) would prevent packages from resolving, though they were already installed in the container image.

---

## Task 5: dbt_test

```bash
cd /opt/repo/dbt && dbt test
```

Runs all dbt schema tests across every layer: 42 staging tests, 42 intermediate tests, 27 dimension tests and 54 mart tests. A model that builds without error but returns bad data fails here.

**Limitation:** `dbt_test` runs after `dbt_marts`, which means the mart tables are already built and Lightdash can read them before the tests confirm they are correct. A stricter gate would split the run: test staging, build intermediate, test intermediate, build marts, test marts. The current order catches failures before the next DAG run, not before the current one's output reaches dashboards.

Runs with `pipeline_runner.json` credentials.

**What can go wrong:** any failing assertion exits non-zero and stops the DAG run. Check the Airflow task logs for the specific test name and table.

---

## Virtualenv isolation

Airflow 2.9.3 uses SQLAlchemy 1.4.x. PyIceberg 0.12.0 requires SQLAlchemy 2.x. These two cannot coexist in the same Python environment.

The solution: an isolated virtualenv at `/opt/pipeline-venv`, built during the Docker image build from `airflow/requirements-pipeline.txt`. It contains PyIceberg, PyArrow, and dbt-bigquery, independent of Airflow's base packages. All five tasks call `${VENV}/python` and `${VENV}/dbt` directly, where `VENV=/opt/pipeline-venv/bin`.

---

## Credentials and bind mounts

Three paths are bind-mounted from the host into every container:

| `airflow/.env` variable | Mounted at | Used by |
|---|---|---|
| `BRONZE_WRITER_CREDS_PATH` | `/opt/credentials/bronze_writer.json` | `bronze_write` and `bronze_verify` tasks |
| `PIPELINE_CREDS_PATH` | `/opt/credentials/pipeline_runner.json` | `register_bq`, `dbt_marts` and `dbt_test` tasks |
| `DBT_PROFILES_DIR` | `/home/airflow/.dbt/` | `dbt_marts` and `dbt_test` tasks |

`airflow/.env` is gitignored. Copy `airflow/.env.example` and fill in the three absolute host paths before starting the stack.

**Auth note.** Both credential files use `gcloud auth application-default login --impersonate-service-account`, which stores a refresh token for your personal login that is then used to impersonate the service account. This is least-privilege but it is not unattended: the underlying personal login session must remain valid. For fully unattended operation, replace the impersonated credentials with a service account key file or Workload Identity Federation.

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
| `bronze_verify` | Re-reads the landing and re-checks, unless the newest run marker already passed (then skipped; use `force_verify` to check again). |
| `register_bq` | Re-points all five external tables to the current Bronze state. |
| `dbt_marts` | Rebuilds all 11 mart tables with `--full-refresh`. |
| `dbt_test` | Re-runs all schema tests. |

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
