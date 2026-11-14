# Orchestration

The pipeline uses Apache Airflow 2.9.3 on Docker Compose with a LocalExecutor. This document covers the DAG design, scheduling, failure handling, and what happens at each task boundary.

---

## Overview

The DAG `fintech_pipeline` has eight tasks:

```
bronze_write >> bronze_verify >> register_bq >> check_bq_schema
    >> dbt_build_staging >> dbt_build_dimensions >> dbt_build_intermediate >> dbt_build_marts
```

Each task is a `BashOperator`. Each depends on the previous one succeeding, with one exception: `register_bq` also runs when `bronze_verify` is skipped. If a task fails, the DAG run stops and downstream tasks do not execute.

Tasks retry twice with a five-minute delay (`default_args`), except `bronze_write`, which has `retries=0`.

Each `dbt_build_*` task runs `dbt build`, which runs the layer's models and its tests together, so a failing test stops every later layer. Tests tagged `periodic` are excluded and run only by hand (`dbt test -s tag:periodic`).

The generator and CDC consumer are not Airflow tasks. They run separately to load new data before the DAG is triggered. The DAG assumes the GCS landing bucket already has data when it runs.

---

## DAG configuration

```python
DAG(
    dag_id="fintech_pipeline",
    schedule=None,
    start_date=datetime(2026, 11, 6, tzinfo=timezone.utc),
    catchup=False,
    tags=["fintech", "bronze", "dbt"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
)
```

`schedule=None` means the DAG only runs when triggered manually. Do not enable a schedule until the catalog rebuild failures in the `bronze_write` section are understood or an alerting and retry policy is decided. To enable a cron schedule, set `schedule="0 6 * * *"` (06:00 UTC, 08:00 SAST) in `dags/fintech_pipeline.py` and unpause from the UI:

```bash
docker compose -f airflow/docker-compose.yml exec scheduler airflow dags unpause fintech_pipeline
```

`catchup=False` means Airflow will not try to run past missed intervals if the schedule is re-enabled.

**Params.** Two boolean params are set in the trigger form or with `--conf`:

| Param | Default | Effect |
|---|---|---|
| `full_refresh` | false | Passes `--full-refresh` to every dbt layer. For controlled rebuilds only. |
| `force_verify` | false | Makes `bronze_verify` check every landed object even if the landing is unchanged since a verified run marker. |

---

## Task 1: bronze_write

```bash
rm -f /home/airflow/.config/fintech_bronze/catalog.db \
  && (python -m bronze.main --rebuild-catalog || true) \
  && python -m bronze.main --write --verified-marker-file /tmp/bronze_verified_marker \
  && airflow variables set bronze_last_verified_marker "$(cat /tmp/bronze_verified_marker)"
```

Four steps in one shell command (the DAG's version also prints the time, the catalog folder and the running processes if the rebuild fails, and skips the last step when no verified marker was recorded):

1. **Delete the local catalog file.** The Airflow container filesystem is ephemeral. If a catalog from a previous run exists, `--rebuild-catalog` refuses to overwrite it. Deleting it first is safe because the catalog is a cache, not the source of truth.
2. **Rebuild the catalog** from the newest run marker in GCS. The rebuild registers the five tables from the marker and includes a row-count scan over the Bronze data files (5 to 7 minutes, and Class B reads against `fintech_bronze`; the Class B cost of one run has not been measured). The `|| true` means a fresh Bronze bucket with no marker does not fail the task. If the rebuild fails, the command prints the time, the catalog folder with timestamps and the running processes, so the cause can be investigated.
3. **Write.** If nothing new has landed since the newest run marker, the write reads no landed object and finishes in seconds (32 s measured on 12 November 2026). Otherwise it reads the landing, appends new events to the Iceberg tables, and writes the run marker and catalog copy.
4. **Record the marker as verified**, only when the write's first pass read every landed object (not when the write skipped its index). `bronze_verify` then skips, because that pass and `bronze.check` are the same index build. This step has unit tests only and has not yet been run against a real new-data run.

Runs with `GOOGLE_APPLICATION_CREDENTIALS` set to the bind-mounted `bronze_writer.json` file. `retries=0`, because the task appends; re-trigger by hand after a failure.

**What can go wrong:**

| Failure | Behaviour |
|---|---|
| No new objects in the landing since the last run | Writes nothing. Exits 0. |
| Missing or inaccessible credentials file | Exits 3 before any GCS operation. |
| Schema drift in the landing | Stops before any rows are appended. |
| Catalog rebuild fails | The `|| true` hides it. The write then refuses to run against an incomplete catalog, prints "Nothing was written" and exits 2. The data is not at risk. Re-trigger. |

**Known issue: intermittent catalog rebuild failures.** The rebuild has failed intermittently with three different SQLite errors: `table iceberg_tables already exists` (10 November 2026), `UNIQUE constraint failed` while creating the `bronze` namespace, and `attempt to write a readonly database` (both 12 November). Each cleared on the next run. A competing run, a container crash, the filesystem and a one-step retry inside the command were all ruled out, and the root cause is unknown. If it recurs, keep the complete task log from the top: the diagnostics above are in it. The investigation notes are kept outside the repo.

---

## Task 2: bronze_verify

```bash
bash scripts/verify_unless_unchanged.sh
```

Reads every landed object, validates every event against the change-event contract, and proves each one maps correctly to its Bronze row. Writes nothing. Never reads the Bronze tables, so it stays within container memory limits at any dataset size.

`bronze.verify` (run manually outside the DAG) performs the full digest comparison between Bronze and the landing. That is not done here because PyIceberg loads the full Bronze tables into memory: at 8.3M rows in `merchant_transactions` alone, the process is killed by the OS before it finishes inside a container.

Runs with `bronze_writer.json` credentials.

**When it is skipped.** The check takes about an hour at 8.5M events (56 min 36 s on 12 November 2026) and reads every landed object, so it is skipped when the landing has not changed since a run marker that already passed it. The script reads the newest run marker (`python -m bronze.check --newest-marker`: one listing and one read, no landed object) and compares its id with the Airflow Variable `bronze_last_verified_marker`. If they are equal the task exits 99 and Airflow shows it as skipped (7 to 11 s measured). Otherwise it runs the check, and when the check passes it records the newest marker's id in the Variable. `bronze_write` runs first, so anything new in the landing has already produced a newer marker by the time this task starts. When that run read every landed object itself, because pass 1 is the same index build as `bronze.check`, it records the new marker as verified (`--verified-marker-file`, then `airflow variables set`), so this task is skipped after a run with new data too. A run that skipped its own index records nothing. If `bronze.check` ever gains a check that pass 1 does not make, remove `--verified-marker-file` from the `bronze_write` command, or this task would skip it. The skip trusts that landed objects are never overwritten, as the `bronze_write` skip does. If the Variable is lost or empty, the check simply runs.

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

**What can go wrong:** a BigQuery metadata API failure on one table leaves the others registered and that table pointing at the old snapshot. The task exits non-zero and the dbt layers do not run.

---

## Task 4: check_bq_schema

```bash
python scripts/check_bq_schema.py --project payments-512910 --location us-east1
```

Fails if a BigQuery external Bronze table's columns differ from the Bronze schema contract (`bronze.schema.build_schema`), so a stale registration stops the run before dbt. Reads table metadata only. Runs with `pipeline_runner.json` credentials.

---

## Tasks 5 to 8: the dbt layers

```bash
cd /opt/repo/dbt && dbt deps && dbt build --select path:models/<layer> --exclude tag:periodic
```

One task per layer, in this order: `dbt_build_staging`, `dbt_build_dimensions`, `dbt_build_intermediate`, `dbt_build_marts`. `dbt deps` runs first in each so packages are present on a fresh container. `dbt build` runs each model and its tests together, so a failing test stops every later layer and bad data does not reach the marts.

| Task | Models | Tests | Result on 10 November 2026 |
|---|---|---|---|
| `dbt_build_staging` | 5 views | 42 | 47 of 47 passed |
| `dbt_build_dimensions` | 3 tables | 29 | 32 of 32 passed |
| `dbt_build_intermediate` | 2 incremental models, 4 tables | 43 | 49 of 49 passed |
| `dbt_build_marts` | 11 incremental models | 54 | 65 of 65 passed |

`--full-refresh` is not passed by default. The four large models (the enriched and GPV intermediates and the settlement and revenue marts) are incremental with a literal 9-day lookback, so a normal run builds only recent days. Trigger the DAG with `full_refresh` set to true for a controlled rebuild, for example after a Bronze reset: an incremental run after a reset would add new rows on top of rows from the previous dataset.

Runs with `pipeline_runner.json` credentials.

**What can go wrong:** a dbt model compilation error, a failing test or a BigQuery query failure stops the run. `dbt deps` failure (network issue reaching dbt Hub) would prevent packages from resolving, though they were already installed in the container image. Check the Airflow task logs for the specific test name and table.

**Test cost.** The 168 tests executed in the DAG billed 3.002 GB, 68.6% of the 4.379 GB the whole DAG billed on 10 November 2026. The full breakdown is in the warehouse cost study, which is kept outside the repo.

---

## Run times

| Run | Total | Notes |
|---|---|---|
| Full DAG, nothing new, 10 November 2026 | 1 h 35 min | `bronze_write` about 53 min, `bronze_verify` about 38 min, four dbt layers about 4 min |
| Full DAG, nothing new, 12 November 2026 | 1 h 7 min 34 s | After the `bronze_write` skip; `bronze_verify` 56 min 36 s of it |
| Full DAG, nothing new, 12 November 2026 (21:25 UTC) | 11 min 33 s | After both skips |
| Full DAG with new data, 7 November 2026 (8.54M events) | about 2 h 6 min | `bronze_write` about 124 min |

A run with a small amount of new data is estimated at about 1 hour (catalog rebuild 5 to 7 min, pass 1 about 42 min, pass 2 a few minutes, dbt about 5 min, `bronze_verify` skipped). That estimate has not been measured.

---

## Virtualenv isolation

Airflow 2.9.3 uses SQLAlchemy 1.4.x. PyIceberg 0.12.0 requires SQLAlchemy 2.x. These two cannot coexist in the same Python environment.

The solution: an isolated virtualenv at `/opt/pipeline-venv`, built during the Docker image build from `airflow/requirements-pipeline.txt`. It contains PyIceberg, PyArrow, and dbt-bigquery, independent of Airflow's base packages. All pipeline tasks call `${VENV}/python` and `${VENV}/dbt` directly, where `VENV=/opt/pipeline-venv/bin`.

---

## Credentials and bind mounts

Three paths are bind-mounted from the host into every container:

| `airflow/.env` variable | Mounted at | Used by |
|---|---|---|
| `BRONZE_WRITER_CREDS_PATH` | `/opt/credentials/bronze_writer.json` | `bronze_write` and `bronze_verify` tasks |
| `PIPELINE_CREDS_PATH` | `/opt/credentials/pipeline_runner.json` | `register_bq`, `check_bq_schema` and the four `dbt_build_*` tasks |
| `DBT_PROFILES_DIR` | `/home/airflow/.dbt/` | the four `dbt_build_*` tasks |

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
| `bronze_write` | Skips events already written (content-hash idempotency) and writes any new ones. Reads no landed object when nothing is new. |
| `bronze_verify` | Re-reads the landing and re-checks, unless the newest run marker already passed, which includes a run where `bronze_write` read every landed object itself (then skipped; use `force_verify` to check again). |
| `register_bq` | Re-points all five external tables to the current Bronze state. |
| `check_bq_schema` | Re-checks the five external tables against the contract. |
| `dbt_build_*` | Rebuilds each layer's models incrementally (or fully, with `full_refresh`) and re-runs its tests. |

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
