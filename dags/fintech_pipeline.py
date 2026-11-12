"""
fintech_pipeline: orchestrates the full fintech data pipeline end to end.

Task order:
    bronze_write >> bronze_verify >> register_bq >> check_bq_schema
        >> dbt_build_staging
        >> dbt_build_dimensions >> dbt_build_intermediate >> dbt_build_marts

bronze_write
    python -m bronze.main --write
    Reads new CDC events from gs://cdc_landing and appends them to the five Bronze
    Iceberg tables in gs://fintech_bronze. When nothing new has landed since the newest run
    marker it reads no object and finishes in seconds. Runs as the bronze-writer service account.

bronze_verify
    python -m bronze.check
    Reads every landed object, validates every event against the change-event contract
    and proves each one maps correctly to its Bronze row. Writes nothing and never reads
    the Bronze tables, so it stays within container memory limits at any dataset size.
    bronze.verify (run manually outside the DAG) performs the full digest comparison
    between Bronze and the landing. Runs as the bronze-writer service account.

register_bq
    python scripts/register_bronze_bq.py --project ... --location ... --connection ...
    Re-points the five BigQuery external Iceberg tables at the metadata file written
    by bronze_write. Runs as the pipeline-runner service account.

check_bq_schema
    python scripts/check_bq_schema.py
    Fails if a BigQuery external Bronze table's columns differ from the Bronze contract
    (bronze.schema.build_schema), so a stale registration stops the run before dbt.
    Runs as pipeline-runner.

dbt_build_staging, dbt_build_dimensions, dbt_build_intermediate, dbt_build_marts
    dbt deps && dbt build --select path:models/<layer>
    Builds one layer at a time. dbt build runs each model and its tests together, so a
    failing test stops every layer after it. --full-refresh is NOT passed by default.
    Trigger the DAG with the full_refresh param set to true for a controlled rebuild.
    dbt deps runs first so packages are present on a fresh container. Runs as
    pipeline-runner.

Lightdash
    No step needed. Lightdash queries BigQuery directly and picks up the new mart data
    on the next dashboard load.

Pipeline virtualenv: /opt/pipeline-venv
    Airflow 2.9.3 uses SQLAlchemy 1.4.x; pyiceberg 0.12.0 requires 2.x. All pipeline
    tasks use the isolated venv to avoid the conflict.

Credentials:
    bronze_write and bronze_verify use bronze_writer.json (bronze-writer service account).
    register_bq, check_bq_schema and the dbt_build_* tasks use pipeline_runner.json (pipeline-runner service
    account). Create that account via Terraform (infra/iam.tf) and set PIPELINE_CREDS_PATH
    in airflow/.env. Until it is set up, PIPELINE_CREDS_PATH can point to your ADC file
    as a temporary fallback.

Schedule: paused at creation (schedule=None). Enable with schedule="0 6 * * *" and
unpause from the UI or:
    docker compose -f airflow/docker-compose.yml exec scheduler airflow dags unpause fintech_pipeline
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

REPO = "/opt/repo"
VENV = "/opt/pipeline-venv/bin"
CREDS_BRONZE = "/opt/credentials/bronze_writer.json"
CREDS_PIPELINE = "/opt/credentials/pipeline_runner.json"

with DAG(
    dag_id="fintech_pipeline",
    description="Bronze write, verify, BQ registration, dbt build and test",
    schedule=None,
    start_date=datetime(2026, 11, 6, tzinfo=timezone.utc),
    catchup=False,
    tags=["fintech", "bronze", "dbt"],
    doc_md=__doc__,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    params={
        "full_refresh": Param(
            False,
            type="boolean",
            description="Controlled rebuild only. Passes --full-refresh to every dbt layer.",
        )
    },
) as dag:

    bronze_write = BashOperator(
        task_id="bronze_write",
        retries=0,
        bash_command=(
            "rm -f /home/airflow/.config/fintech_bronze/catalog.db"
            f" && cd {REPO} && ({VENV}/python -m bronze.main --rebuild-catalog || true)"
            f" && {VENV}/python -m bronze.main --write"
        ),
        env={"GOOGLE_APPLICATION_CREDENTIALS": CREDS_BRONZE},
        append_env=True,
    )

    bronze_verify = BashOperator(
        task_id="bronze_verify",
        bash_command=f"cd {REPO} && {VENV}/python -m bronze.check",
        env={"GOOGLE_APPLICATION_CREDENTIALS": CREDS_BRONZE},
        append_env=True,
    )

    register_bq = BashOperator(
        task_id="register_bq",
        bash_command=(
            f"cd {REPO} && {VENV}/python scripts/register_bronze_bq.py"
            " --project payments-512910 --location us-east1 --connection fintech-bronze"
        ),
        env={"GOOGLE_APPLICATION_CREDENTIALS": CREDS_PIPELINE},
        append_env=True,
    )

    check_bq_schema = BashOperator(
        task_id="check_bq_schema",
        bash_command=(
            f"cd {REPO} && PYTHONPATH={REPO} {VENV}/python scripts/check_bq_schema.py"
            " --project payments-512910 --location us-east1"
        ),
        env={"GOOGLE_APPLICATION_CREDENTIALS": CREDS_PIPELINE},
        append_env=True,
    )

    FULL_REFRESH = "{{ '--full-refresh' if params.full_refresh else '' }}"

    def dbt_layer(layer: str) -> BashOperator:
        return BashOperator(
            task_id=f"dbt_build_{layer}",
            bash_command=(
                f"cd {REPO}/dbt && {VENV}/dbt deps"
                f" && {VENV}/dbt build --select path:models/{layer} --exclude tag:periodic {FULL_REFRESH}"
            ),
            env={"GOOGLE_APPLICATION_CREDENTIALS": CREDS_PIPELINE},
            append_env=True,
        )

    build_staging = dbt_layer("staging")
    build_dimensions = dbt_layer("dimensions")
    build_intermediate = dbt_layer("intermediate")
    build_marts = dbt_layer("marts")

    (
        bronze_write
        >> bronze_verify
        >> register_bq
        >> check_bq_schema
        >> build_staging
        >> build_dimensions
        >> build_intermediate
        >> build_marts
    )
