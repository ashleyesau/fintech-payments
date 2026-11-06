"""
fintech_pipeline: orchestrates the full fintech data pipeline end to end.

Task order:
    bronze_write  >>  register_bq  >>  dbt_marts

bronze_write
    python -m bronze.main --write
    Reads new CDC events from gs://cdc_landing and appends them to the five Bronze
    Iceberg tables in gs://fintech_bronze. Runs as the bronze-writer service account
    (GOOGLE_APPLICATION_CREDENTIALS set to the bind-mounted credentials file).

register_bq
    python scripts/register_bronze_bq.py --project ... --location ... --connection ...
    Re-points the five BigQuery external Iceberg tables at the metadata file written
    by the previous step. Required after every Bronze write: without it dbt reads the
    stale snapshot from the previous run. Runs with ADC (gcloud auth application-default
    login).

dbt_marts
    dbt deps && dbt run --select marts
    Rebuilds all 11 mart fact tables from the updated Bronze sources. dbt deps runs
    first so dbt_utils is present on a fresh container. Runs with ADC.

Lightdash
    No step needed. Lightdash queries BigQuery directly and picks up the new mart data
    on the next dashboard load.

Pipeline virtualenv: /opt/pipeline-venv
    Airflow 2.9.3 uses SQLAlchemy 1.4.x; pyiceberg 0.12.0 requires 2.x. All pipeline
    tasks use the isolated venv to avoid the conflict.

Schedule: 0 6 * * * (06:00 UTC, 08:00 SAST). Paused at creation; unpause from the UI
or: docker compose exec scheduler airflow dags unpause fintech_pipeline
"""
from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = "/opt/repo"
VENV = "/opt/pipeline-venv/bin"
CREDS_BRONZE = "/opt/credentials/bronze_writer.json"
CREDS_ADC = "/opt/credentials/adc.json"

with DAG(
    dag_id="fintech_pipeline",
    description="Bronze write, BQ re-registration, dbt marts rebuild",
    schedule="0 6 * * *",
    start_date=datetime(2026, 11, 6, tzinfo=timezone.utc),
    catchup=False,
    tags=["fintech", "bronze", "dbt"],
    doc_md=__doc__,
) as dag:

    bronze_write = BashOperator(
        task_id="bronze_write",
        bash_command=(
            "rm -f /home/airflow/.config/fintech_bronze/catalog.db"
            f" && cd {REPO} && {VENV}/python -m bronze.main --rebuild-catalog"
            f" && {VENV}/python -m bronze.main --write"
        ),
        env={"GOOGLE_APPLICATION_CREDENTIALS": CREDS_BRONZE},
        append_env=True,
    )

    register_bq = BashOperator(
        task_id="register_bq",
        bash_command=(
            f"cd {REPO} && {VENV}/python scripts/register_bronze_bq.py"
            " --project payments-512910 --location us-east1 --connection fintech-bronze"
        ),
        env={"GOOGLE_APPLICATION_CREDENTIALS": CREDS_ADC},
        append_env=True,
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=f"cd {REPO}/dbt && {VENV}/dbt deps && {VENV}/dbt run --select marts",
        env={"GOOGLE_APPLICATION_CREDENTIALS": CREDS_ADC},
        append_env=True,
    )

    bronze_write >> register_bq >> dbt_marts
