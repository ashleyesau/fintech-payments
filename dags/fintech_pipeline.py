"""
fintech_pipeline: orchestrates the full fintech data pipeline end to end.

Task order:
    bronze_write >> bronze_verify >> register_bq >> dbt_marts >> dbt_test

bronze_write
    python -m bronze.main --write
    Reads new CDC events from gs://cdc_landing and appends them to the five Bronze
    Iceberg tables in gs://fintech_bronze. Runs as the bronze-writer service account.

bronze_verify
    python -m bronze.verify --landing-only
    Proves every landed event maps to a valid Bronze row and no key is held twice.
    --landing-only is used because fintech_cdc is not accessible from inside the
    Airflow container network. Run python -m bronze.verify (without the flag) manually
    for the full source comparison. Runs as the bronze-writer service account.

register_bq
    python scripts/register_bronze_bq.py --project ... --location ... --connection ...
    Re-points the five BigQuery external Iceberg tables at the metadata file written
    by bronze_write. Runs as the pipeline-runner service account.

dbt_marts
    dbt deps && dbt run --full-refresh
    Rebuilds all dbt models from scratch (staging, dimensions, intermediate, marts).
    --full-refresh is used so no layer is ever stale after a Bronze write. dbt deps
    runs first so dbt_utils is present on a fresh container. Runs as pipeline-runner.

dbt_test
    dbt test
    Runs all dbt schema tests across every layer. A model that builds without error
    but returns bad data fails here. Runs as pipeline-runner.

Lightdash
    No step needed. Lightdash queries BigQuery directly and picks up the new mart data
    on the next dashboard load.

Pipeline virtualenv: /opt/pipeline-venv
    Airflow 2.9.3 uses SQLAlchemy 1.4.x; pyiceberg 0.12.0 requires 2.x. All pipeline
    tasks use the isolated venv to avoid the conflict.

Credentials:
    bronze_write and bronze_verify use bronze_writer.json (bronze-writer service account).
    register_bq, dbt_marts and dbt_test use pipeline_runner.json (pipeline-runner service
    account). Create that account via Terraform (infra/iam.tf) and set PIPELINE_CREDS_PATH
    in airflow/.env. Until it is set up, PIPELINE_CREDS_PATH can point to your ADC file
    as a temporary fallback.

Schedule: paused at creation (schedule=None). Enable with schedule="0 6 * * *" and
unpause from the UI or:
    docker compose -f airflow/docker-compose.yml exec scheduler airflow dags unpause fintech_pipeline
"""
from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
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
) as dag:

    bronze_write = BashOperator(
        task_id="bronze_write",
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
        bash_command=f"cd {REPO} && {VENV}/python -m bronze.verify --landing-only",
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

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=f"cd {REPO}/dbt && {VENV}/dbt deps && {VENV}/dbt run --full-refresh",
        env={"GOOGLE_APPLICATION_CREDENTIALS": CREDS_PIPELINE},
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {REPO}/dbt && {VENV}/dbt test",
        env={"GOOGLE_APPLICATION_CREDENTIALS": CREDS_PIPELINE},
        append_env=True,
    )

    bronze_write >> bronze_verify >> register_bq >> dbt_marts >> dbt_test
