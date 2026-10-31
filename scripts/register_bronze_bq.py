"""
scripts/register_bronze_bq.py: register the five Bronze Iceberg tables as BigQuery
external tables in the bronze dataset (Semantic Layer plan, S0).

Reads the newest run marker from gs://fintech_bronze, extracts the metadata_location
for each of the five tables, and executes one statement per table:

    CREATE OR REPLACE EXTERNAL TABLE `<project>.bronze.<table>`
    WITH CONNECTION `projects/<project>/locations/<location>/connections/<name>`
    OPTIONS (format = 'ICEBERG', uris = ['<metadata_location>']);

With --dry-run the DDL is printed to stdout without executing anything.

Run with your own gcloud login (Application Default Credentials), not as bronze-writer.
bronze-writer has no BigQuery permissions; this script needs bigquery.jobs.create and
bigquery.tables.create, which your project-owner account already has.

    python scripts/register_bronze_bq.py --connection fintech-bronze

Exit codes:
    0  all five tables registered (or --dry-run printed the DDL)
    1  usage or configuration error
    2  no run marker found in gs://fintech_bronze/_runs/
    3  a GCS or BigQuery API call failed
"""
from __future__ import annotations

import json
import sys

import click

_BUCKET = "fintech_bronze"
_RUNS_PREFIX = "_runs/"

_TABLES = (
    "merchants",
    "merchant_transactions",
    "loan_applications",
    "loan_repayments",
    "risk_events",
)


def _full_connection(project: str, location: str, name: str) -> str:
    """Return the fully-qualified connection resource path."""
    if name.startswith("projects/"):
        return name
    return f"projects/{project}/locations/{location}/connections/{name}"


def _read_newest_marker(project: str) -> dict:
    """Read and parse the newest run marker JSON from gs://fintech_bronze/_runs/."""
    from google.cloud import storage as gcs
    from google.api_core import exceptions as gcp_exc

    try:
        client = gcs.Client(project=project)
        blobs = sorted(
            [b for b in client.list_blobs(_BUCKET, prefix=_RUNS_PREFIX)
             if b.name.endswith(".json")],
            key=lambda b: b.name,
        )
    except gcp_exc.GoogleAPICallError as exc:
        click.echo(f"error: could not list {_BUCKET}/{_RUNS_PREFIX}: {exc}", err=True)
        sys.exit(3)

    if not blobs:
        click.echo(f"error: no run marker found in gs://{_BUCKET}/{_RUNS_PREFIX}", err=True)
        sys.exit(2)

    try:
        return json.loads(blobs[-1].download_as_bytes())
    except gcp_exc.GoogleAPICallError as exc:
        click.echo(f"error: could not read marker {blobs[-1].name}: {exc}", err=True)
        sys.exit(3)


@click.command()
@click.option(
    "--connection",
    required=True,
    help=(
        "BigQuery Cloud Resource connection name or full resource path. "
        "Short name (e.g. fintech-bronze) is expanded to "
        "projects/<project>/locations/<location>/connections/<name>."
    ),
)
@click.option(
    "--project",
    default="payments-512910",
    show_default=True,
    help="GCP project for BigQuery datasets and the connection.",
)
@click.option(
    "--location",
    default="us-east1",
    show_default=True,
    help="BigQuery dataset and connection location.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print DDL statements to stdout without executing them.",
)
def main(
    connection: str,
    project: str,
    location: str,
    dry_run: bool,
) -> None:
    """Register Bronze Iceberg tables as BigQuery external Iceberg tables.

    Run with your own gcloud login, not as bronze-writer.
    """
    conn_path = _full_connection(project, location, connection)

    click.echo(f"Reading newest run marker from gs://{_BUCKET}/{_RUNS_PREFIX} ...", err=True)
    marker = _read_newest_marker(project)
    run_id = marker.get("run_id", "(unknown)")
    click.echo(f"Marker: {run_id}", err=True)

    tables_data = marker.get("tables", {})
    statements: list[tuple[str, str, int]] = []
    for table in _TABLES:
        entry = tables_data.get(table)
        if entry is None:
            click.echo(f"error: table {table!r} not found in marker", err=True)
            sys.exit(2)
        meta_loc = entry["metadata_location"]
        rows = entry["rows"]
        ddl = (
            f"CREATE OR REPLACE EXTERNAL TABLE `{project}.bronze.{table}`\n"
            f"WITH CONNECTION `{conn_path}`\n"
            f"OPTIONS (\n"
            f"  format = 'ICEBERG',\n"
            f"  uris = ['{meta_loc}']\n"
            f");"
        )
        statements.append((table, ddl, rows))

    if dry_run:
        for table, ddl, rows in statements:
            click.echo(f"\n-- {table}  ({rows:,} rows expected)")
            click.echo(ddl)
        click.echo("\n-- dry-run: no DDL executed")
        return

    try:
        from google.cloud import bigquery as bq
        from google.api_core import exceptions as gcp_exc
    except ImportError:
        click.echo(
            "error: google-cloud-bigquery is not installed. "
            "Run: pip install --default-timeout 100 google-cloud-bigquery>=3.4.0",
            err=True,
        )
        sys.exit(1)

    client = bq.Client(project=project)

    for table, ddl, rows in statements:
        click.echo(f"  Registering {table} ...", err=True)
        try:
            job = client.query(ddl)
            job.result()
        except gcp_exc.GoogleAPICallError as exc:
            click.echo(
                f"error: BigQuery DDL failed for {table}: {exc}",
                err=True,
            )
            sys.exit(3)
        click.echo(f"  {table}: OK  (expected {rows:,} rows)", err=True)

    click.echo(
        f"\nAll five tables registered in {project}.bronze  "
        f"[connection: {conn_path}]",
        err=True,
    )


if __name__ == "__main__":
    main()
