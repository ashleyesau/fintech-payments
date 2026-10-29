"""
permissions.py: what may the cdc-consumer account do on the two data buckets? No write, no object call.

    GOOGLE_APPLICATION_CREDENTIALS=/Users/ashleyesau/.config/gcloud_cdc/application_default_credentials.json \
        python -m cdc.permissions

It signs in as whatever the credentials file names, refuses to go on unless that is the cdc-consumer
service account (so it cannot run as the owner, be told everything is granted and look like a pass), and
then asks Google what that account may do with the testIamPermissions call: one metadata call per
bucket, nothing written, no Class A or Class B operation. The answer must match the least-privilege
table (Infrastructure plan, section 8):

    cdc_landing     granted: create, get and list objects (what the consumer's three calls need)
                    denied:  update and delete objects, anything on the bucket itself, any bucket IAM change
    fintech_bronze  denied:  everything above (the consumer has no role there)

Exit codes: 0 every permission is as expected; 1 at least one differs (a grant that should not be there,
or one that is missing); 2 the credentials are not the cdc-consumer account, or Google could not be asked.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional, Sequence

import click

from cdc.config import settings

ACCOUNT = "cdc-consumer@payments-512910.iam.gserviceaccount.com"
PROJECT = "payments-512910"
SCOPE = "https://www.googleapis.com/auth/cloud-platform"
BRONZE_BUCKET = "fintech_bronze"

EXIT_OK, EXIT_DIFFERS, EXIT_REFUSED = 0, 1, 2

# What cdc/gcs.py needs: upload_from_string with if_generation_match=0 (create), download_as_bytes (get)
# and list_blobs (list). Nothing else.
LANDING_GRANTED = ("storage.objects.create", "storage.objects.get", "storage.objects.list")

# Google refuses to test these on a bucket with uniform bucket-level access (both buckets have it; there is no
# object-level IAM there) and answers 400 for the whole call, so they are never asked. Found by the first real run.
NOT_TESTABLE = ("storage.objects.getIamPolicy", "storage.objects.setIamPolicy")

# Never granted to the consumer on either bucket. Update and delete are the point: a landed object can
# never be overwritten or removed through this account.
NEVER = (
    "storage.objects.update",
    "storage.objects.delete",
    "storage.buckets.get",
    "storage.buckets.update",
    "storage.buckets.delete",
    "storage.buckets.setIamPolicy",
)


@dataclass(frozen=True)
class Expectation:
    bucket: str
    granted: tuple[str, ...]
    denied: tuple[str, ...]


@dataclass(frozen=True)
class Row:
    bucket: str
    permission: str
    expected: bool
    actual: bool

    @property
    def ok(self) -> bool:
        return self.expected == self.actual


def expectations(landing_bucket: str, bronze_bucket: str = BRONZE_BUCKET) -> list[Expectation]:
    return [
        Expectation(landing_bucket, LANDING_GRANTED, NEVER),
        Expectation(bronze_bucket, (), LANDING_GRANTED + NEVER),
    ]


def ask(client, expectation: Expectation) -> list[Row]:
    """One testIamPermissions call for the bucket: every permission we expect, granted or denied."""
    asked = list(expectation.granted) + list(expectation.denied)
    answer = set(client.bucket(expectation.bucket).test_iam_permissions(asked))
    return [Row(expectation.bucket, permission, permission in expectation.granted, permission in answer)
            for permission in asked]


def run(client, identity: str, wanted: Sequence[Expectation], account: str = ACCOUNT,
        out: Callable[[str], None] = click.echo) -> int:
    if identity != account:
        out(f"Refusing to go on: the credentials resolve to {identity or 'a user login'}, not {account}. "
            "Point GOOGLE_APPLICATION_CREDENTIALS at the cdc-consumer file, or this would test the wrong account.")
        return EXIT_REFUSED
    out(f"account {identity}")
    wrong = 0
    for expectation in wanted:
        out(f"gs://{expectation.bucket}")
        for row in ask(client, expectation):
            wrong += 0 if row.ok else 1
            state = "granted" if row.actual else "denied "
            note = "ok" if row.ok else ("WRONG: this should be denied" if row.actual else "WRONG: this should be granted")
            out(f"  {state}  {row.permission}  {note}")
    if wrong:
        out(f"{wrong} permission(s) differ from the least-privilege table")
        return EXIT_DIFFERS
    out("every permission is as the least-privilege table says")
    return EXIT_OK


def resolve_identity() -> str:
    """The service account the application-default credentials resolve to ('' for a user login)."""
    import google.auth
    from google.auth.transport.requests import Request

    credentials, _ = google.auth.default(scopes=[SCOPE])
    credentials.refresh(Request())
    return getattr(credentials, "service_account_email", "") or ""


def make_client(project: str):
    """The impersonated file carries no project, so it is passed (as BRONZE_PROJECT is for Bronze)."""
    from google.cloud import storage

    return storage.Client(project=project)


@click.command()
@click.option("--landing-bucket", default=settings.gcs_bucket, show_default=True)
@click.option("--bronze-bucket", default=BRONZE_BUCKET, show_default=True)
@click.option("--project", default=PROJECT, show_default=True)
def main(landing_bucket: str, bronze_bucket: str, project: str) -> None:
    try:
        identity = resolve_identity()
        if identity != ACCOUNT:
            code = run(None, identity, [])
        else:
            code = run(make_client(project), identity, expectations(landing_bucket, bronze_bucket))
    except Exception as error:  # Google could not be asked: say so, never a traceback with credentials in it
        click.echo(f"Could not ask Google: {type(error).__name__}: {error}")
        raise SystemExit(EXIT_REFUSED)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
