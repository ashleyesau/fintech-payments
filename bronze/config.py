"""
config.py: Bronze settings and the credentials guard.

Every field reads from an environment variable with the BRONZE_ prefix, or from .env. The
landing bucket, the slot name and the source database are not repeated here: they come from CdcSettings
(CDC_GCS_BUCKET, CDC_SLOT_NAME and CDC_DB), so the reader and the consumer can never disagree about where the objects are.

Authentication is application-default credentials and nothing else. The Bronze job runs as the
bronze-writer service account (GCS Plan (V2) section 13): read on cdc_landing, read and write on
fintech_bronze. The account is selected per process by GOOGLE_APPLICATION_CREDENTIALS. Google reads
that variable from the process environment, not from .env, so the guard reads the environment too.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from bronze.errors import ConfigError
from cdc.config import CdcSettings

CREDENTIALS_VARIABLE = "GOOGLE_APPLICATION_CREDENTIALS"


class BronzeSettings(BaseSettings):
    # The Bronze bucket (decided G-D7). Table locations are gs://<bucket>/bronze/<table>/.
    bucket: str = "fintech_bronze"

    # The Google project the Bronze job's client is made for. The bronze-writer credentials file (an
    # impersonated service account) carries no project, and without one the client cannot be made.
    project: str = "payments-512910"

    # The local SQLite catalog (decided G-D3). It is a cache of the bucket's metadata files, so it
    # lives outside the repo; the default is under the user's config directory.
    catalog_path: str = "~/.config/fintech_bronze/catalog.db"

    # Landed objects read at once (decision 1, measured at 8 for uploads; reads are the same shape).
    read_workers: int = Field(default=8, ge=1)

    # Rows a pass 2 batch holds before it is sorted and committed (GCS Plan (V2) section 19, question 1). Whole
    # landed objects are added until the batch holds at least this many rows, so a batch is a little over.
    # One batch is one Iceberg commit. Measured: 669 and 673 MB above the start at 100,000 with the index live;
    # 250,000 cost 898 MB for 28 objects against 52. Raise it only after the 11M-event measurement (P6).
    batch_rows: int = Field(default=100_000, ge=1)

    model_config = SettingsConfigDict(env_file=".env", env_prefix="BRONZE_", extra="ignore")

    @property
    def catalog_file(self) -> Path:
        return Path(self.catalog_path).expanduser()

    @property
    def landing_bucket(self) -> str:
        return CdcSettings().gcs_bucket

    @property
    def slot_name(self) -> str:
        return CdcSettings().slot_name

    @property
    def source_db(self) -> str:
        """The database the landed events came from (source.db of every event): the mapping refuses an event
        from any other, so one warehouse is built from one source database (G2 Q1)."""
        return CdcSettings().db

    @property
    def landing_prefix(self) -> str:
        """The prefix of the run being read: cdc/<slot_name>/."""
        return CdcSettings().gcs_prefix


def require_credentials(environ: Optional[Mapping[str, str]] = None) -> Path:
    """Refuse to go on unless GOOGLE_APPLICATION_CREDENTIALS names a file; return its path.

    Without the variable Google falls back to whatever login the machine has, which on the
    development Mac is the owner's own, with write access to the landing bucket. The account's
    permissions are the real control; this guard only makes sure it is the account in use.
    """
    env = os.environ if environ is None else environ
    value = env.get(CREDENTIALS_VARIABLE, "").strip()
    if not value:
        raise ConfigError(f"{CREDENTIALS_VARIABLE} is not set. Point it at the bronze-writer credentials "
                          "file before running a Bronze command: without it Google falls back to your own "
                          "login, which can write to the landing bucket.")
    path = Path(value).expanduser()
    if not path.is_file():
        raise ConfigError(f"{CREDENTIALS_VARIABLE} names a file that does not exist: {path}")
    return path


settings = BronzeSettings()
