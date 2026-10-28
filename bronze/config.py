"""
config.py: Bronze settings and the credentials guard.

Every field reads from an environment variable with the BRONZE_ prefix, or from .env. The
landing bucket and the slot name are not repeated here: they come from CdcSettings (CDC_GCS_BUCKET
and CDC_SLOT_NAME), so the reader and the consumer can never disagree about where the objects are.

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

    # The local SQLite catalog (decided G-D3). It is a cache of the bucket's metadata files, so it
    # lives outside the repo; the default is under the user's config directory.
    catalog_path: str = "~/.config/fintech_bronze/catalog.db"

    # Landed objects read at once (decision 1, measured at 8 for uploads; reads are the same shape).
    read_workers: int = Field(default=8, ge=1)

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
