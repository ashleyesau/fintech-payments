"""
config.py: CDC consumer settings.

Separate from the generator's SimSettings on purpose: the consumer has its own connection
fields, so moving it to a dedicated role (REPLICATION plus SELECT) is a configuration
change. Every field reads from an environment variable with the CDC_ prefix, or from .env.

The consumer reads the fintech_cdc database. The generator is pointed at the same database
with POSTGRES_DB, so a CDC run needs both set (see the README).
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

from cdc.errors import SinkError

CREDENTIALS_VARIABLE = "GOOGLE_APPLICATION_CREDENTIALS"


class CdcSettings(BaseSettings):
    # Consumer connection
    host: str = "localhost"
    port: int = 5432
    db: str = "fintech_cdc"
    user: str = "sim"
    password: str = "sim"

    # Slot and publication. Slot names are cluster-wide, and one slot name is used per run:
    # a regenerated run gets a new name (cdc_r02) and so a new bucket prefix.
    slot_name: str = "cdc_r01"
    publication: str = "fintech_cdc"

    # GCS sink. Authentication is application-default credentials; nothing goes in the repo.
    gcs_bucket: str = "cdc_landing"

    # The Google project the storage client is made for. The consumer runs as the cdc-consumer service account
    # through an impersonated credentials file, and that file carries no project: without one the client cannot
    # be made (the first real Bronze run failed exactly so). It is always passed, so there is one code path.
    project: str = "payments-512910"

    # Fixed in M4 from the measured decode speed and per-object upload latency (DECISION_LOG,
    # "M4 design decisions"). batch_size is a floor of changes: a batch is whole transactions.
    # poll_interval_seconds is how long --follow sleeps when a fetch returns nothing; a drain
    # never sleeps. upload_workers is the number of objects uploaded at once in one flush.
    batch_size: int = 10_000
    poll_interval_seconds: float = 60.0
    upload_workers: int = 8

    model_config = SettingsConfigDict(env_file=".env", env_prefix="CDC_", extra="ignore")

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.db} "
            f"user={self.user} password={self.password}"
        )

    @property
    def gcs_prefix(self) -> str:
        """Object prefix for this run: cdc/<slot_name>/, so runs never mix."""
        return f"cdc/{self.slot_name}/"


settings = CdcSettings()


def require_credentials(environ: Optional[Mapping[str, str]] = None) -> Path:
    """Refuse to go on unless GOOGLE_APPLICATION_CREDENTIALS names a file; return its path.

    The consumer runs as the cdc-consumer service account (create and read back, never update or delete).
    Without the variable Google falls back to whatever login the machine has, which on the development Mac is
    the owner's own and can overwrite or delete a landed object. The account's permissions are the real
    control (python -m cdc.permissions shows them); this guard only makes sure a credentials file was named.
    It never reads the file, and it cannot tell whose file it is. It is a SinkError, exit code 3: the
    credentials are missing.
    """
    env = os.environ if environ is None else environ
    value = env.get(CREDENTIALS_VARIABLE, "").strip()
    if not value:
        raise SinkError(f"{CREDENTIALS_VARIABLE} is not set. Point it at the cdc-consumer credentials file before "
                        "running the consumer: without it Google falls back to your own login, which can overwrite "
                        "or delete landed objects.")
    path = Path(value).expanduser()
    if not path.is_file():
        raise SinkError(f"{CREDENTIALS_VARIABLE} names a file that does not exist: {path}")
    return path
