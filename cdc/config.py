"""
config.py: CDC consumer settings.

Separate from the generator's SimSettings on purpose: the consumer has its own connection
fields, so moving it to a dedicated role (REPLICATION plus SELECT) is a configuration
change. Every field reads from an environment variable with the CDC_ prefix, or from .env.

The consumer reads the fintech_cdc database. The generator is pointed at the same database
with POSTGRES_DB, so a CDC run needs both set (see the README).
"""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


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
