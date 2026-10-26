"""
config.py: Simulation parameters and YAML config loaders.

SimSettings reads from environment variables (or a .env file). All tuneable
values live in the config/ YAML files so scenario changes never require code
edits: only a config file or env-var override.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict

CONFIG_DIR = Path(__file__).parent.parent / "config"


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

class SimSettings(BaseSettings):
    # Database
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "fintech"
    postgres_user: str = "sim"
    postgres_password: str = "sim"

    # Runtime
    log_level: str = "INFO"

    # Simulation defaults (overridable via env or CLI flags)
    default_merchant_count: int = 1_000
    simulation_start_date: str = "2025-01-01"
    simulation_end_date: str = "2026-01-01"
    streaming_speed_factor: int = 3_600  # simulated seconds per real second

    # Feature flags
    enable_risk_events: bool = True
    enable_loan_applications: bool = True
    enable_payshap: bool = True

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )


settings = SimSettings()

# Module-level DSN kept for compatibility with tests/test_db.py.
DSN = settings.dsn


# ---------------------------------------------------------------------------
# YAML config loaders
# ---------------------------------------------------------------------------

# Cached because the generator reads calibration once per merchant-day; callers
# must treat the returned dicts as read-only.
@lru_cache(maxsize=None)
def _load_yaml(name: str) -> dict[str, Any]:
    path = CONFIG_DIR / name
    with open(path) as fh:
        return yaml.safe_load(fh) or {}


def load_calibration() -> dict[str, Any]:
    return _load_yaml("calibration.yaml")


def load_fees() -> dict[str, Any]:
    return _load_yaml("fees.yaml")


def load_payments() -> dict[str, Any]:
    return _load_yaml("payments.yaml")


def load_lending() -> dict[str, Any]:
    return _load_yaml("lending.yaml")


def load_causal_rules() -> dict[str, Any]:
    return _load_yaml("causal_rules.yaml")


def load_archetypes_config() -> dict[str, Any]:
    return _load_yaml("archetypes.yaml")
