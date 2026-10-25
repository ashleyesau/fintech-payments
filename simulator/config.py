"""
config.py: Simulation parameters, archetype definitions, and feature flags.

Loaded once at startup. All tuneable values live here so scenario changes
(different archetype mix, simulation duration, peak-day overrides) never
require code changes: only a config edit or env-var override.

Phase 2 work: populate with real archetype parameters from the design docs.
"""
from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "fintech")
DB_USER = os.getenv("POSTGRES_USER", "sim")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sim")

DSN = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"


# ---------------------------------------------------------------------------
# Simulation defaults
# ---------------------------------------------------------------------------

DEFAULT_MERCHANT_COUNT = 1_000
SIMULATION_START_DATE = "2025-01-01"
SIMULATION_END_DATE = "2026-01-01"

# Streaming mode: target simulated seconds per real second
STREAMING_SPEED_FACTOR = 3_600  # 1 real second = 1 simulated hour


# ---------------------------------------------------------------------------
# Feature flags
# ---------------------------------------------------------------------------

ENABLE_RISK_EVENTS = True
ENABLE_LOAN_APPLICATIONS = True
ENABLE_PAYSHAP = True  # PayShap as a payment channel
