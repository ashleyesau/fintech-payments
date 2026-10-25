# fintech-payments

A synthetic-data platform for an SA SME fintech — merchant acquiring (card + instant EFT) and merchant cash advance. Generates stateful, causally coherent event data written to a local Postgres OLTP store, ready for a CDC pipeline downstream.

## What this demonstrates

- Stateful event simulation with realistic merchant lifecycle, seasonal patterns, and causal cross-stream dependencies (payments ↔ lending ↔ risk)
- Postgres as an OLTP source of truth with `wal_level=logical` — ready for CDC in V2
- Pydantic-validated event schemas matching the [Event Schemas](https://www.notion.so/SA-Fintech-Project-Recon-35e86c781f668045b701e10a0e7bf089) contract
- Two generator modes: **backfill** (full date range as fast as possible) and **streaming** (real-time pace)

## Stack

| Component | V1 | V2+ |
|---|---|---|
| Generator | Python + psycopg3 | — |
| OLTP store | Postgres 16 (Docker) | — |
| CDC | — | Python WAL reader / Debezium |
| Lakehouse | — | Iceberg on GCS |
| Warehouse | — | BigQuery |
| Transforms | — | dbt |

## Quick start

```bash
# 1. Start Postgres
docker-compose up -d

# 2. Set up Python environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Run schema migration (Phase 1)
# psql -h localhost -U sim -d fintech -f migrations/001_initial_schema.sql

# 4. Run backfill (Phase 7)
# python -m simulator.main --mode backfill --start-date 2025-01-01 --end-date 2026-01-01
```

## Project structure

```
simulator/          Core simulation modules
tests/              Schema compliance and reconciliation tests
migrations/         Postgres DDL (Phase 1)
.github/workflows/  CI
docker-compose.yml  Local Postgres stack
```

## Documentation

Full design lives in Notion: [SA Fintech Project Recon](https://www.notion.so/SA-Fintech-Project-Recon-35e86c781f668045b701e10a0e7bf089)

## Licence

MIT
