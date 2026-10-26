# fintech-payments

A synthetic-data platform for an SA SME fintech: merchant acquiring (card + instant EFT) and term lending with DebiCheck collection. Generates stateful, causally coherent event data written to a local Postgres OLTP store, ready for a CDC pipeline downstream.

## What this demonstrates

- Stateful event simulation with realistic merchant lifecycle, seasonal patterns, and causal cross-stream dependencies (payments, lending, risk)
- Postgres as an OLTP source of truth with `wal_level=logical`, ready for CDC in V2
- Pydantic-validated event schemas across five streams: `merchants`, `merchant_transactions`, `loan_applications`, `loan_repayments`, `risk_events`
- Two generator modes: **backfill** (full date range as fast as possible) and **streaming** (real-time pace)

## Stack

| Component | V1 | V2+ |
|---|---|---|
| Generator | Python + psycopg3 | |
| OLTP store | Postgres 16 (Docker) | |
| CDC | | Python WAL reader / Debezium |
| Lakehouse | | Iceberg on GCS |
| Warehouse | | BigQuery |
| Transforms | | dbt |

## Quick start

```bash
# 1. Start Postgres (Docker Desktop must be running)
docker compose up -d

# 2. Set up Python environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
cp .env.example .env

# 3. Apply the schema migrations in order (first time only)
docker exec -i fintech_pg psql -U sim -d fintech < migrations/001_initial_schema.sql
docker exec -i fintech_pg psql -U sim -d fintech < migrations/002_loan_repayments_fk.sql

# 4. Run tests (DB tests are skipped if Postgres is not running)
pytest tests/ -q                 # everything
pytest tests/ -q -m "not slow"   # skips the simulation-heavy tests

# 5. Run a backfill: 100 merchants over 2025
python -m simulator.main --mode backfill --start-date 2025-01-01 --end-date 2026-01-01 --merchant-count 100
```

## Running a backfill

Backfill advances one simulated day at a time as fast as the machine allows, writes each day to Postgres in one transaction, then writes a checkpoint.

- The end date is exclusive: 2025-01-01 to 2026-01-01 is 365 days.
- `--merchant-count` is the number of merchants onboarded, not the number active. They join over the first six months (10, 18, 25, 20, 15 and 12 percent a month), and from month seven each merchant that churns is replaced. About a third never transact, so 100 merchants gives roughly 67 that ever trade.
- A fresh run needs an empty `merchants` table and no existing checkpoint. It refuses to start otherwise and never deletes anything.
- `--resume` continues a stopped run from `checkpoint.json` (or `--checkpoint`), taking the start date, merchant count and seed from it. The checkpoint holds every merchant, numpy's RNG state and the run flags, so a resumed run writes the same rows an uninterrupted one would. A resume under different `ENABLE_RISK_EVENTS` or `ENABLE_LOAN_APPLICATIONS` values is refused.
- `--seed` (default `RANDOM_SEED`) seeds a fresh run.
- The run prints merchants onboarded, merchants that ever transacted, the final merchant states and the rows written.

A 100-merchant, 365-day backfill took 183 seconds in the sandbox. Scaling by merchant count puts 1,000 merchants at about 30 minutes, plus a checkpoint of about 1 second a day at that size. That is an extrapolation, not a measurement, and a laptop is slower.

## Running a stream

Stream mode generates a day exactly as backfill does, then replays the day's rows in time order across all merchants and writes each row on its own. The sleep between two rows is their timestamp gap divided by the speed factor (`STREAMING_SPEED_FACTOR`, default 3,600 simulated seconds per real second, so one simulated day takes about 24 seconds). A transaction and the risk events that point at it commit together. Generating the next day is not paced.

```bash
python -m simulator.main --mode backfill --start-date 2025-01-01 --end-date 2025-06-01 --merchant-count 50 --checkpoint demo.json
python -m simulator.main --mode stream --duration-days 1 --checkpoint demo.json
```

- Stream mode continues from the checkpoint if the file exists, and starts fresh from the start date if it does not. A fresh stream has no transactions for its first few days, because merchants activate one to thirty days after onboarding, so the demo above backfills first.
- `--duration-days` sets how many simulated days to stream. `--speed-factor` overrides the setting for one run.
- The streaming writer commits every row and manages about 900 rows a second in earlier measurements. At 1,000 merchants a day is about 43,000 rows, which is roughly 1,800 rows a second at the default speed, so a stream of that size falls behind its schedule. It writes late, logs a warning with the worst lateness, and drops nothing. A stream of about 50 merchants stays on schedule: a day of 1,181 transactions took 24 seconds.
- A stream stopped part-way through a day replays that day on the next run and skips the rows already written.

## Validating a run

`validate.sql` holds the reconciliation queries. Each one reads the five tables and returns a value next to the band from the Simulation Validation Targets, with the number of rows the value rests on.

```bash
python -m simulator.validation                          # every check against POSTGRES_DB
python -m simulator.validation --only par --only collections
psql -h localhost -U sim -d fintech -f validate.sql     # the same SQL, raw result sets
```

- The checks are measured, not asserted. A 100-merchant run is too noisy to hold a band, so the output is read across several seeds and larger runs. A run needs about 15 months to reach December, January and February, and the loan book needs longer than that to season at the default application rate.
- PAR is computed two ways from `loan_repayments`. The recorded measure takes `days_past_due` from a loan's latest row; it matches the generator's own loan state except for loans in arrears after their term ended, which the generator moves between buckets without writing a row. The instalment-age measure ages the oldest unpaid instalment, which is what a query over the table alone gives, and it runs ahead of the recorded measure between the monthly arrears cycles.
- `tests/test_reconcile.py` builds a 13-month, 100-merchant database with application propensity raised tenfold. It checks that the SQL loan book equals the generator's loans to the cent, asserts the seven failure modes from the Validation Targets, and plants each fault in the temp tables to confirm the query fires. It is marked slow.

## Project structure

```
simulator/          Core simulation modules
config/             YAML calibration, archetypes, fees and causal rules
scripts/            portfolio_check.py: end-to-end run that prints portfolio quality (minutes, not a test)
validate.sql        Reconciliation queries against the Validation Targets bands (run by simulator/validation.py)
tests/              Schema, calendar, generator, lending, repayment, risk, config, onboarding, checkpoint, runner, streaming, reconciliation and DB tests
migrations/         Postgres DDL
docker-compose.yml  Local Postgres stack
.github/workflows/  CI: Postgres service container, migrations, full test suite
```
