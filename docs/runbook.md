# Runbook

A synthetic-data platform for an SA SME fintech: merchant acquiring (card + instant EFT) and term lending with DebiCheck collection. Generates stateful, causally coherent event data written to a local Postgres OLTP store, ready for a CDC pipeline downstream.

This is not real data or a benchmark. Parameters come from the project's design documents where those give a value, and are marked illustrative in `config/` where they do not.

---

## Quick start

```bash
docker compose up -d
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
cp .env.example .env
docker exec -i fintech_pg psql -U sim -d fintech < migrations/001_initial_schema.sql
docker exec -i fintech_pg psql -U sim -d fintech < migrations/002_loan_repayments_fk.sql
docker exec -i fintech_pg psql -U sim -d fintech < migrations/003_loan_applications_replica_identity.sql
pytest tests/ -q -m "not slow"
python -m simulator.main --mode backfill --start-date 2025-01-01 --end-date 2026-01-01 --merchant-count 100
```

---

## Commands and their optional flags

Run all commands from the repo root with the venv active. `python -m ...` commands are the project's own. `scripts/...` tools run as `PYTHONPATH=. python scripts/NAME.py`. Google-facing commands read credentials from `GOOGLE_APPLICATION_CREDENTIALS`.

A test (`tests/test_readme_commands.py`) fails if any flag in the code is missing from this table, or if a flag listed here is not in the code. Every command also answers `--help`.

| Command | With no flag | Optional flags | Notes |
|---|---|---|---|
| `python -m simulator.main` | Requires `--mode` | `--mode` (required), `--start-date`, `--end-date`, `--duration-days`, `--merchant-count`, `--seed`, `--checkpoint`, `--resume`, `--speed-factor` | `--mode backfill` or `stream`. `--merchant-count` is onboarded, not active. `--end-date` exclusive, backfill only. `--resume` reads start date, count and seed from the checkpoint. |
| `python -m simulator.validation` | Runs all checks in `validate.sql` against `POSTGRES_DB` | `--only`, `--dsn` | `--only par` filters by check name (repeatable). `--dsn` points at another database. Reads only. |
| `python -m cdc.main` | Requires `--once` or `--follow` | `--once`, `--follow` | `--once` drains and exits. `--follow` polls every `CDC_POLL_INTERVAL_SECONDS`. Each object is one Class A operation: `--follow` against a stream is for short demos only. |
| `python -m cdc.reconcile` | Reads every landed object and checks it against the source database | `--directory`, `--workers` | `--directory DIR` reads from a local directory (no GCS cost). `--workers` controls parallel reads (default 8). |
| `python -m cdc.lag` | Prints one slot report | `--watch` | `--watch N` prints one line every N seconds. Asks Postgres only; no GCS cost. |
| `python -m cdc.permissions` | Signs in as the credentials file's account and checks bucket permissions | `--landing-bucket`, `--bronze-bucket`, `--project` | Defaults to the CDC settings. Metadata call only; writes nothing. |
| `python -m bronze.check` | Reads every landed object, checks every event, maps to Bronze rows. Writes nothing. **Proves events are valid; does not prove Arrow conversion.** | `--roundtrip`, `--newest-marker` | **`--roundtrip` also proves the Arrow conversion.** ~2x the GCS cost. `--newest-marker` prints the newest run marker's id and stops (one listing, one read, no landed object); the DAG uses it to skip a check the landing does not need. Cannot combine with `--roundtrip`. |
| `python -m bronze.main` | **Plan only**: reads and checks everything, prints what a write would do. | `--write`, `--rebuild-catalog` | **`--write` does the work.** `--rebuild-catalog` reconstructs a lost local catalog from the GCS run marker; cannot combine with `--write`. |
| `python -m bronze.verify` | **Full proof**: Bronze against landed objects and against the source database. Reads only. | `--landing-only` | **`--landing-only` skips the source comparison and is NOT full proof.** Use only on a machine with no Postgres. |
| `python -m bronze.orphans` | **Lists only**: orphaned files, missing files, marker/catalog agreement. | `--delete`, `--min-age-hours` | **`--delete` removes files permanently; soft delete is off.** Default `--min-age-hours` is 24. `--min-age-hours 0` deletes however young: only when nothing is writing. |
| `scripts/register_bronze_bq.py` | Reads the newest run marker and executes `CREATE OR REPLACE EXTERNAL TABLE` for all five tables | `--connection`, `--project`, `--location`, `--dry-run` | `--connection` is required. **`--dry-run` prints DDL without executing.** Run with your own login (ADC), not `bronze-writer`. |
| `scripts/check_bq_schema.py` | Requires `--project`; checks all five BigQuery external Bronze tables against the Bronze schema contract | `--project`, `--dataset`, `--location` | `--project` is required. `--dataset` defaults to `bronze`; `--location` defaults to `us-east1`. Reads table metadata only. |
| `scripts/boosted_backfill.py` | 300-merchant backfill, 2025-01-01 to 2027-01-01, application propensity x5, into `POSTGRES_DB` | `--db`, `--seed`, `--merchants`, `--start-date`, `--end-date`, `--boost`, `--checkpoint` | `--db` keeps it away from `fintech`. For lending checks, not a demo scenario. |
| `scripts/capture_pgoutput_fixtures.py` | Captures raw pgoutput messages and rewrites `tests/fixtures/pgoutput.json` | none | Run only when fixtures need to change. Needs `wal_level=logical`. |
| `scripts/measure_bronze_scale.py` | Requires `--work` | `--work`, `--transactions`, `--batch-rows`, `--sample-seconds` | Local warehouse and landing only; no GCS. ~80 min at the default 11M-event size. |
| `scripts/measure_decode_speed.py` | Times the CDC decoder on the slot without consuming it | `--slot`, `--changes`, `--repeat` | Nothing is written or consumed. The slot must hold at least `--changes` messages. |
| `scripts/measure_upload_latency.py` | Uploads 30 objects to the landing bucket, reads 5 back and deletes them | `--bucket`, `--prefix`, `--objects`, `--workers`, `--reads`, `--project` | Run with your own login (cdc-consumer cannot delete). ~61 Class A at the default. |
| `scripts/portfolio_check.py` | Runs 120 merchants for 540 days in memory and prints portfolio quality | `--merchants`, `--days`, `--boost`, `--seed` | Takes minutes. Writes no database. |
| `scripts/rehearse_bronze.py` | Requires `--work` | `--work`, `--database`, `--slot`, `--merchants`, `--seed` | Full end-to-end sandbox rehearsal. All three `--work`, `--database` and `--slot` must be new names. |
| `scripts/storage_request_counts.py` | Counts the month's GCS Class A and B operations from Cloud Monitoring | `--project`, `--start`, `--first-end`, `--end`, `--planned-a` | Run with your own login, `GOOGLE_APPLICATION_CREDENTIALS` unset. Read only. **`--planned-a N`** shows headroom after N more Class A operations. |
| `scripts/cloud_crash_run.py` | Requires `--work`; also refuses if `BRONZE_BUCKET` is `fintech_bronze` | `--work` | G4 crash tests against the throwaway bucket. **Never run with `BRONZE_BUCKET=fintech_bronze`.** |

---

## Running a backfill

Backfill advances one simulated day at a time and writes each day to Postgres in one transaction.

**Key rules:**
- End date is exclusive: 2025-01-01 to 2026-01-01 is 365 days
- `--merchant-count` is onboarded, not active. About a third never transact
- A fresh run refuses a non-empty `merchants` table or an existing checkpoint
- `--resume` continues from `checkpoint.json`, using the start date, count and seed stored in it
- A resume under different `ENABLE_RISK_EVENTS` or `ENABLE_LOAN_APPLICATIONS` values is refused

**Reading the Churned count.** The Churned count looks high and is meant to. In the 1,000-merchant run, 486 of 1,185 onboarded merchants ended Churned. About a third of signups never activate and are marked Churned 30 days after onboarding. Churned is terminal, so the count is a running total, not a rate. The comparable rate is churn after trading. The run summary prints "never activated" and "churned after trading" separately.

**Timing:** 1,000 merchants over 455 days took 3,337 seconds (~56 min) on a 2017 MacBook Air. A 100-merchant year takes 183 seconds in the sandbox or 476 seconds on the Air.

---

## Running a stream

Stream replays the same day as backfill in timestamp order, sleeping the gap between events divided by `STREAMING_SPEED_FACTOR` (default 3,600, so one day takes ~24 seconds).

```bash
python -m simulator.main --mode backfill --start-date 2025-01-01 --end-date 2025-06-01 --merchant-count 50 --checkpoint demo.json
python -m simulator.main --mode stream --duration-days 1 --checkpoint demo.json
```

The demo above backfills first because a fresh stream has no transactions for its first few days (merchants activate 1 to 30 days after onboarding).

**Notes:**
- The streaming writer commits every row: ~900 rows/second
- At 1,000 merchants, a day is ~43,000 rows (~1,800 rows/second at 3,600x), which falls behind schedule. The writer logs a lag warning and drops nothing
- 50 merchants stays on schedule: 1,181 transactions in 24 seconds

---

## CDC setup

The CDC consumer reads a separate database (`fintech_cdc`) and never touches `fintech`.

### Key settings

- **Migration 003** sets `REPLICA IDENTITY FULL` on `loan_applications` (the only updated table). Updates then carry the full old row. The other four tables are insert-only.
- **`max_slot_wal_keep_size`** in `docker-compose.yml` caps WAL retention. A slot that exceeds the cap is invalidated and cannot be recovered.
- **Settings** are in `cdc/config.py` (`CdcSettings`, `CDC_` prefix, mirrored in `.env.example`).

### How the decoder works

`cdc/pgoutput.py` turns binary pgoutput messages into typed Python values. `cdc/types.py` maps column types (timestamps to UTC with 6 fractional digits, `NUMERIC` as a string, arrays as lists). Any unmapped type, unchanged TOAST marker, or unrecognised message raises an error that stops the consumer without confirming the batch.

`cdc/envelope.py` builds the change event. Each event carries: source table, commit LSN, change LSN, commit time, event timestamp, event date in SAST, position in the transaction, transaction size, and an envelope version.

`cdc/columns.py` checks every Relation message against the Pydantic model in `simulator/models.py`. A schema change stops the consumer before any events land.

### First-time setup

The slot must exist before data is written. Create it once:

```bash
docker compose up -d --force-recreate
docker exec fintech_pg psql -U sim -d fintech -c "CREATE DATABASE fintech_cdc OWNER sim"
for f in migrations/*.sql; do docker exec -i fintech_pg psql -U sim -d fintech_cdc -v ON_ERROR_STOP=1 < "$f"; done
docker exec fintech_pg psql -U sim -d fintech_cdc -c "SELECT pg_create_logical_replication_slot('cdc_r01', 'pgoutput')"
```

`--force-recreate` replaces the container to pick up updated flags. Never add `-v`; that deletes the data volume.

Point the generator at the CDC database:

```bash
POSTGRES_DB=fintech_cdc python -m simulator.main --mode backfill --start-date 2025-01-01 --end-date 2025-02-15 --merchant-count 100 --checkpoint checkpoint_cdc.json
```

### Inspect the slot (non-consuming)

```bash
docker exec fintech_pg psql -U sim -d fintech_cdc -c "SELECT chr(get_byte(data, 0)) AS message, count(*) FROM pg_logical_slot_peek_binary_changes('cdc_r01', NULL, 5000, 'proto_version', '1', 'publication_names', 'fintech_cdc') GROUP BY 1 ORDER BY 1"
docker exec fintech_pg psql -U sim -d fintech_cdc -c "SELECT slot_name, wal_status, pg_size_pretty(safe_wal_size) AS safe, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots"
```

Message types: `B`/`C` (begin/commit), `R` (column list), `I` (insert), `U` (update). The 5,000 is a lower bound; a transaction is never cut.

To drop a slot: `SELECT pg_drop_replication_slot('cdc_r01')`. The cluster allows four slots.

---

## Running the CDC consumer

```bash
GOOGLE_APPLICATION_CREDENTIALS=/Users/you/.config/gcloud_cdc/application_default_credentials.json python -m cdc.main --once
GOOGLE_APPLICATION_CREDENTIALS=/Users/you/.config/gcloud_cdc/application_default_credentials.json python -m cdc.main --follow
```

### Credentials

The consumer runs as `cdc-consumer` through an impersonated credentials file. It refuses to start (exit 3) without `GOOGLE_APPLICATION_CREDENTIALS` set. Create the file once:

```bash
CLOUDSDK_CONFIG=$HOME/.config/gcloud_cdc gcloud auth application-default login \
  --impersonate-service-account=cdc-consumer@payments-512910.iam.gserviceaccount.com \
  --project=payments-512910
```

This stores a refresh token (not a key) outside the repo. Run `python -m cdc.permissions` to verify the account has the right GCS access before running the consumer.

### Who runs what

| Command | Runs as |
|---|---|
| `cdc.main`, `cdc.permissions` | `cdc-consumer` |
| `cdc.reconcile`, `bronze.check` | `bronze-writer` or your own login |
| `cdc.lag` | No Google login needed |
| `scripts/measure_upload_latency.py` | Your own login (cdc-consumer cannot delete) |

### How batching works

A batch is at least `CDC_BATCH_SIZE` (10,000) messages. Each batch produces one object per table per event date, named `cdc/<slot>/<table>/event_date=YYYY-MM-DD/lsn_<first>_<last>.ndjson.gz`. Objects upload `CDC_UPLOAD_WORKERS` (8) at a time. The slot is confirmed only after every object in the batch has landed.

### Replays

A replay builds the same object names from the same events, but `ts_ms` and `ingested_at` differ. When a name is taken, the consumer reads the existing object and compares events (excluding those two fields). Equal means already landed. Different stops the run (exit 2). Nothing is overwritten. Dedup downstream on `(source.commit_lsn, transaction.position)`.

### Slot monitoring

The consumer warns when WAL headroom falls under half the cap and again under a tenth. A lost slot stops the run (exit 4). A lost slot cannot be repaired: start a new slot (`cdc_r02`) with a fresh database and checkpoint.

### Exit codes

| Exit | Meaning |
|---|---|
| 0 | Drained, or stopped cleanly with Ctrl-C |
| 1 | Lost database connection, missing slot, usage error, or bug |
| 2 | Contract stop: delete/truncate, schema drift, undecodable message, or existing object with different events |
| 3 | Bucket failed after retries, or `GOOGLE_APPLICATION_CREDENTIALS` not set |
| 4 | Slot is lost |
| 130 | Second Ctrl-C |

---

## Watching the slot

```bash
python -m cdc.lag
python -m cdc.lag --watch 10
```

Asks Postgres only. No Google calls, no cost. Prints: slot position, server WAL position, lag, WAL held, cap, headroom, and a level (healthy / under half / under a tenth / unreserved / lost).

**Behind vs held:** behind is all WAL since the slot's confirmed position (including other databases). Held is what the slot keeps from its restart position, which Postgres moves forward some time after a confirm. Held can stay above behind for a while after a drain.

Run `cdc.lag` before every large operation. A slot that has sat unread since creation is already holding WAL.

---

## Larger runs: chunking

A 100-merchant year is ~1M changes: small enough to run the generator and consumer alongside each other without losing the slot. A 1,000-merchant run produces ~8.95M changes and needs chunking.

For much larger runs (e.g., 1,000 merchants, ~11M transactions), run the generator in chunks and let the consumer drain between them:

```bash
POSTGRES_DB=fintech_cdc python -m simulator.main --mode backfill --start-date 2025-01-01 --end-date 2025-05-01 --merchant-count 1000 --checkpoint checkpoint_cdc.json
python -m cdc.lag --watch 10
POSTGRES_DB=fintech_cdc python -m simulator.main --mode backfill --end-date 2025-09-01 --resume --checkpoint checkpoint_cdc.json
```

Start the next chunk when `behind` in the lag report is close to zero. If headroom falls under half the cap while generating, stop, let the consumer catch up, then `--resume`.

---

## Reconciling a run

```bash
python -m cdc.reconcile
python -m cdc.reconcile --directory DIR
```

Checks whether the landed objects add up to the source database. Reads every object and verifies:

1. Each object matches its name (table, event date, first and last commit LSN)
2. Every transaction is whole (no missing positions)
3. Landed inserts match source rows; landed updates match the disbursed loans
4. Events replayed into a blank database reproduce every source table (by digest)

**Requirements:**
- The source must be quiet and the consumer drained (`python -m cdc.main --once` must say "nothing was waiting")
- The replay creates a scratch database on the source server and drops it at the end

**Exit codes:** 0 (RECONCILED), 2 (PROBLEM lines printed), 3 (bucket failure or missing credentials), 1 (other).

**Cost against the bucket:** one listing (Class A) and one read per object (Class B).

---

## A full run from start to finish

Three terminals:

```bash
GOOGLE_APPLICATION_CREDENTIALS=/Users/you/.config/gcloud_cdc/application_default_credentials.json CDC_DB=fintech_cdc CDC_SLOT_NAME=cdc_r01 python -m cdc.main --follow
```

```bash
POSTGRES_DB=fintech_cdc python -m simulator.main --mode backfill --start-date 2025-01-01 --end-date 2026-01-01 --merchant-count 100 --checkpoint checkpoint_cdc.json
```

```bash
CDC_DB=fintech_cdc CDC_SLOT_NAME=cdc_r01 python -m cdc.lag --watch 10
```

Start the consumer first. When the generator finishes, wait until `behind` is near zero, Ctrl-C once in the consumer terminal, then:

```bash
GOOGLE_APPLICATION_CREDENTIALS=/Users/you/.config/gcloud_cdc/application_default_credentials.json CDC_DB=fintech_cdc CDC_SLOT_NAME=cdc_r01 python -m cdc.main --once
CDC_DB=fintech_cdc CDC_SLOT_NAME=cdc_r01 python -m cdc.reconcile
```

`--once` should say "nothing was waiting". Reconcile should end in `RECONCILED`.

**Measured on the 1,000-merchant run:**

| Step | Result |
|---|---|
| Generator | 3,337 s, 8,537,090 rows (213,923 merchant snapshots, 8,321,600 transactions, 293 loan applications (158 disbursed), 523 repayments, 593 risk events) |
| Consumer | 8.95M changes across two runs (WiFi interruption mid-way); slot stayed healthy throughout |
| Landed | 2,610 objects in gs://cdc_landing/cdc/cdc_r03/ |
| Bronze write | 8.5M events written from scratch via Airflow, 124 min |

A repeat run needs a new slot name (`cdc_r02`), a fresh database and a new checkpoint.

---

## Bronze: checking the landing

```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/bronze_credentials.json python -m bronze.check
GOOGLE_APPLICATION_CREDENTIALS=/path/to/bronze_credentials.json python -m bronze.check --roundtrip
```

Reads every landed object, checks every event against the contract, and maps every event to its Bronze row. **Writes nothing.**

`--roundtrip` adds a second pass: converts each event to Arrow, reads it back, and requires it to equal the original. A pass with `--roundtrip` proves the Arrow conversion. Without it, conversion is not checked.

**Exit codes:** 0 (all events valid), 2 (contract stop, with the object and field named), 3 (bucket failure), 1 (other).

**Cost (1,000-merchant run):** ~1 Class A + ~2,610 Class B. With `--roundtrip`: ~2 Class A + ~5,220 Class B.

---

## Bronze: writing

```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/bronze_credentials.json python -m bronze.main
GOOGLE_APPLICATION_CREDENTIALS=/path/to/bronze_credentials.json python -m bronze.main --write
GOOGLE_APPLICATION_CREDENTIALS=/path/to/bronze_credentials.json python -m bronze.main --rebuild-catalog
```

**Without `--write`:** plan only. Reads every landed object, checks existing tables, reads existing keys, and prints what a write would do. Nothing is written to the bucket.

**`--write`:** does the work in this order:
1. Checks every existing Bronze table against the contract
2. Pass 1 (builds the key index)
3. Reads existing keys from each table
4. Creates any missing tables
5. Appends per table in batches of `BRONZE_BATCH_ROWS` rows, one Iceberg commit per batch
6. Writes the run marker to `_runs/`
7. Copies the catalog to `_catalog/catalog.db`

A second `--write` over the same landing writes nothing. A failure mid-way leaves committed batches; running again continues from there.

**`--rebuild-catalog`:** registers the five tables in an empty local catalog from the newest run marker, after the catalog file was lost. Cannot be combined with `--write`.

**Exit codes:** 0 (done or nothing to do), 2 (contract stop), 3 (bucket failed), 130 (Ctrl-C). Every non-zero exit prints one `error:` line.

**Cost (first write, 1,000-merchant run):** ~190 to 220 Class A + ~2,610 Class B.

---

## Bronze: verifying

```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/bronze_credentials.json python -m bronze.verify
GOOGLE_APPLICATION_CREDENTIALS=/path/to/bronze_credentials.json python -m bronze.verify --landing-only
```

**A pass of the plain command is the full proof.** It checks:
1. Bronze holds every landed event (checked against the landing)
2. No key is held twice
3. Bronze replayed into a blank database equals the source database in all five tables (checked against `fintech_cdc`)

**Before the first Google call**, verify checks: credentials, that the local catalog holds five tables, and that the source database is reachable. A failure stops the run with no GCS operations spent.

**`--landing-only`** skips the source comparison. Its result says `SOURCE COMPARISON NOT RUN, THIS IS NOT FULL PROOF`. Use only on a machine with no Postgres.

**Exit codes:** 0 (verified), 2 (mismatch or source changed during check), 3 (bucket failure or source unreachable), 1 (other).

**Cost (1,000-merchant run):** ~1 Class A + ~2,610 Class B (landing) + Bronze table reads.

**G4/G5 evidence (2026-10-31):** `python -m bronze.verify` passed four times on the real bucket with identical ten-way digests. All nine GCS phase acceptance criteria are met on evidence.

---

## Bronze: orphan cleanup

```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/bronze_credentials.json python -m bronze.orphans
GOOGLE_APPLICATION_CREDENTIALS=/path/to/bronze_credentials.json python -m bronze.orphans --delete
```

A crashed write leaves data files, manifests, or manifest lists that nothing refers to. The command lists them and optionally deletes them.

**Without `--delete`:** lists orphans, missing files, and whether the marker and catalog copy agree. Reports both directions: orphans in the bucket AND files the tables use that are missing.

**`--delete`:** removes files only when all of these hold:
- The file is under `bronze/TABLE/data/` or `bronze/TABLE/metadata/`
- Nothing uses it
- It is at least `--min-age-hours` old (default 24, so an in-progress write is never touched)
- The whole check is clean

The list is printed before anything is deleted. Read it first. **Soft delete is off: a deleted file is gone.**

**Exit codes:** 0 (clean), 2 (something wrong: data loss, catalog behind marker, unexpected object), 3 (bucket failure), 1 (other).

---

## Troubleshooting

### `bronze.verify`

| Symptom | Action |
|---|---|
| Exit 2 "NOT PROVEN: source changed" | Stop generator and consumer. Run `python -m cdc.lag`. Run verify again when nothing is writing. |
| Exit 2 "NOT VERIFIED": digest mismatch | Read the PROBLEM lines. Do not write to either bucket; do not delete anything; investigate. |
| Exit 3 (source) | Check Docker is running. Confirm `CDC_DB=fintech_cdc`. Run `python -m cdc.lag`. |
| Exit 3 (bucket) | Check `GOOGLE_APPLICATION_CREDENTIALS`. Retry. |
| Exit 1 (catalog missing a table) | Run `python -m bronze.main --rebuild-catalog`. |

### `bronze.orphans`

| Symptom | Action |
|---|---|
| Exit 2 "catalog BEHIND the marker" | Run `python -m bronze.main --rebuild-catalog` (must exit 0), then run orphans again. |
| Exit 2 "files a table uses are not in the bucket" | Stop all writes. Do not delete anything. Investigate. |
| Exit 2 "unexpected object" | Read the list. Never run with `--delete` until a run without it exits 0. |

### `bronze.main`

| Symptom | Action |
|---|---|
| Exit 2 "table drifted" | A Bronze table differs from the current schema. No ALTER TABLE path; needs a new contract version. Do not write until an upgrade path is designed. |
| Exit 2 "catalog behind marker" | Run `--rebuild-catalog`. |
| Exit 2 "pass 2 did not match pass 1" | The landing changed during the run. Run again when nothing is landing. |
| Exit 3 (bucket failed during write) | Error says how many batches committed. Run again; it continues from there. |

---

## GCS free-tier limits

5,000 Class A (writes, listings) and 50,000 Class B (reads) per billing month in `us-east1`. Usage pools across buckets in the project.

**Approximate cost per command (1,000-merchant run, 2,610 objects):**

| Command | Class A | Class B |
|---|---|---|
| `bronze.verify` | ~1 | ~3,500 |
| `bronze.verify --landing-only` | ~1 | ~2,610 |
| `bronze.check` | ~1 | ~2,610 |
| `bronze.check --roundtrip` | ~2 | ~5,220 |
| `bronze.orphans` | ~1 | ~20 to 50 |
| `bronze.main` (plan) | ~3 | ~2,610 |
| `bronze.main --write`, nothing new since the newest run marker | ~3 | ~1 to 10 (marker and table metadata; no landed object is read) |
| `bronze.main --write` (new data to append) | ~190 to 220 | ~2,610 (pass 1) plus the new objects again (pass 2) |
| `bronze.main --rebuild-catalog` | ~1 | ~300 |

A `--write` run skips reading the landing when nothing is new: the tables are at the newest run marker's snapshots, the listing holds the marker's number of objects, and the highest `last_lsn` in the object names is the marker's `highest_commit_lsn`. It trusts that landed objects are never overwritten. Any doubt (a count or LSN that differs, a table ahead of the marker) does the full run. A plan (no `--write`) and `bronze.check` always read and check every object, so use them when you want the full proof.

In the DAG, `bronze_verify` runs `bronze.check` only when the newest run marker has not already passed it (`scripts/verify_unless_unchanged.sh`, the Airflow Variable `bronze_last_verified_marker`). Otherwise the task is skipped and reads no landed object. `python -m bronze.check --newest-marker` prints the newest marker's id for one listing and one read. Trigger the DAG with `force_verify` set to true to check every object whatever is recorded.

**Check the month's standing:**
```bash
env -u GOOGLE_APPLICATION_CREDENTIALS PYTHONPATH=. python scripts/storage_request_counts.py
```

**When Class A headroom is low:** use `bronze.check` without `--roundtrip`. Defer `bronze.verify` to the next billing month.

---

## Infrastructure (Terraform)

GCP resources are managed in `infra/`: the two data buckets, service accounts, and IAM bindings. See [infra/README.md](../infra/README.md) and [docs/decisions/0001-terraform-for-the-gcp-infrastructure.md](decisions/0001-terraform-for-the-gcp-infrastructure.md).

**Service account permissions:**

| Principal | `cdc_landing` | `fintech_bronze` |
|---|---|---|
| `cdc-consumer` | create + read back | none |
| `bronze-writer` | read | read + write + delete |

Both accounts use impersonation (refresh token, no key files). Both refuse to start without `GOOGLE_APPLICATION_CREDENTIALS` set.

CI runs `terraform fmt -check`, `terraform init -backend=false`, and `terraform validate` on every push. No credentials, no plan.

---

## BI layer (Lightdash)

**Live project:** https://app.lightdash.cloud/projects/96ca0acd-484c-4428-b1b7-4f5affb25d83/home

### Dashboards

| Dashboard | Models | What it shows |
|---|---|---|
| GPV Overview | `fct_payments_daily` | Monthly GPV, take rate, GPV by archetype |
| Portfolio Quality | `fct_loan_portfolio_daily`, `fct_vintage_curves` | PAR30/60/90, portfolio size, vintage curves |
| Settlement Reconciliation | `fct_settlement_daily` | Gross GPV vs net payout, acquirer revenue |
| Revenue | `fct_revenue_daily` | Revenue by month and stream |
| Merchant Health | `fct_merchant_health_daily` | Flagged merchants, risk score, GPV-declining merchants |
| Merchant Monthly Statement | `fct_merchant_monthly_summary` | Monthly GPV, loan balance, collections |
| Lending Operations | `fct_loan_applications_daily`, `fct_collections_performance_daily`, `fct_loan_unit_economics` | Application funnel, collection rates, net lending margin |
| Risk and Fraud | `fct_risk_events_daily` | Risk events by rule type, blocks vs flags |

### Making a change

```bash
cd dbt && npx @lightdash/cli@latest deploy
npx @lightdash/cli@latest upload
npx @lightdash/cli@latest download
git add lightdash/ && git commit -m "..."
```

Dashboard content is version-controlled under `lightdash/`. Always run `lightdash download` after any UI change and commit the result.

---

## Orchestration (Airflow)

See [docs/orchestration.md](orchestration.md) for the full Airflow runbook.

**Quick reference:**

```bash
cp airflow/.env.example airflow/.env
docker compose -f airflow/docker-compose.yml up airflow-init
docker compose -f airflow/docker-compose.yml up -d webserver scheduler
```

UI: `http://localhost:8080` (admin / admin).

```bash
docker compose -f airflow/docker-compose.yml exec scheduler airflow dags trigger fintech_pipeline
docker compose -f airflow/docker-compose.yml exec scheduler airflow dags pause fintech_pipeline
docker compose -f airflow/docker-compose.yml exec scheduler airflow dags unpause fintech_pipeline
```

---

## Validation

```bash
python -m simulator.validation
python -m simulator.validation --only par
psql -h localhost -U sim -d fintech -f validate.sql
```

`validate.sql` holds the reconciliation queries. Each returns a value next to its Validation Targets band.

**Notes:**
- Results vary by seed and run size. Read across several seeds and larger runs for stable figures.
- PAR is computed two ways: from recorded `days_past_due` (generator-consistent) and from instalment age (what a table query alone gives). The instalment-age measure runs ahead between monthly arrears cycles
- For lending checks, use `scripts/boosted_backfill.py` (default 5x application propensity over 24 months)
- `tests/test_reconcile.py` asserts the seven failure modes and plants each fault to confirm the query fires

---

## Deviations from source documents

Where the design documents disagreed or produced numbers that could not all hold, the choice was recorded. Each amended band has a guarding test.

**Bands amended:** gross take rate (1.9 to 2.4%), annual write-off rate (15 to 25%), E-commerce evening share (21 to 24%), EFT funnel completion (62 to 70%), OTP share of failures (34 to 42%), January vs December (27 to 40% below), Black Friday (1.3 to 1.9x an average Friday).

**Causal Rules over Simulator Design** for the weekday, hourly and seasonal tables of four archetypes, Spaza trading on Sunday, and Black Friday lifting ticket size only after the cap.

**State machine additions** not in Causal Rules: New to Churned, Delinquent to Stressed, never-activating merchants becoming Churned 30 days after onboarding, and transition probabilities applied as daily hazards (not per-day rolls).

**Collections:** retry success is 0.2063 per attempt. Three retries at the first-strike rate would recover 99%, not ~50%.

---

## Project structure

```
simulator/        Core simulation modules
cdc/              CDC consumer: decoder, envelope, reader, GCS writer, lag report, reconciler
bronze/           Bronze writer: check, main, verify, orphans commands
config/           YAML calibration, archetypes, fees, causal rules
scripts/          Measurement and rehearsal tools
validate.sql      Reconciliation queries against Validation Targets
tests/            Full test suite
migrations/       Postgres DDL (001 schema, 002 FK, 003 replica identity)
infra/            Terraform for GCP resources
docs/             Deep-dive documentation
lightdash/        Dashboard and chart YAML (content-as-code)
dbt/              dbt project (staging, intermediate, dimensions, marts)
dags/             Airflow DAG
airflow/          Airflow Docker Compose stack
docker-compose.yml  Local Postgres stack
.github/workflows/  CI
```
