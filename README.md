# fintech-payments

A synthetic-data platform for an SA SME fintech: merchant acquiring (card + instant EFT) and term lending with DebiCheck collection. Generates stateful, causally coherent event data written to a local Postgres OLTP store, ready for a CDC pipeline downstream.

This is not real data, a model fitted to any company's books, or a benchmark. Parameters come from the project's design documents where those give a value, and are marked illustrative in `config/` where they do not. The largest run onboarded 1,000 merchants over 15 months (1,251 with replacements, about 700 not churned at the end), which is the low-to-middle of the design target of 500 to 1,000 active merchants. Loan counts at the default application rate are small: that run originated 205 loans.

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
| CDC | | Python `pgoutput` consumer (`cdc/`), landing gzipped NDJSON in GCS |
| Lakehouse | | Iceberg on GCS (read side, schemas and mapping built, writing next) |
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
docker exec -i fintech_pg psql -U sim -d fintech < migrations/003_loan_applications_replica_identity.sql

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
- The run prints merchants onboarded, merchants that ever transacted, the final merchant states, churn split in two (see below) and the rows written.

**Reading churn.** A run's Churned count looks high, and is meant to. In the 100-merchant year (seed 42) 50 of 115 onboarded merchants end Churned, but 42 of those never transacted. About a third of signups never activate (`activation_share` 0.67, an illustrative value with no source document) and are marked Churned 30 days after onboarding, which models the drop-off between signing up and taking a first payment. Replacement merchants are drawn the same way, so they add more. Churned is terminal and merchants stay in the list, so the Churned count over everything onboarded is a running total, not a churn rate. The rate that compares with real merchant attrition is churn after trading: 8 of the 72 merchants that transacted in that run, about 1.4 to 2.2 percent a month over seeds 42 to 44 (the validation band is 2 to 5 percent). The summary therefore prints "never activated" and "churned after trading" separately, and `validate.sql` already leaves never-activators out of its monthly churn check. Any model built on this data should do the same.

A 100-merchant, 365-day backfill took 183 seconds in the sandbox. A 1,000-merchant backfill over 455 days (1,251 merchants onboarded, 11.0 million transactions, 279,000 snapshot rows) took 3,337 seconds, about 56 minutes, including a checkpoint after every simulated day. A laptop is slower: the 100-merchant run took 476 seconds on a 2017 MacBook Air.

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

## CDC setup (V2)

The CDC work reads the write-ahead log of a separate database, `fintech_cdc`, so the data in `fintech` is never touched. The plan is in the project's Notion workspace (CDC Plan (V2)); this section covers the database, the slot, the decoder and the change-event envelope, and "Running the CDC consumer" below covers the consumer that lands the events in GCS.

- **Migration 003** sets `REPLICA IDENTITY FULL` on `loan_applications`, the only table the generator updates (the disbursement). An update then carries the old row as well as the new one. The other four tables are insert-only and keep the default.
- **`max_slot_wal_keep_size=3GB`** in `docker-compose.yml` caps how much WAL an unread slot can pin. An unread slot grows the disk until it fills, and a slot that passes the cap is invalidated and cannot be recovered. About 750 bytes of WAL a row means 3 GB is roughly 35 to 40 minutes of consumer downtime during a backfill on a 2017 MacBook Air.
- **Settings** are in `cdc/config.py` (`CdcSettings`, `CDC_` prefix, mirrored in `.env.example`). The consumer has its own connection fields, separate from the generator's `POSTGRES_` ones.
- **The decoder** is `cdc/pgoutput.py`: it turns the binary `pgoutput` messages (begin, relation, insert, update, delete, truncate, commit) into typed messages, one at a time, with the column values mapped by `cdc/types.py` to the JSON values the change-event contract specifies (timestamps in UTC with six fractional digits, `NUMERIC` as a string, arrays as lists). Anything it cannot decode exactly, such as an unmapped column type, an unchanged TOAST marker or a truncated message, raises an error that stops the consumer without confirming the batch (`cdc/errors.py`). `require_supported()` additionally refuses deletes, truncates and updates that lack the full old row. `cdc/lsn.py` converts LSNs between Postgres text and integers.
- **The change event** is built by `cdc/envelope.py`. `Envelope` is the contract as code: every field in the contract's order, validated on construction, so an event that breaks the contract cannot be built. `build_events()` turns one transaction into its events, adding the source (database, table, commit LSN, the change's own LSN, commit time), the position in the transaction and its size, `event_ts` and `event_date` (the row's own event time, as a South African date; a disbursement update keeps its insert's date, and an unpaid repayment with no payment time falls back to its due date), and `ts_ms` and `ingested_at`. `to_line()` writes one compact, ASCII-only NDJSON line. `cdc/transactions.py` cuts the message stream into whole transactions, applying the stop rules on the way, and `cdc/columns.py` compares every Relation message with its model in `simulator/models.py` (column names, their order and each column's Postgres type), so a schema change stops the consumer instead of landing wrong data.
- **Decoder tests** run on raw messages captured from a real Postgres 16 slot (`tests/fixtures/pgoutput.json`, rewritten by `PYTHONPATH=. python scripts/capture_pgoutput_fixtures.py`, which needs a Postgres with `wal_level=logical` and uses a scratch database), so they need no database. `scripts/measure_decode_speed.py --slot cdc_r01` times the decoder on the messages waiting in a slot without consuming them.

Set up the database and the first slot once. The slot must exist before any data is written, so every row arrives as a normal insert and no snapshot phase is needed.

```bash
docker compose up -d --force-recreate
docker exec fintech_pg psql -U sim -d fintech -c "CREATE DATABASE fintech_cdc OWNER sim"
for f in migrations/*.sql; do docker exec -i fintech_pg psql -U sim -d fintech_cdc -v ON_ERROR_STOP=1 < "$f"; done
docker exec fintech_pg psql -U sim -d fintech_cdc -c "SELECT pg_create_logical_replication_slot('cdc_r01', 'pgoutput')"
```

`--force-recreate` replaces the container so it picks up the new flag. It keeps the `postgres_data` volume; never add `-v` or run `docker compose down -v`, which deletes the data.

Point the generator at the CDC database with its own checkpoint (a fresh run refuses a non-empty `merchants` table or an existing checkpoint):

```bash
POSTGRES_DB=fintech_cdc python -m simulator.main --mode backfill --start-date 2025-01-01 --end-date 2025-02-15 --merchant-count 100 --checkpoint checkpoint_cdc.json
```

Look at what the slot holds, without consuming it, and how much WAL it pins:

```bash
docker exec fintech_pg psql -U sim -d fintech_cdc -c "SELECT chr(get_byte(data, 0)) AS message, count(*) FROM pg_logical_slot_peek_binary_changes('cdc_r01', NULL, 5000, 'proto_version', '1', 'publication_names', 'fintech_cdc') GROUP BY 1 ORDER BY 1"
docker exec fintech_pg psql -U sim -d fintech_cdc -c "SELECT slot_name, wal_status, pg_size_pretty(safe_wal_size) AS safe, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots"
```

The first query returns message types: `B` and `C` (begin and commit), `R` (a table's column list), `I` (insert) and `U` (update). The 5,000 is a lower bound on changes, because a transaction is never cut. An unread slot pins WAL until something reads it, so do not leave one idle through a large backfill. To abandon a run, drop its slot with `SELECT pg_drop_replication_slot('cdc_r01')`. Slot names are cluster-wide and the cluster allows four.

The CDC tests (`tests/test_cdc_foundation.py` and later files) need Postgres with `wal_level=logical`, which the compose file sets. They skip when Postgres is unreachable and fail when it is reachable but not logical, or when `CI` is set and it is unreachable.

The lost-slot tests (`tests/test_cdc_lost_slot.py`) need a Postgres they can lose a slot on, and the compose container's 3 GB cap is too large and cannot be changed from SQL, so they start a throwaway cluster with a 16 MB cap from local Postgres binaries (`initdb` and `pg_ctl`, found on the `PATH` or under `/usr/lib/postgresql`), on a free port in a temporary directory, and delete it afterwards. Docker is not used and no data of yours is touched. Without the binaries, which a Mac running only Docker does not have, those tests skip; under CI they fail instead. The cluster's Postgres version is the local binaries' version, not the container's, and the test run prints it.

## Running the CDC consumer

The consumer reads the slot and lands the changes in a GCS bucket as gzipped NDJSON, one object per table and event date, confirming the slot only after the objects have landed.

```bash
python -m cdc.main --once
python -m cdc.main --follow
```

`--once` drains what is waiting and exits. `--follow` drains, then looks again every `CDC_POLL_INTERVAL_SECONDS` (60) until Ctrl-C. Exactly one of the two is required. Run it alongside a backfill (a second terminal), with the generator pointed at the same database and the consumer's `CDC_DB` and `CDC_SLOT_NAME` matching it.

- **Credentials.** Google application-default credentials: `gcloud auth application-default login`, then `gcloud auth application-default set-quota-project <project>`. Never a key file; nothing is read from the repo. The bucket is `CDC_GCS_BUCKET` (`cdc_landing`).
- **What it does.** A batch is whole transactions, at least `CDC_BATCH_SIZE` messages (10,000; Postgres counts begin, commit and relation messages too). Each batch becomes one object per table and event date (the row's own event time, as a South African date), named `cdc/<slot>/<table>/event_date=YYYY-MM-DD/lsn_<first>_<last>.ndjson.gz`, with the lowest and highest commit LSN as 16 hex digits. The objects upload `CDC_UPLOAD_WORKERS` (8) at a time, each created only if the name is free. Only when every object has landed does it confirm the slot, to the `end_lsn` of the last transaction. A crash or a failed upload before that leaves the slot where it was, and the next run replays the batch.
- **Replays.** A replay builds the same names with the same events, but `ts_ms` and `ingested_at` are the consumer's clock, so the bytes differ. When a name is taken the consumer reads the existing object and compares the events without those two fields: the same events mean it already landed, anything else stops the run (exit 2) and nothing is overwritten. Downstream, dedup on `(source.commit_lsn, transaction.position)`; `cdc/dedup.py` has the helper.
- **Retries.** An upload or read that fails transiently (a timeout, a dropped connection, HTTP 408, 429 or 5xx) is retried up to 5 attempts, waiting 1, 2, 4 and 8 seconds, each attempt with a 60 second timeout. A permission or login error fails at once.
- **The slot.** Before every batch it checks `pg_replication_slots`. It warns once when the WAL headroom falls under half of the cap and again under a tenth, and when the slot is `unreserved`; a lost slot stops the run (exit 4). A lost slot cannot be repaired: regenerate the run with the same seed into a fresh database, a new slot name (`cdc_r02`) and so a new bucket prefix.
- **Ctrl-C.** The first one finishes the batch in hand (landed and confirmed), then exits 0. The second stops at once without confirming (exit 130); uploads already running still have to end before the process exits.

| Exit | Meaning |
|---|---|
| 0 | Drained, or stopped cleanly with Ctrl-C |
| 1 | Anything else: a lost database connection, a missing slot, a usage error, a bug (a bug also prints its traceback) |
| 2 | A contract stop: a delete or truncate, schema drift, an undecodable message, or an existing object with different events |
| 3 | The sink failed after its retries, or Google credentials are missing |
| 4 | The slot is lost |
| 130 | A second Ctrl-C |

Every non-zero exit prints one `error:` line to stderr, never prints a credential, and leaves the slot unconfirmed past what has landed.

- **Settings** are the `CDC_` variables in `.env.example`: `CDC_HOST`, `CDC_PORT`, `CDC_DB`, `CDC_USER`, `CDC_PASSWORD`, `CDC_SLOT_NAME`, `CDC_PUBLICATION`, `CDC_GCS_BUCKET`, `CDC_BATCH_SIZE`, `CDC_POLL_INTERVAL_SECONDS` and `CDC_UPLOAD_WORKERS`. The prefix in the bucket comes from the slot name, so a new slot name is a new prefix and runs never mix.
- **Changing the WAL cap.** `max_slot_wal_keep_size` is passed to Postgres as a `-c` flag in `docker-compose.yml`, and a flag overrides `ALTER SYSTEM`, so to change it edit the compose file and run `docker compose up -d --force-recreate` (the volume is kept).
- **The free tier.** The bucket is in us-east1, where the Always Free allowance (as summarised from Google's pricing page; check it) is 5 GB of storage and 5,000 Class A operations a month, and an upload is one. The number of objects has a floor of the distinct (table, event date) pairs, however large the batches: about 2 objects per simulated day, so the 45-day, 100-merchant run in "CDC setup" lands as 83 objects and a 100-merchant year as roughly 750 to 1,000. Streaming at the default 3,600x speed lands about 315 objects an hour at the default poll interval (more at shorter ones), so about 16 hours of it would use a month's operations; `--follow` against a stream is for short demos. Gzip is about 14x on this data, so storage is not the constraint.
- **Speed.** Uploads from South Africa to us-east1 took about 0.5 to 1 second an object whatever its size when measured, so they run in parallel; `scripts/measure_upload_latency.py` measures that against the bucket (about 60 Class A operations a run, and it deletes what it writes).
- **Intel Macs.** `cryptography` 50 has wheels for Apple Silicon only, so `requirements.txt` pins `cryptography<49`. On a slow connection use `pip install --default-timeout 100 -r requirements.txt`.
- **Tests never touch the real bucket.** Delivery is tested with an in-memory sink and a scripted reader (`tests/fakes.py`), and on a real slot with a real reader; the GCS writer is tested against a fake client that raises Google's real exception classes. The real bucket is covered by a manual smoke test and by the run in "A run from start to finish".

## Watching the slot

```bash
python -m cdc.lag
python -m cdc.lag --watch 10
```

The lag report asks Postgres alone, so it loads no Google library, touches no bucket and costs no operations. It prints where the slot is (its confirmed position), where the server's WAL is now, how far behind that is, how much WAL the slot is holding, the cap and the headroom left, and a level: healthy, under half the cap left, under a tenth left, unreserved, lost, or no cap. `--watch 10` prints one compact line every 10 seconds until Ctrl-C. It reads the same `CDC_` settings as the consumer, so `CDC_DB` and `CDC_SLOT_NAME` point it at the run being watched.

- **Behind and held are different numbers.** Behind is all the WAL the server has written since the slot's confirmed position, including other databases' and rows the publication does not cover. Held is the WAL the slot keeps from its restart position, which Postgres moves forward some time after a confirm, so held can stay above behind for a while after the consumer has drained.
- **Exit codes.** 0 for a slot at any level but lost (a warning is information here, not a failure), 4 for a lost slot (the report is printed, with the recovery), 1 for a missing slot, a lost connection or a usage error.
- **Before a big run** run it on the slot. A slot that has sat unread since it was created is already holding WAL, and its headroom is what the run starts with.

## Larger runs: chunking

The 3 GB cap holds roughly 4 million rows of WAL (about 750 bytes a row) and a 100-merchant year is about 1.0 million changes, so a consumer running alongside it cannot lose the slot: on the Air, with the generator and consumer sharing its two cores, it never fell more than 96.0 MB behind and the slot never held more than 105.9 MB (the sandbox rehearsal peaked at 10.2 MB behind). A far larger backfill, such as 1,000 merchants (about 11 million transactions), has not been put through the consumer. If one is run, run the generator in chunks and let the consumer drain between them, so the slot cannot be outrun. A chunk stops at its exclusive end date, and `--resume` continues from the checkpoint:

```bash
POSTGRES_DB=fintech_cdc python -m simulator.main --mode backfill --start-date 2025-01-01 --end-date 2025-05-01 --merchant-count 1000 --checkpoint checkpoint_cdc.json
python -m cdc.lag --watch 10
POSTGRES_DB=fintech_cdc python -m simulator.main --mode backfill --end-date 2025-09-01 --resume --checkpoint checkpoint_cdc.json
```

Start the next chunk when `behind` in the lag report is close to zero (with `--follow` running that happens within a poll interval of a chunk ending). If the headroom falls under half the cap while the generator is running, stop the generator, let the consumer catch up and `--resume`. The checkpoint is written after every simulated day, and a resumed run equals an uninterrupted one (`tests/test_checkpoint.py`, with loans in flight); a two-chunk run of 30 merchants over two months matched a single run by row count and by a digest of every table. The number of objects grows with the number of batches, so check the free-tier arithmetic above before a bigger run.

## Checking a run against its source

```bash
python -m cdc.reconcile
python -m cdc.reconcile --directory DIR
```

Do the landed objects add up to the source database? The command reads every object under the slot's prefix, from the bucket or from a directory laid out like it, one at a time (a 100-merchant year is about 1.3 GB as JSON, so it never holds the run), and checks, in this order:

- each object is what its name says (table, event date, first and last commit LSN), is valid gzipped NDJSON of events, and its events ascend strictly by commit LSN and position;
- every transaction is whole once duplicates are removed (positions 0 to size minus 1);
- the landed inserts of each table equal the rows in the source, and the landed updates touch exactly the loans the source shows as disbursed;
- the events replayed into a temporary blank database reproduce every table of the source, compared by row count and an order-independent digest of every row, computed in SQL. A mismatch is explained by key ("350 in the source only, for example ...").

Duplicates (the same change landed twice, as at-least-once delivery allows) are counted and absorbed. Objects whose LSN ranges overlap within a table and event date are a note, not a failure, because a replay that covered more data writes overlapping objects by design.

| Exit | Meaning |
|---|---|
| 0 | Reconciled: it prints `RECONCILED` with the object, event, duplicate, insert and update counts |
| 2 | Not reconciled: every problem is printed on its own `PROBLEM:` line |
| 3 | The bucket failed after its retries, or Google credentials are missing |
| 1 | Anything else, such as a lost database connection |

- **The source must be quiet and the consumer drained**: stop `--follow`, and `python -m cdc.main --once` must say "nothing was waiting".
- **The replay database** is created on the server the `CDC_` settings name, so the user needs `CREATEDB` (`sim` is a superuser in the compose file), and it is dropped afterwards. The replay writes about as much WAL as the source did (38.8 MB for a 34.4 MB source in a 3-month run), in another database, and the idle slot holds it until Postgres moves the slot's restart position on (which can take a while after a drain): after the 100-merchant year the lag report showed 732.7 MB held, with 2,355 MB of headroom left under the 3 GB cap.
- **Cost against the bucket.** It lists the prefix once (a Class A operation) and reads each object once (a Class B operation, a free allowance of 50,000 a month as summarised from Google's pricing page; check it).

## A run from start to finish

The definition of done for the CDC work is one 100-merchant year taken through the whole path: generated into `fintech_cdc`, consumed into the bucket while it is generated, and reconciled. It needs the database and the slot from "CDC setup", an empty `fintech_cdc` (check with `docker exec fintech_pg psql -U sim -d fintech_cdc -c "SELECT count(1) FROM merchants"`) and no `checkpoint_cdc.json`, then `python -m cdc.lag` on the slot as the pre-flight. Three terminals:

```bash
CDC_DB=fintech_cdc CDC_SLOT_NAME=cdc_r01 python -m cdc.main --follow
```

```bash
POSTGRES_DB=fintech_cdc python -m simulator.main --mode backfill --start-date 2025-01-01 --end-date 2026-01-01 --merchant-count 100 --checkpoint checkpoint_cdc.json
```

```bash
CDC_DB=fintech_cdc CDC_SLOT_NAME=cdc_r01 python -m cdc.lag --watch 10
```

Start the consumer first, so nothing waits in the slot. When the generator finishes, wait until `behind` in the lag report is close to zero, press Ctrl-C once in the consumer terminal (it finishes the batch in hand and exits 0), then:

```bash
CDC_DB=fintech_cdc CDC_SLOT_NAME=cdc_r01 python -m cdc.main --once
CDC_DB=fintech_cdc CDC_SLOT_NAME=cdc_r01 python -m cdc.reconcile
```

The `--once` run should say "nothing was waiting", and the reconcile should end in `RECONCILED`. The objects stay in the bucket. A repeat run needs a new slot name (`cdc_r02`) and so a new prefix, a fresh database and a new checkpoint file.

Measured on a 2017 MacBook Air against the real bucket (slot `cdc_r01`, seed 42, 100 merchants over 2025):

| What | Result |
|---|---|
| Generator | 663 s, 1,017,902 rows: 997,650 transactions, 20,078 merchant snapshots, 37 applications (18 disbursed), 76 repayments, 43 risk events. The sandbox run of the same seed wrote the same counts |
| Consumer alongside | Never more than 96.0 MB behind or 105.9 MB held; least headroom 2,977 MB of 3,072 MB; level healthy throughout. Fully caught up (56 bytes behind) within 11 s of the generator ending |
| Landed | 908 objects, 91.9 MB gzipped, one upload each (908 Class A operations of the month's 5,000) |
| `--once` afterwards | "nothing was waiting in the slot" |
| Reconcile against the bucket | `RECONCILED` in 3 min 43 s: 908 objects, 1,017,902 events, 0 duplicates, 1,017,884 inserts and 18 updates, and the replay equals the source in all five tables |

The consumer was not restarted during this run; restarts, kills and a lost slot are covered by the tests (`tests/test_cdc_killpoints.py`, `tests/test_cdc_lost_slot.py`).

## Reading the landed objects (Bronze: the read side, the schemas and the mapping)

The next layer is an Apache Iceberg Bronze layer on GCS over the objects the consumer lands (the GCS phase). The read side is built: a package, `bronze/`, that lists the landed objects, reads each one once, checks every event against the change-event contract, maps every event to the row Bronze will hold and keeps a small index of what is there. It writes nothing, anywhere. `python -m bronze.check` runs it and prints the result. The Bronze table schemas and the event-to-row mapping are built and tested (next section); no Bronze table exists yet, and nothing is written to `fintech_bronze` until the write side.

- **What it checks.** Every object name against the contract (`cdc/<slot>/<table>/event_date=YYYY-MM-DD/lsn_<first>_<last>.ndjson.gz`) and every event: the fields, `envelope_version` 1, one of the five tables, `after` (and `before`) columns equal to the models in name, number and order, no before image on an insert, updates only on `loan_applications`, a position inside its transaction, and the event agreeing with its object's name. It then maps every event to its Bronze row (see below) and throws the row away, so a value of the wrong shape stops the run here, naming the object, the event, the table and the column, before anything could be written. Copies of one change (`source.commit_lsn`, `transaction.position`) must agree on everything except `ts_ms` and `ingested_at`, and every transaction must have all of its positions. The index holds about 12 bytes an event, not the events, so a run of 1M events needs a few hundred MB, not 7 GB.
- **Credentials.** The Bronze job runs as the `bronze-writer` service account: it may read `cdc_landing` and read and write `fintech_bronze`, and it has no write permission on the landing bucket. It is used through an impersonated credentials file in its own gcloud configuration directory (never in the repo, never a key file), selected for one command with `GOOGLE_APPLICATION_CREDENTIALS`. Anything run without that variable uses your own login, which is not restricted, so the command refuses to start without it. The file carries no Google project, so the project comes from `BRONZE_PROJECT`.
- **Settings.** `BRONZE_PROJECT` (`payments-512910`), `BRONZE_BUCKET` (`fintech_bronze`), `BRONZE_CATALOG_PATH` (a local SQLite file outside the repo, used by the write side), `BRONZE_READ_WORKERS` (8 objects read at once). The landing bucket, the slot (so the prefix being read) and the source database are the consumer's `CDC_GCS_BUCKET`, `CDC_SLOT_NAME` and `CDC_DB`: there is one place that says where the landed objects are and which database they came from. An event from any other database is refused.

```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/bronze_credentials.json python -m bronze.check
```

It lists the prefix once and reads every object once (one listing, which is a Class A operation, and one read per object, which is a Class B operation: about 1 and 908 for a 100-merchant year), printing progress to stderr. Exit codes: 0 every object is valid, every event maps to a Bronze row and every transaction is complete; 1 anything else that is not a contract stop (missing credentials, a usage error, a bug); 2 a contract stop (a bad name, an event with columns or values the contract does not allow, copies that disagree, an incomplete transaction: the counts are still printed); 3 the bucket failed; 130 Ctrl-C.

The 100-merchant run in `gs://cdc_landing/cdc/cdc_r01/`, read on the MacBook Air (2026-10-28):

| | |
|---|---|
| Objects | 908 |
| Events | 1,017,902 (1,017,902 unique, 0 duplicates) |
| Transactions | 694 (0 incomplete) |
| Inserts and updates | 1,017,884 and 18 (the 18 updates are the loan disbursements) |
| Per table | `merchants` 20,078; `merchant_transactions` 997,650; `risk_events` 43; `loan_applications` 37 inserts and 18 updates; `loan_repayments` 76 |
| Time and memory | 2 min 43 s; 352 MB peak for the whole process, of 8 GiB (the first read, before the mapping existed; a second read of the same code took 4 min 51 s and 393 MB) |

These are the figures the reconciler printed for the same objects: a second, independent reader agrees with the first.

Read again with the mapping in pass 1 (2026-10-28, the Air, as `bronze-writer`, one read of the same 908 objects): every one of the 1,017,902 real events mapped to a Bronze row, with the same counts as above and the source database `fintech_cdc`. It took 6 min 4 s and peaked at 442 MB of 8 GiB. That is slower and larger than the two earlier reads, but one run cannot separate the mapping's cost from the spread between runs (the two earlier reads of identical code differed by 2 min 8 s and 41 MB) and the difference was not investigated.

### The schemas and the mapping

`bronze/schema.py` generates the Iceberg schema of each of the five Bronze tables from the model column lists, so the table definitions have one source. Each row has 13 envelope columns (all required: `op`, `event_date`, `event_ts`, `commit_lsn`, `change_lsn`, `commit_ts`, `transaction_position`, `transaction_size`, `transaction_xid`, `ts_ms`, `ingested_at`, `flags` and `envelope_version`), then the table's own columns under their raw names (all optional), and for `loan_applications` the same 24 columns again with the prefix `before_`. The envelope's `transaction.id` is `transaction_xid` because `merchant_transactions` already has a `transaction_id`. `source.db`, `source.schema` and `source.table` are not columns: the table is the Iceberg table, and one Bronze warehouse holds one landing prefix and one source database (recorded in table properties when a table is created). Integer widths (`int` against `long`) cannot come from the models, so they are an explicit 26 entry table, and a test builds a blank database from the migrations and fails if a migration changes one. Field ids are numbered 1 to N in column order, then list elements, so the ids are known before a table exists. Each table is partitioned by the month of `event_date` (field `event_month`); the data files are Parquet with zstd. A column whose name clashes with another, ignoring case, stops the schema being built.

`bronze/mapping.py` turns one landed event into one row (`event_to_row`) and a row back into its event (`row_to_event`). The conversion and the shape check are the same code and nothing is coerced: timestamps are exactly `YYYY-MM-DDTHH:MM:SS.ffffffZ`, dates exactly `YYYY-MM-DD`, an integer is an integer that fits its column, NUMERIC (`total_cost_percent`) stays its exact text, a null stays null, and an unknown flag, a column added, missing or renamed, a wrong `envelope_version`, or an `event_date` that is not the South African date of `event_ts` is an error that names where. Tests round-trip the contract's own examples and awkward values in every column (nulls, the limits of each integer width, unicode, empty and null lists, the South African date boundary); the real events were shown to map, not yet to round-trip through a table (that is the write side's check against the source database). `sort_rows` orders a batch by `(commit_lsn, transaction_position)` as a courtesy; no reader may rely on any order.

The Bronze tests are `tests/test_bronze_*.py`. `test_bronze_schema.py` compares the generated schemas with a hand-typed copy of the Bronze contract's column lists (`tests/bronze_contract.py`) and does a real local `create_table`; `test_bronze_mapping.py` holds the round trips and about 700 wrong-shape cases; `test_bronze_drift.py` runs each kind of drift (an extra, missing, renamed or retyped column, a wrong version) through the whole of pass 1 and the command. The width test needs Postgres (it skips locally when Postgres is down and fails under CI). Most tests run on synthetic landings (`tests/bronze_synth.py`), including a generated one of 1.2M events in 1,200 objects, larger than the real run, that the memory test reads in its own process under a 400 MB ceiling (slow: about 100 to 160 s). `tests/test_bronze_dependencies.py` checks that the exact PyIceberg, PyIceberg-core and PyArrow pins in `requirements.txt` are installed and work (they are the set verified on the Air, Python 3.13 and macOS 12) and never skips.

## Validating a run

`validate.sql` holds the reconciliation queries. Each one reads the five tables and returns a value next to the band from the Simulation Validation Targets, with the number of rows the value rests on.

```bash
python -m simulator.validation                          # every check against POSTGRES_DB
python -m simulator.validation --only par --only collections
psql -h localhost -U sim -d fintech -f validate.sql     # the same SQL, raw result sets
```

- The checks are measured, not asserted. A 100-merchant run is too noisy to hold a band, so the output is read across several seeds and larger runs. A run needs about 15 months to reach December, January and February, and the loan book needs longer than that to season at the default application rate.
- PAR is computed two ways from `loan_repayments`. The recorded measure takes `days_past_due` from a loan's latest row; it matches the generator's own loan state except for loans in arrears after their term ended, which the generator moves between buckets without writing a row. The instalment-age measure ages the oldest unpaid instalment, which is what a query over the table alone gives, and it runs ahead of the recorded measure between the monthly arrears cycles.
- `scripts/boosted_backfill.py` runs the same backfill with the monthly application propensity multiplied (default 5, 300 merchants over 24 months), into a database you name. The lending checks come from these runs, because the default rate gives too few loans; repayment behaviour is untouched. `PYTHONPATH=. python scripts/boosted_backfill.py --db val_x44 --seed 44`, then `POSTGRES_DB=val_x44 python -m simulator.validation`.
- `tests/test_reconcile.py` builds a 13-month, 100-merchant database with application propensity raised tenfold. It checks that the SQL loan book equals the generator's loans to the cent, asserts the seven failure modes from the Validation Targets, and plants each fault in the temp tables to confirm the query fires. It is marked slow.

## Where the generator departs from its source documents

The design documents (Event Schemas, Causal Dependency Rules, Simulator Design, Simulation Validation Targets) disagree with each other in places, and some of their numbers cannot all hold at once. Where that happened the choice was recorded, and each amended band has a test that fails if the reasoning stops holding.

- **Validation bands changed.** Gross take rate (1.9 to 2.4%), annual write-off rate (15 to 25%), E-commerce evening share (21 to 24%), EFT funnel completion (62 to 70%) and OTP share of failures (34 to 42%), fraud event rate floor (0.1%), January against December (27 to 40% below), Black Friday (1.3 to 1.9 times an average Friday), first payment default (5 to 10%), net approval (up to 80%), automation among eligible applicants (75 to 85%) and the approved share from the lowest risk bucket (75% or more). In each case the sourced rate card, funnel, matrix or modifiers give a value outside the original band. The annual credit loss ratio is not checked: it is net of recoveries, which V1 does not model.
- **Causal Rules over Simulator Design** for the weekday, hourly and seasonal tables of four archetypes, for Spaza trading on Sunday, and for Black Friday lifting ticket size only, after the cap.
- **Additions to the state machine** that Causal Rules does not give: New to Churned, Delinquent to Stressed, merchants that never activate becoming Churned 30 days after onboarding, and transition probabilities read as probabilities over a 30-day window, applied as daily hazards. Rolled once a day as written, a seasonal GPV drop moved about a third of merchants in one week.
- **Collections.** Retry success is 0.2063 an attempt, because three retries at the first-strike rate would recover 99% of failures, not about half. An unpaid instalment gets a month of late recovery before it counts as 1 to 30 days past due, so the retries and the matrix do not count the same miss twice. A dormant merchant either closed its business or moved acquirer.
- **Risk score.** Each merchant draws a baseline score at onboarding (illustrative; the mean rises with the archetype's refund, fraud and churn rates) and the score never falls below it. With every merchant starting at 10, 90 to 94% of active merchants sat in the lowest bucket.
- **Modelling choices.** A loan is its application (`loan_id` equals `application_id`); terms are 3, 6, 9 and 12 months; the `merchants` table is a daily snapshot insert that stops 30 days after a merchant churns unless it has a loan; reactivation is not modelled.

Results left outside their bands, with the reason: GPV growth between months 3 and 6 (28 to 33% against 5 to 25%) follows the onboarding ramp in the config. After transition probabilities became daily hazards, merchants take about a month to leave New, so the New share is 18 to 20% at 15 months (band 5 to 15%) and Delinquent is 1.3 to 1.7% (band 2 to 6%); platform churn is 1.3 to 1.9% at 24 months (band 2 to 5%) and twelve-month retention 52 to 59% (band 30 to 50%). The 56 to 75 and 76 to 100 risk buckets sit at 6 to 9% and 9 to 10% against 8 to 15% and 3 to 8%. Recorded PAR60 and PAR90 at 24 months are a little under their bands. None of these has a source value to tune against, so they are reported as measured.

Two limits on what the checks can say. At the default application rate a 15-month run originates about 120 loans, so PAR is read on runs of 24 months with application propensity multiplied by 5. And the stream writes no repayment rows for arrears cycles after a loan's term ends, so PAR computed from recorded days past due is a lower bound; ageing the unpaid instalments gives about 2.4 times that figure.

## Project structure

```
simulator/          Core simulation modules
cdc/                The CDC consumer: settings, the pgoutput decoder and type map, transactions, the change-event envelope, the column drift check, LSN helpers, errors; the slot reader, object naming and the replay compare (landing.py), the sink interface and its GCS writer, the loop (consumer.py), the dedup helper and the command line (main.py); the lag report (lag.py) and the reconciler (reconcile.py), each its own command
bronze/             Bronze: settings and the credentials guard, the read-only landing reader, the per-object checks and the content hash (events.py), the streaming index (index.py), the command (check.py), the table schemas (schema.py) and the event-to-row mapping (mapping.py); read only, nothing is written yet
config/             YAML calibration, archetypes, fees and causal rules
scripts/            capture_pgoutput_fixtures.py, measure_decode_speed.py and measure_upload_latency.py (CDC decoder fixtures, decode speed, upload latency to the bucket); portfolio_check.py: end-to-end run that prints portfolio quality (minutes, not a test); boosted_backfill.py: a backfill with more loans, for the lending checks
validate.sql        Reconciliation queries against the Validation Targets bands (run by simulator/validation.py)
tests/              Schema, calendar, generator, lending, repayment, risk, config, onboarding, checkpoint, runner, streaming, reconciliation, DB, CDC foundation, decoder, envelope, landing, reader, consumer, GCS writer, command line, round trip, kill point, failure, lost slot, lag report and reconciler tests, and the Bronze tests (landing reader, event checks, index, command, schemas, mapping, drift, memory ceiling, dependencies; tests/bronze_synth.py builds the synthetic landings) (tests/fakes.py holds the in-memory sink, the scripted reader and the fake storage client; tests/pgcluster.py starts the throwaway small-cap cluster)
migrations/         Postgres DDL (001 schema, 002 repayments foreign key, 003 replica identity)
docker-compose.yml  Local Postgres stack
.github/workflows/  CI: Postgres 16 started with logical decoding, migrations 001 to 003, full test suite (the lost-slot tests use the runner's own Postgres binaries and print their version)
```
