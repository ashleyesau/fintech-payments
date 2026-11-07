# Data model

The pipeline has four distinct schema layers: the OLTP source in Postgres, the change-event envelope in the CDC landing, the Bronze Iceberg tables on GCS, and the dbt-transformed tables in BigQuery. Each layer has its own contract. This document describes each schema, the key design choices, and the lineage from source to mart.

---

## Layer 1: OLTP source (Postgres `fintech_cdc`)

Five tables in the `public` schema. Created by three migrations in `migrations/`.

**Key conventions across all tables:**
- `TEXT` for UUIDs (not native UUID type, for V1 simplicity)
- `BIGINT` for all amounts, in ZAR cents (minor units)
- `TIMESTAMPTZ` for timestamps
- `TEXT` with `CHECK` constraints for enum columns (no Postgres enum type)
- No foreign keys on `merchant_id` (the composite PK on `merchants` makes an FK column-reference impossible)

### `merchants`

| Column | Type | Notes |
|---|---|---|
| `merchant_id` | TEXT | Part of composite PK |
| `snapshot_at` | TIMESTAMPTZ | Part of composite PK. 23:59 SAST each day. |
| `archetype` | TEXT | One of 11 archetypes (spaza_shop, restaurant, ...) |
| `province` | TEXT | SA province |
| `is_metro` | BOOLEAN | |
| `merchant_state` | TEXT | Enum: New, Growing, Stable, Stressed, Delinquent, Churned |
| `risk_score` | INT | Defaults to 10. Floor 80 on write-off. |
| `loan_count` | INT | |
| `has_active_loan` | BOOLEAN | |
| `onboarded_at` | TIMESTAMPTZ | |
| `first_transaction_at` | TIMESTAMPTZ | NULL until first payment |

INSERT-only daily snapshot at 23:59 SAST. Snapshots stop 30 days after a merchant becomes Churned unless a loan is active.

### `merchant_transactions`

32 columns. INSERT-only. PK `transaction_id`.

Key columns: `merchant_id`, `event_type` (payment, refund, chargeback), `channel` (card_present, card_ecom, instant_eft, payshap, payment_link), `gross_amount_minor` (BIGINT), `net_amount_minor`, `acquirer_markup_minor`, `created_at`, `settlement_date`, `status`, `correlation_id` (nullable; on refund and chargeback rows, holds the original `transaction_id`).

### `loan_applications`

24 columns. PK `application_id`. One INSERT at decision, one UPDATE at disbursement.

The disbursement UPDATE sets `disbursed_at` and `disbursement_amount_minor`. The CDC consumer captures both the `c` (insert) and `u` (update) events. `REPLICA IDENTITY FULL` is set on this table (migration 003) so the update carries the full old row.

Key columns: `application_id`, `merchant_id`, `decision` (approved, declined, counter_offer), `actioned_by` (automated_engine, manual_review), `approved_amount_minor`, `disbursement_amount_minor` (NULL at insert), `disbursed_at` (NULL at insert), `approved_term_months`, `total_cost_percent` (NUMERIC(5,4)), `decline_reasons` (TEXT[]).

### `loan_repayments`

22 columns. INSERT-only. PK `repayment_id`. FK `loan_id` to `loan_applications.application_id` (migration 002).

One row per collection attempt. Retries, cures and write-offs are new rows.

Key columns: `loan_id`, `installment_number`, `retry_attempt`, `days_past_due`, `par_bucket` (current, 1-30, 31-60, 61-90, 90+), `collection_status`, `payment_ts` (nullable for unpaid attempts; fall back to `due_date`), `outstanding_before_minor`, `outstanding_after_minor`, `is_cured`, `write_off_flag`.

### `risk_events`

13 columns. INSERT-only. PK `alert_id`.

Key columns: `merchant_id`, `entity_id` (polymorphic: `transaction_id` for transaction-linked rules, `merchant_id` for merchant-level rules), `rule_type`, `action` (allow, flag_review, block), `risk_score` (merchant score after the delta), `action_ts`, `resolved_ts` (always NULL in V1).

---

## Layer 2: CDC landing (gzipped NDJSON)

Each change event is one JSON line. The envelope wraps the row data with source metadata.

**Envelope fields (all events):**

| Field | Type | Notes |
|---|---|---|
| `source.db` | string | Database name |
| `source.table` | string | Table name |
| `source.commit_lsn` | string | LSN of the transaction commit |
| `source.change_lsn` | string | LSN of this specific change |
| `source.commit_ts` | string | Commit timestamp (UTC, 6 fractional digits) |
| `op` | string | `c` (insert) or `u` (update) |
| `before` | object or null | Full old row on `u`; null on `c` |
| `after` | object | New row |
| `transaction.position` | int | 0-indexed position within the transaction |
| `transaction.size` | int | Total changes in the transaction |
| `event_ts` | string | Row's own event timestamp (UTC) |
| `event_date` | string | SAST date derived from `event_ts` |
| `ts_ms` | int | Wall-clock milliseconds at landing |
| `ingested_at` | string | ISO timestamp at landing |
| `envelope_version` | int | Always 1 in V1 |

**Object naming:** `cdc/<slot_name>/<table>/<event_date>/part-<seq>-<commit_lsn>.ndjson.gz`. One object per table per event date. Dedup key: `(source.commit_lsn, transaction.position)`.

---

## Layer 3: Bronze Iceberg tables (GCS `gs://fintech_bronze`)

Five Iceberg tables managed by PyIceberg 0.12.0, backed by Parquet files. The Iceberg table format adds a transaction log (snapshot history, schema metadata, manifest files) on top of the Parquet data files.

**Partition:** `month(event_date)`. Each month's data lands in one or more Parquet files under `bronze/<table>/data/event_date_month=YYYY-MM/`.

**Key design choices:**
- Append-only Bronze. The Bronze writer appends new events; it never updates or deletes.
- `loan_applications` keeps both the `c` and `u` rows. The Silver/staging layer deduplicates to the latest per `application_id`.
- Each column maps from the Postgres type to a Parquet type via a width table in `bronze/schema.py`. BIGINT stays INT64. NUMERIC(5,4) becomes FLOAT64. TEXT[] (the `decline_reasons` array) becomes a Parquet list of strings.

**Run marker:** after each write, the writer creates a JSON marker object under `gs://fintech_bronze/_runs/<timestamp>Z.json`. The marker records the Iceberg metadata file path for each table, the row counts added, and the slot name. The catalog (`_catalog/catalog.db`) is a SQLite copy rebuilt from this marker; it is a cache, not the source of truth.

**Bronze table schemas** (defined in `bronze/schema.py`, derived from the Postgres migrations):

The five tables mirror their OLTP sources. The Bronze envelope adds `op` and `ingested_at` to each row. `event_date` (pre-computed SAST date) is the partition column.

---

## Layer 4: BigQuery (staging, intermediate, marts)

BigQuery holds the external Bronze tables (dataset `bronze`), the dbt-managed staging views and intermediate tables (dataset `staging`), and the mart fact tables (dataset `marts`). All in project `payments-512910`, `us-east1`.

### Staging views

Five views. Each selects from the corresponding `bronze.*` external table, applies `WHERE op = 'c'` for insert-only streams, deduplicates where needed, drops envelope columns, and renames fields to match dbt conventions.

`stg_loan_applications` deduplicates to the latest row per `application_id` by `(commit_lsn DESC, transaction_position DESC)` and drops all `before_*` columns.

### Dimensions

Three tables in the `marts` dataset:

- `merchants_scd`: SCD2 history. Built by `dbt/models/dimensions/merchants_scd.sql` using LAG window functions over all daily Bronze rows to detect attribute changes. 431 rows on the 100-merchant dataset (average 3.7 SCD2 records per merchant).
- `dim_merchants`: current-state view. `WHERE dbt_is_current = TRUE`. One row per active merchant.
- `dim_date`: date spine from 2025-01-01 to 2026-12-31 (730 rows). Built with `dbt_utils.date_spine`. Columns include `is_sa_holiday` (26 SA public holidays from the `sa_public_holidays` seed), `is_business_day`, `is_payday_period` (days 2-4 and 25-27), `is_month_end`, `is_quarter_end`.

### Intermediate tables

Eight tables in the `staging` dataset:

| Model | Rows (100-merchant) | Notes |
|---|---|---|
| `int_payments__transaction_enriched` | 997,600 | Joins transactions to `merchants_scd` on SCD2 date range |
| `int_payments__merchant_daily_gpv` | 15,600 | Daily GPV per merchant |
| `int_payments__merchant_gpv_rolling` | 15,600 | 30d and 60d rolling GPV windows (table, not materialized view: see [Design decisions](design-decisions.md)) |
| `int_lending__loan_daily_snapshot` | 5,800 | Date spine per loan via `GENERATE_DATE_ARRAY` |
| `int_lending__loan_fee_schedule` | 156 | Monthly fee amortisation per loan |
| `int_lending__portfolio_par` | ephemeral | PAR30/60/90 rates and balances |
| `int_lending__vintage_cohorts` | ephemeral | Month-end cohort snapshots |
| `int_risk__merchant_risk_daily` | 15,600 | Daily risk profile per merchant |

### Mart fact tables

Eleven incremental tables in the `marts` dataset. All use `insert_overwrite` or `merge` incremental strategies with explicit lookback windows. `dbt run --full-refresh` rebuilds from scratch; used in the Airflow DAG.

| Table | Rows (1,000-merchant) | Grain | Key metrics |
|---|---|---|---|
| `fct_payments_daily` | 161,500 | merchant + txn_date | gross_gpv, net_gpv, take_rate |
| `fct_settlement_daily` | 396,100 | merchant + settlement_date + batch_id + channel | gross_gpv, net_payout, acquirer_revenue |
| `fct_revenue_daily` | 158,975 | revenue_date + revenue_stream + merchant | card_acquiring UNION loan_fees |
| `fct_merchant_health_daily` | 161,500 | merchant + snapshot_date | risk_score, is_flagged, is_gpv_declining |
| `fct_loan_portfolio_daily` | 601 | snapshot_date | PAR30/60/90 rates and balances |
| `fct_loan_applications_daily` | 37 | event_date + decision + channel | approval rate, disbursement rate |
| `fct_collections_performance_daily` | 60 | event_date + collection_status + par_bucket | collection_rate |
| `fct_loan_unit_economics` | 1,200 | loan_id + fee_month | fee_income, bad_debt_provision, net_lending_margin |
| `fct_vintage_curves` | 563 | vintage_month + month_on_book | par30_rate_by_balance |
| `fct_risk_events_daily` | 563 | event_date + rule_type + archetype | block count, flag count |
| `fct_merchant_monthly_summary` | 6,300 | merchant + month | monthly GPV, loan balance, collections |

Row counts from the 1,000-merchant run (session 38, 2026-11-07).

### Seeds

Two CSV seeds in `dbt/seeds/`:
- `sa_public_holidays.csv`: 26 South African public holidays for 2025-2026, used in `dim_date`.
- `provision_rates.csv`: CALC-5 loan loss provision rates by PAR bucket (current 2%, 1-30 10%, 31-60 30%, 61-90 60%, 90+ 100%), used in `fct_loan_unit_economics`.

---

## Lineage summary

```
Postgres (5 tables)
  -> CDC envelope (NDJSON)
    -> Bronze Iceberg (5 tables, Parquet, GCS)
      -> BigQuery external tables (bronze.*)
        -> dbt staging views (5 views)
          -> dbt dimensions (merchants_scd, dim_merchants, dim_date)
          -> dbt intermediate (8 models)
            -> dbt mart tables (11 models, marts.*)
              -> Lightdash dashboards (8 dashboards)
```

---

*For how data flows between components, see [Architecture](architecture.md). For how the mart layer is rebuilt on each run, see [Orchestration](orchestration.md).*
