# Data model

The pipeline has four distinct schema layers: the OLTP source in Postgres, the CDC landing envelope, the Bronze Iceberg tables on GCS, and the dbt-transformed tables in BigQuery. Each layer has its own contract.

---

## Layer 1: OLTP source (Postgres `fintech_cdc`)

Five tables in the `public` schema, created by three migrations in `migrations/`.

**Conventions across all tables:**
- `TEXT` for UUIDs (not native UUID type)
- `BIGINT` for all amounts, in ZAR cents (minor units)
- `TIMESTAMPTZ` for timestamps
- `TEXT` with `CHECK` constraints for enum columns
- No foreign keys on `merchant_id` (the composite PK on `merchants` makes an FK column-reference impossible)

### `merchants`

| Column | Type | Notes |
|---|---|---|
| `merchant_id` | TEXT | Part of composite PK |
| `snapshot_at` | TIMESTAMPTZ | Part of composite PK. 23:59 SAST each day. |
| `archetype` | TEXT | One of 11 archetypes |
| `province` | TEXT | SA province |
| `is_metro` | BOOLEAN | |
| `merchant_state` | TEXT | New, Growing, Stable, Stressed, Delinquent, Churned |
| `risk_score` | INT | Defaults to 10. Floor 80 on write-off. |
| `loan_count` | INT | |
| `has_active_loan` | BOOLEAN | |
| `onboarded_at` | TIMESTAMPTZ | |
| `first_transaction_at` | TIMESTAMPTZ | NULL until first payment |

INSERT-only daily snapshot at 23:59 SAST. Snapshots stop 30 days after a merchant becomes Churned, unless a loan is active.

### `merchant_transactions`

32 columns. INSERT-only. PK `transaction_id`.

Key columns:

| Column | Notes |
|---|---|
| `event_type` | payment, refund, chargeback |
| `channel` | card_present, card_ecom, instant_eft, payshap, payment_link |
| `gross_amount_minor` | BIGINT, ZAR cents |
| `settlement_date` | DATE |
| `status` | completed, declined, failed, etc. |
| `correlation_id` | Nullable. On refund and chargeback rows, holds the original `transaction_id`. |

### `loan_applications`

24 columns. PK `application_id`. One INSERT at decision, one UPDATE at disbursement.

The disbursement UPDATE sets `disbursed_at` and `disbursement_amount_minor`. `REPLICA IDENTITY FULL` is set on this table (migration 003), so the update carries the full old row.

Key columns:

| Column | Notes |
|---|---|
| `decision` | approved, declined, counter_offer |
| `actioned_by` | automated_engine, manual_review |
| `approved_amount_minor` | BIGINT |
| `disbursement_amount_minor` | NULL at insert |
| `disbursed_at` | NULL at insert |
| `approved_term_months` | INT |
| `total_cost_percent` | NUMERIC(5,4) |
| `decline_reasons` | TEXT[] |

### `loan_repayments`

22 columns. INSERT-only. PK `repayment_id`. FK `loan_id` to `loan_applications.application_id` (migration 002).

One row per collection attempt. Retries, cures and write-offs are new rows.

Key columns:

| Column | Notes |
|---|---|
| `installment_number` | INT |
| `retry_attempt` | INT |
| `days_past_due` | INT |
| `par_bucket` | current, 1-30, 31-60, 61-90, 90+ |
| `collection_status` | success, unpaid_nsf, disputed, reversed, etc. |
| `payment_ts` | Nullable. Unpaid attempts have no payment time; fall back to `due_date`. |
| `outstanding_before_minor` | BIGINT |
| `outstanding_after_minor` | BIGINT |
| `is_cured` | BOOLEAN |
| `write_off_flag` | BOOLEAN |

### `risk_events`

13 columns. INSERT-only. PK `alert_id`.

Key columns:

| Column | Notes |
|---|---|
| `entity_id` | Polymorphic. `transaction_id` for transaction-linked rules, `merchant_id` for merchant-level rules. |
| `rule_type` | velocity_breach, refund_spike, chargeback_accumulation, etc. |
| `action` | allow, flag_review, block |
| `risk_score` | Merchant score after the delta |
| `resolved_ts` | Always NULL in V1 |

---

## Layer 2: CDC landing (gzipped NDJSON)

Each change event is one JSON line. The envelope wraps the row data with source metadata.

**Envelope fields:**

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

**Object naming:** `cdc/<slot_name>/<table>/<event_date>/part-<seq>-<commit_lsn>.ndjson.gz`

One object per table per event date. Dedup key: `(source.commit_lsn, transaction.position)`.

---

## Layer 3: Bronze Iceberg tables (GCS `gs://fintech_bronze`)

Five Iceberg tables managed by PyIceberg 0.12.0, backed by Parquet files. The Iceberg layer adds a transaction log (snapshot history, schema metadata, manifest files) on top of the Parquet data files.

**Partition:** `month(event_date)`. Each month's data lands under `bronze/<table>/data/event_date_month=YYYY-MM/`.

**Key design choices:**
- Append-only. The Bronze writer never updates or deletes.
- `loan_applications` keeps both `c` and `u` rows. Staging deduplicates to the latest per `application_id`.
- Column type mapping in `bronze/schema.py`: BIGINT stays INT64, NUMERIC(5,4) becomes FLOAT64, TEXT[] becomes a Parquet list of strings.

**Run marker.** After each write, the writer creates a JSON marker under `gs://fintech_bronze/_runs/<timestamp>Z.json`. It records the Iceberg metadata file path for each table, the row counts added, and the slot name.

**Catalog.** The SQLite file at `_catalog/catalog.db` is a cache rebuilt from the run marker. It is not the source of truth. If lost, `--rebuild-catalog` reconstructs it without reading any landed objects.

The five Bronze tables mirror their OLTP sources. Each row gains `op` and `ingested_at` from the envelope, and `event_date` as the partition column.

**Row counts (1,000-merchant run):**

| Table | Rows | Notes |
|---|---|---|
| `merchants` | 213,923 | Daily snapshots across all merchants |
| `merchant_transactions` | 8,321,600 | |
| `loan_applications` | 451 | 293 inserts + 158 disbursement updates |
| `loan_repayments` | 523 | |
| `risk_events` | 593 | |

---

## Layer 4: BigQuery (staging, intermediate, marts)

Three datasets in project `payments-512910`, `us-east1`:
- `bronze`: external Iceberg tables pointing at the Bronze GCS files
- `staging`: dbt views and intermediate tables
- `marts`: dbt mart fact tables

### Staging views

Five views. Each selects from the corresponding `bronze.*` external table, applies `WHERE op = 'c'` for insert-only streams, deduplicates where needed, and drops envelope columns.

`stg_loan_applications` deduplicates to the latest row per `application_id` by `(commit_lsn DESC, transaction_position DESC)` and drops all `before_*` columns.

### Dimensions

Three tables in the `marts` dataset:

| Model | Rows (1,000-merchant) | Notes |
|---|---|---|
| `merchants_scd` | 4,991 | SCD2 history via LAG window functions. Avg 4.2 records per merchant. |
| `dim_merchants` | 1,185 | Current state only. `WHERE dbt_is_current = TRUE`. |
| `dim_date` | 730 | Date spine 2025-01-01 to 2026-12-31. Built with `dbt_utils.date_spine`. Includes `is_sa_holiday`, `is_business_day`, `is_payday_period`. |

### Intermediate tables

Eight tables in the `staging` dataset:

| Model | Rows (1,000-merchant) | Notes |
|---|---|---|
| `int_payments__transaction_enriched` | 8,321,600 | Joins transactions to `merchants_scd` on SCD2 date range |
| `int_payments__merchant_daily_gpv` | 161,460 | Daily GPV per merchant |
| `int_payments__merchant_gpv_rolling` | 161,460 | 30d and 60d rolling GPV windows |
| `int_lending__loan_daily_snapshot` | 45,279 | Date spine per loan via `GENERATE_DATE_ARRAY` |
| `int_lending__loan_fee_schedule` | 1,182 | Monthly fee amortisation per loan |
| `int_lending__portfolio_par` | ephemeral | PAR30/60/90 rates and balances |
| `int_lending__vintage_cohorts` | ephemeral | Month-end cohort snapshots |
| `int_risk__merchant_risk_daily` | 161,460 | Daily risk profile per merchant |

### Mart fact tables

Eleven incremental tables in the `marts` dataset. All use `insert_overwrite` or `merge` strategies with explicit lookback windows. `dbt run --full-refresh` rebuilds from scratch; used in the Airflow DAG.

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

Row counts verified on the 1,000-merchant run (session 38, 2026-11-07).

### Seeds

| File | Used in | Contents |
|---|---|---|
| `sa_public_holidays.csv` | `dim_date` | 26 SA public holidays for 2025-2026 |
| `provision_rates.csv` | `fct_loan_unit_economics` | Loan loss provision rates by PAR bucket |

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
