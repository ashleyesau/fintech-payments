{{
    config(
        materialized='incremental',
        unique_key=['loan_id', 'fee_month'],
        incremental_strategy='merge',
        partition_by={'field': 'fee_month', 'data_type': 'date'}
    )
}}

-- Loan unit economics mart. Grain: loan_id + fee_month (calendar month on book).
-- CALC-5: provision rates from seeds/provision_rates.csv; applied to month-end
-- closing balance per PAR bucket.
--   bad_debt_provision_minor = closing_balance * provision_rate
--   net_lending_margin_minor = fee_income - bad_debt_provision
-- Merge: provisions change as DPD changes, so past rows may need updating.
-- Month-end balance from LAST_DAY snapshot in int_lending__loan_daily_snapshot.

WITH fee_schedule AS (
    SELECT * FROM {{ ref('int_lending__loan_fee_schedule') }}
),

-- Month-end snapshot: last available snapshot day per loan per calendar month
month_end_balance AS (
    SELECT
        loan_id,
        DATE_TRUNC(snapshot_date, MONTH)                    AS fee_month,
        outstanding_principal_minor                         AS closing_balance_minor,
        par_bucket,
        is_written_off
    FROM {{ ref('int_lending__loan_daily_snapshot') }}
    WHERE snapshot_date = LAST_DAY(snapshot_date, MONTH)
),

rates AS (
    SELECT par_bucket, provision_rate FROM {{ ref('provision_rates') }}
)

SELECT
    f.loan_id,
    f.merchant_id,
    f.fee_month,
    f.month_number,
    f.cohort_date,

    -- fee income: straight-line monthly fee (CALC-2)
    f.monthly_fee_minor                                                         AS fee_income_minor,

    -- balance state at month end
    COALESCE(b.closing_balance_minor, 0)                                        AS closing_balance_minor,
    COALESCE(b.par_bucket, 'current')                                           AS par_bucket,
    COALESCE(b.is_written_off, FALSE)                                           AS is_written_off,

    -- CALC-5: provision rate for this PAR bucket (default 'current' = 2%)
    COALESCE(r.provision_rate, 0.02)                                            AS provision_rate,

    -- CALC-5: bad_debt_provision = closing_balance * rate
    CAST(
        ROUND(COALESCE(b.closing_balance_minor, 0) * COALESCE(r.provision_rate, 0.02))
        AS INT64
    )                                                                           AS bad_debt_provision_minor,

    -- CALC-5: net_lending_margin = fee_income - bad_debt_provision
    f.monthly_fee_minor
    - CAST(
        ROUND(COALESCE(b.closing_balance_minor, 0) * COALESCE(r.provision_rate, 0.02))
        AS INT64
    )                                                                           AS net_lending_margin_minor

FROM fee_schedule f
LEFT JOIN month_end_balance b
    ON  f.loan_id   = b.loan_id
    AND f.fee_month = b.fee_month
LEFT JOIN rates r
    ON COALESCE(b.par_bucket, 'current') = r.par_bucket

{% if is_incremental() %}
WHERE f.fee_month >= (
    SELECT DATE_SUB(MAX(fee_month), INTERVAL 3 MONTH) FROM {{ this }}
)
{% endif %}
