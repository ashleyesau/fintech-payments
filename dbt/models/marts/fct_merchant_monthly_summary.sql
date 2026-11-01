{{
    config(
        materialized='incremental',
        unique_key=['merchant_id', 'month'],
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'month', 'data_type': 'date'}
    )
}}

-- Merchant monthly statement mart. Grain: merchant_id + month (first day of month).
-- CALC-6:
--   GPV source: int_payments__merchant_daily_gpv (not fct_payments_daily to avoid
--               mart-to-mart dependency).
--   Month-end loan balance: last day of month from int_lending__loan_daily_snapshot.
--   Collections: stg_loan_repayments filtered by due_date.

WITH monthly_gpv AS (
    SELECT
        merchant_id,
        DATE_TRUNC(txn_date, MONTH)                             AS month,
        SUM(gross_gpv_minor)                                    AS gross_gpv_minor,
        SUM(net_gpv_minor)                                      AS net_gpv_minor,
        SUM(total_acquirer_markup_minor)                        AS acquirer_revenue_minor,
        SUM(txn_count)                                          AS txn_count,
        SUM(completed_payment_count)                            AS completed_payment_count,
        SUM(refund_count)                                       AS refund_count,
        SUM(chargeback_count)                                   AS chargeback_count
    FROM {{ ref('int_payments__merchant_daily_gpv') }}
    GROUP BY merchant_id, DATE_TRUNC(txn_date, MONTH)
),

-- CALC-6: month-end loan balance from last day of month snapshot
month_end_loans AS (
    SELECT
        merchant_id,
        DATE_TRUNC(snapshot_date, MONTH)                        AS month,
        SUM(outstanding_principal_minor)                        AS month_end_loan_balance_minor,
        COUNT(DISTINCT loan_id)                                 AS active_loan_count
    FROM {{ ref('int_lending__loan_daily_snapshot') }}
    WHERE snapshot_date = LAST_DAY(snapshot_date, MONTH)
    GROUP BY merchant_id, DATE_TRUNC(snapshot_date, MONTH)
),

-- CALC-6: collections from stg_loan_repayments filtered by due_date month
monthly_collections AS (
    SELECT
        merchant_id,
        DATE_TRUNC(due_date, MONTH)                             AS month,
        SUM(amount_paid)                                        AS amount_collected_minor,
        COUNTIF(collection_status = 'success')                  AS successful_collections,
        COUNT(*)                                                AS collection_attempts
    FROM {{ ref('stg_loan_repayments') }}
    GROUP BY merchant_id, DATE_TRUNC(due_date, MONTH)
)

SELECT
    gpv.merchant_id,
    gpv.month,

    -- payment metrics
    gpv.gross_gpv_minor,
    gpv.net_gpv_minor,
    gpv.acquirer_revenue_minor,
    gpv.txn_count,
    gpv.completed_payment_count,
    gpv.refund_count,
    gpv.chargeback_count,
    SAFE_DIVIDE(gpv.acquirer_revenue_minor, gpv.gross_gpv_minor)    AS take_rate,

    -- lending metrics (zero when no active loans)
    COALESCE(loans.month_end_loan_balance_minor, 0)                 AS month_end_loan_balance_minor,
    COALESCE(loans.active_loan_count, 0)                            AS active_loan_count,

    -- collections metrics (zero when no repayments due that month)
    COALESCE(col.amount_collected_minor, 0)                         AS amount_collected_minor,
    COALESCE(col.successful_collections, 0)                         AS successful_collections,
    COALESCE(col.collection_attempts, 0)                            AS collection_attempts

FROM monthly_gpv gpv
LEFT JOIN month_end_loans loans
    ON  gpv.merchant_id = loans.merchant_id
    AND gpv.month       = loans.month
LEFT JOIN monthly_collections col
    ON  gpv.merchant_id = col.merchant_id
    AND gpv.month       = col.month

{% if is_incremental() %}
WHERE gpv.month >= (
    SELECT DATE_SUB(MAX(month), INTERVAL 3 MONTH) FROM {{ this }}
)
{% endif %}
