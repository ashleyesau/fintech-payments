{{
    config(materialized='materialized_view')
}}

-- Rolling 30-day and 60-day GPV windows per merchant, using a date-range window
-- so each day's value is the true calendar-day sum without zero-filling gaps.
-- BigQuery maintains this view automatically after each refresh of the source table.
--
-- CALC-8: RANGE BETWEEN INTERVAL N DAY PRECEDING AND CURRENT ROW gives a correct
-- calendar-day window. No zero-fill expansion of missing days needed.
-- 30d window = 29 preceding + current day = 30 calendar days.
-- 60d window = 59 preceding + current day = 60 calendar days.

SELECT
    merchant_id,
    txn_date,
    gross_gpv_minor,
    net_gpv_minor,
    completed_payment_count,
    txn_count,

    SUM(gross_gpv_minor) OVER (
        PARTITION BY merchant_id
        ORDER BY txn_date
        RANGE BETWEEN INTERVAL 29 DAY PRECEDING AND CURRENT ROW
    )                                                   AS rolling_30d_gpv_minor,

    SUM(gross_gpv_minor) OVER (
        PARTITION BY merchant_id
        ORDER BY txn_date
        RANGE BETWEEN INTERVAL 59 DAY PRECEDING AND CURRENT ROW
    )                                                   AS rolling_60d_gpv_minor,

    SUM(completed_payment_count) OVER (
        PARTITION BY merchant_id
        ORDER BY txn_date
        RANGE BETWEEN INTERVAL 29 DAY PRECEDING AND CURRENT ROW
    )                                                   AS rolling_30d_txn_count,

    SUM(total_acquirer_markup_minor) OVER (
        PARTITION BY merchant_id
        ORDER BY txn_date
        RANGE BETWEEN INTERVAL 29 DAY PRECEDING AND CURRENT ROW
    )                                                   AS rolling_30d_acquirer_markup_minor

FROM {{ ref('int_payments__merchant_daily_gpv') }}
