{{
    config(materialized='table')
}}

-- Rolling 30-day and 60-day GPV windows per merchant, using a date-range window
-- so each day's value is the true calendar-day sum without zero-filling gaps.
--
-- SL-D10: changed from materialized_view to table.
-- BigQuery materialized views do not support RANGE BETWEEN with offset boundaries
-- (OFFSET PRECEDING requires a numeric ORDER BY; INTERVAL PRECEDING requires
-- TIMESTAMP, not DATE). A regular table with UNIX_DATE is the correct fix.
--
-- CALC-8 semantics preserved: ORDER BY UNIX_DATE(txn_date) makes the key
-- numeric (days since epoch). RANGE BETWEEN 29 PRECEDING = all rows within 29
-- integer-days of the current row's date = exactly 30 calendar days.
-- 30d window = 29 preceding + current day.
-- 60d window = 59 preceding + current day.

SELECT
    merchant_id,
    txn_date,
    gross_gpv_minor,
    net_gpv_minor,
    completed_payment_count,
    txn_count,

    SUM(gross_gpv_minor) OVER (
        PARTITION BY merchant_id
        ORDER BY UNIX_DATE(txn_date)
        RANGE BETWEEN 29 PRECEDING AND CURRENT ROW
    )                                                   AS rolling_30d_gpv_minor,

    SUM(gross_gpv_minor) OVER (
        PARTITION BY merchant_id
        ORDER BY UNIX_DATE(txn_date)
        RANGE BETWEEN 59 PRECEDING AND CURRENT ROW
    )                                                   AS rolling_60d_gpv_minor,

    SUM(completed_payment_count) OVER (
        PARTITION BY merchant_id
        ORDER BY UNIX_DATE(txn_date)
        RANGE BETWEEN 29 PRECEDING AND CURRENT ROW
    )                                                   AS rolling_30d_txn_count,

    SUM(total_acquirer_markup_minor) OVER (
        PARTITION BY merchant_id
        ORDER BY UNIX_DATE(txn_date)
        RANGE BETWEEN 29 PRECEDING AND CURRENT ROW
    )                                                   AS rolling_30d_acquirer_markup_minor

FROM {{ ref('int_payments__merchant_daily_gpv') }}
