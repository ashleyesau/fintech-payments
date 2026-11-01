{{
    config(
        materialized='incremental',
        partition_by={'field': 'txn_date', 'data_type': 'date'},
        incremental_strategy='insert_overwrite'
    )
}}

-- Daily payment volume mart. Grain: merchant_id + txn_date.
-- Adds take_rate (acquirer_markup / gross_gpv) and current merchant dimension.
-- On incremental runs replaces the last 3 days of partitions to pick up
-- any corrections. First run processes all data.

SELECT
    gpv.merchant_id,
    gpv.txn_date,

    -- merchant dimension at snapshot time (current state)
    m.archetype,
    m.province,
    m.is_metro,
    m.merchant_state,

    -- transaction counts
    gpv.txn_count,
    gpv.completed_payment_count,
    gpv.declined_count,
    gpv.refund_count,
    gpv.chargeback_count,

    -- payment volume (minor units = ZAR cents)
    gpv.gross_gpv_minor,
    gpv.refund_volume_minor,
    gpv.chargeback_volume_minor,
    gpv.net_gpv_minor,

    -- fee breakdown
    gpv.total_interchange_fee_minor,
    gpv.total_scheme_fee_minor,
    gpv.total_acquirer_markup_minor,
    gpv.total_fee_minor,

    -- take rate: acquirer markup as share of gross GPV (NULL when no GPV)
    SAFE_DIVIDE(gpv.total_acquirer_markup_minor, gpv.gross_gpv_minor) AS take_rate

FROM {{ ref('int_payments__merchant_daily_gpv') }} gpv
LEFT JOIN {{ ref('dim_merchants') }} m
    ON gpv.merchant_id = m.merchant_id

{% if is_incremental() %}
WHERE gpv.txn_date >= (
    SELECT DATE_SUB(MAX(txn_date), INTERVAL 3 DAY) FROM {{ this }}
)
{% endif %}
