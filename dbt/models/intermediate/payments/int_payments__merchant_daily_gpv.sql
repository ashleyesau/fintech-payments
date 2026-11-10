{{
    config(
        materialized='incremental',
        partition_by={'field': 'txn_date', 'data_type': 'date'},
        incremental_strategy='insert_overwrite'
    )
}}

-- Daily per-merchant aggregation of payment volume and fees.
-- Grain: one row per (merchant_id, txn_date). Used by the rolling GPV model,
-- the merchant health mart, and the merchant monthly summary mart.
--
-- GPV flag convention (from stg_merchant_transactions):
--   event_type = 'payment' AND status = 'completed'  -> counts toward gross_gpv
--   event_type = 'refund'                             -> counts toward refund_volume
--   event_type = 'chargeback'                         -> counts toward chargeback_volume
--   event_type = 'void'                               -> excluded from GPV

SELECT
    merchant_id,
    event_date                                                          AS txn_date,

    -- transaction counts
    COUNT(*)                                                            AS txn_count,
    COUNTIF(event_type = 'payment' AND status = 'completed')            AS completed_payment_count,
    COUNTIF(event_type = 'payment' AND status = 'declined')             AS declined_count,
    COUNTIF(event_type = 'refund')                                      AS refund_count,
    COUNTIF(event_type = 'chargeback')                                  AS chargeback_count,

    -- payment volume (minor units = ZAR cents)
    COALESCE(
        SUM(CASE WHEN event_type = 'payment' AND status = 'completed'
                 THEN amount_minor END),
        0
    )                                                                   AS gross_gpv_minor,
    COALESCE(
        SUM(CASE WHEN event_type = 'refund' THEN amount_minor END),
        0
    )                                                                   AS refund_volume_minor,
    COALESCE(
        SUM(CASE WHEN event_type = 'chargeback' THEN amount_minor END),
        0
    )                                                                   AS chargeback_volume_minor,
    COALESCE(
        SUM(CASE WHEN event_type = 'payment' AND status = 'completed'
                 THEN amount_minor END),
        0
    )
    - COALESCE(SUM(CASE WHEN event_type = 'refund' THEN amount_minor END), 0)
    - COALESCE(SUM(CASE WHEN event_type = 'chargeback' THEN amount_minor END), 0)
                                                                        AS net_gpv_minor,

    -- fees on completed transactions (minor units = ZAR cents)
    COALESCE(
        SUM(CASE WHEN status = 'completed' THEN interchange_fee_minor END),
        0
    )                                                                   AS total_interchange_fee_minor,
    COALESCE(
        SUM(CASE WHEN status = 'completed' THEN scheme_fee_minor END),
        0
    )                                                                   AS total_scheme_fee_minor,
    COALESCE(
        SUM(CASE WHEN status = 'completed' THEN acquirer_markup_minor END),
        0
    )                                                                   AS total_acquirer_markup_minor,
    COALESCE(
        SUM(CASE WHEN status = 'completed'
                 THEN interchange_fee_minor + scheme_fee_minor + acquirer_markup_minor END),
        0
    )                                                                   AS total_fee_minor

FROM {{ ref('stg_merchant_transactions') }}
{% if is_incremental() %}
-- 9 days = 6-day maximum settlement lag + 3-day mart lookback
WHERE event_date >= {{ incremental_cutoff(this, 'txn_date', 9) }}
{% endif %}
GROUP BY merchant_id, event_date
