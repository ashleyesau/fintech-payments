{{
    config(
        materialized='incremental',
        partition_by={'field': 'settlement_date', 'data_type': 'date'},
        incremental_strategy='insert_overwrite'
    )
}}

-- Daily settlement reconciliation mart. Grain: merchant_id + settlement_date + batch_id + channel.
-- CALC-3:
--   net_payout_minor = gross_gpv - merchant_fee - refund_volume - chargeback_volume
--   acquirer_revenue_minor = SUM(acquirer_markup_minor) on completed payments
--   merchant_fee = interchange + scheme fees (costs borne by the merchant)
--   channel (rail) determines settlement timing; stored directly from the transaction.

SELECT
    merchant_id,
    settlement_date,
    batch_id,
    channel,

    COUNT(*)                                                                    AS txn_count,
    COUNTIF(is_gpv)                                                             AS completed_payment_count,

    -- gross GPV: completed payment amounts only
    SUM(CASE WHEN is_gpv    THEN amount_minor    ELSE 0 END)                    AS gross_gpv_minor,
    SUM(CASE WHEN is_refund THEN amount_minor     ELSE 0 END)                   AS refund_volume_minor,
    SUM(CASE WHEN is_chargeback THEN amount_minor ELSE 0 END)                   AS chargeback_volume_minor,

    -- CALC-3: merchant_fee = interchange + scheme (costs paid by merchant)
    SUM(CASE WHEN is_gpv THEN interchange_fee_minor + scheme_fee_minor
                          ELSE 0 END)                                           AS merchant_fee_minor,

    -- CALC-3: net_payout = gross_gpv - merchant_fee - refund_volume - chargeback_volume
    SUM(CASE WHEN is_gpv THEN amount_minor ELSE 0 END)
        - SUM(CASE WHEN is_gpv THEN interchange_fee_minor + scheme_fee_minor ELSE 0 END)
        - SUM(CASE WHEN is_refund     THEN amount_minor ELSE 0 END)
        - SUM(CASE WHEN is_chargeback THEN amount_minor ELSE 0 END)             AS net_payout_minor,

    -- CALC-3: acquirer_revenue = SUM(acquirer_markup) on completed payments
    SUM(CASE WHEN is_gpv THEN acquirer_markup_minor ELSE 0 END)                 AS acquirer_revenue_minor

FROM {{ ref('int_payments__transaction_enriched') }}
WHERE settlement_date IS NOT NULL

{% if is_incremental() %}
AND settlement_date >= (
    SELECT DATE_SUB(MAX(settlement_date), INTERVAL 3 DAY) FROM {{ this }}
)
{% endif %}

GROUP BY merchant_id, settlement_date, batch_id, channel
