{{ config(tags=['periodic']) }}
-- Periodic check, excluded from the per-run build. Fails if a backdated merchant
-- change means stored merchant_scd_id on old enriched rows no longer matches the
-- SCD2 version the join would pick today. Fix: full-refresh int_payments__transaction_enriched.
SELECT
    e.transaction_id,
    e.event_date,
    e.merchant_scd_id AS stored_scd_id,
    m.dbt_scd_id      AS current_scd_id
FROM {{ ref('int_payments__transaction_enriched') }} e
LEFT JOIN {{ ref('merchants_scd') }} m
    ON  e.merchant_id = m.merchant_id
    AND e.event_date >= DATE(m.dbt_valid_from)
    AND (m.dbt_valid_to IS NULL OR e.event_date < DATE(m.dbt_valid_to))
WHERE e.merchant_scd_id IS DISTINCT FROM m.dbt_scd_id
