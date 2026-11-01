{{
    config(
        materialized='table',
    )
}}

SELECT
    merchant_id,
    snapshot_at,
    archetype,
    province,
    is_metro,
    onboarded_at,
    first_transaction_at,
    merchant_state,
    risk_score,
    loan_count,
    has_active_loan,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_is_current

FROM {{ ref('merchants_scd') }}
WHERE dbt_is_current = TRUE
