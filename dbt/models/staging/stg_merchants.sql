{{
    config(materialized='view')
}}

SELECT
    -- primary key
    merchant_id,
    snapshot_at,

    -- dimension attributes
    archetype,
    province,
    is_metro,
    onboarded_at,
    first_transaction_at,
    merchant_state,
    risk_score,
    loan_count,
    has_active_loan,

    -- envelope columns kept for dbt
    ingested_at,
    event_date

FROM {{ source('bronze', 'merchants') }}
WHERE op = 'c'
