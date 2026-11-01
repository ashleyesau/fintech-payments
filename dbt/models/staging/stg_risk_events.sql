{{
    config(materialized='view')
}}

SELECT
    -- primary key
    alert_id,

    -- entity reference
    entity_type,
    entity_id,
    merchant_id,

    -- rule details
    rule_id,
    rule_type,

    -- risk scoring
    risk_score,
    risk_score_delta,

    -- action taken
    action,
    action_by,
    action_ts,

    -- resolution (always null in the current dataset)
    resolved_ts,
    outcome,

    -- envelope columns kept for dbt
    ingested_at,
    event_date

FROM {{ source('bronze', 'risk_events') }}
WHERE op = 'c'
