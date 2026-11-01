{{
    config(
        materialized='incremental',
        partition_by={'field': 'snapshot_date', 'data_type': 'date'},
        incremental_strategy='insert_overwrite'
    )
}}

-- Daily merchant health and risk mart. Grain: merchant_id + snapshot_date.
-- Source: int_risk__merchant_risk_daily (covers days with at least one transaction).
-- Adds is_flagged and is_gpv_declining computed health flags for Lightdash alerting.

SELECT
    merchant_id,
    snapshot_date,
    current_risk_score,
    merchant_state,
    txn_count,
    gross_gpv_minor,
    net_gpv_minor,
    risk_event_count,
    block_count,
    risk_events_rolling_30d,
    gpv_rolling_30d_minor,
    gpv_30d_trend_minor,

    -- health flag: risk score above 60 or any blocking action that day
    (current_risk_score > 60 OR block_count > 0)                            AS is_flagged,

    -- GPV declining flag: 30d rolling GPV lower than 30 days ago
    COALESCE(gpv_30d_trend_minor < 0, FALSE)                                AS is_gpv_declining

FROM {{ ref('int_risk__merchant_risk_daily') }}

{% if is_incremental() %}
WHERE snapshot_date >= (
    SELECT DATE_SUB(MAX(snapshot_date), INTERVAL 3 DAY) FROM {{ this }}
)
{% endif %}
