{{
    config(
        materialized='incremental',
        partition_by={'field': 'event_date', 'data_type': 'date'},
        incremental_strategy='insert_overwrite'
    )
}}

-- Daily fraud and risk event mart. Grain: event_date + rule_type + archetype (CALC-7).
-- Joins to dim_merchants for current archetype; archetype is stable enough for
-- aggregation at daily grain. Columns: event_count, action breakdown, outcome
-- breakdown, merchant_count_affected (COUNT DISTINCT).
-- action values from migration CHECK: 'allow', 'flag_review', 'block'.
-- outcome is always null in the current dataset; included for future use.

SELECT
    r.event_date,
    r.rule_type,
    m.archetype,

    -- CALC-7: event count and affected merchant count
    COUNT(*)                                                            AS event_count,
    COUNT(DISTINCT r.merchant_id)                                       AS merchant_count_affected,

    -- CALC-7: action breakdown (migration CHECK values)
    COUNTIF(r.action = 'block')                                         AS block_count,
    COUNTIF(r.action = 'flag_review')                                   AS flag_review_count,
    COUNTIF(r.action = 'allow')                                         AS allow_count,

    -- CALC-7: outcome breakdown (always null in current data; reserved for future)
    COUNTIF(r.outcome = 'resolved')                                     AS resolved_count,
    COUNTIF(r.outcome IS NULL)                                          AS unresolved_count,

    -- risk score context
    AVG(r.risk_score)                                                   AS avg_risk_score,
    SUM(r.risk_score_delta)                                             AS total_risk_score_delta

FROM {{ ref('stg_risk_events') }} r
LEFT JOIN {{ ref('dim_merchants') }} m
    ON r.merchant_id = m.merchant_id

{% if is_incremental() %}
WHERE r.event_date >= (
    SELECT DATE_SUB(MAX(event_date), INTERVAL 3 DAY) FROM {{ this }}
)
{% endif %}

GROUP BY r.event_date, r.rule_type, m.archetype
