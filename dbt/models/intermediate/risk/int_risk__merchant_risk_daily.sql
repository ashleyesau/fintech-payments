{{
    config(materialized='table')
}}

-- Daily risk profile per merchant. Joins daily GPV to risk event aggregates
-- and adds rolling 30-day window counts and GPV trend.
-- Grain: one row per (merchant_id, snapshot_date) where the merchant
-- processed at least one transaction that day.
--
-- Rolling windows use the same RANGE BETWEEN INTERVAL pattern as CALC-8
-- (calendar-day window, no zero-fill expansion).

WITH daily_gpv AS (
    SELECT * FROM {{ ref('int_payments__merchant_daily_gpv') }}
),

daily_risk_events AS (
    SELECT
        merchant_id,
        event_date                                                      AS snapshot_date,
        COUNT(*)                                                        AS risk_event_count,
        COUNTIF(action = 'block')                                       AS block_count,
        COUNTIF(action = 'flag')                                        AS flag_count,
        COUNTIF(action = 'review')                                      AS review_count,
        MAX(risk_score)                                                 AS max_event_risk_score,
        SUM(risk_score_delta)                                           AS total_risk_score_delta
    FROM {{ ref('stg_risk_events') }}
    GROUP BY merchant_id, event_date
),

-- Current-state merchant risk score from dimension
merchant_dim AS (
    SELECT merchant_id, risk_score, merchant_state
    FROM {{ ref('dim_merchants') }}
),

combined AS (
    SELECT
        gpv.merchant_id,
        gpv.txn_date                                                    AS snapshot_date,
        gpv.txn_count,
        gpv.gross_gpv_minor,
        gpv.net_gpv_minor,
        COALESCE(re.risk_event_count, 0)                                AS risk_event_count,
        COALESCE(re.block_count, 0)                                     AS block_count,
        COALESCE(re.flag_count, 0)                                      AS flag_count,
        COALESCE(re.review_count, 0)                                    AS review_count,
        re.max_event_risk_score,
        COALESCE(re.total_risk_score_delta, 0)                          AS total_risk_score_delta,
        m.risk_score                                                    AS current_risk_score,
        m.merchant_state
    FROM daily_gpv gpv
    LEFT JOIN daily_risk_events re
        ON  gpv.merchant_id = re.merchant_id
        AND gpv.txn_date    = re.snapshot_date
    LEFT JOIN merchant_dim m
        ON  gpv.merchant_id = m.merchant_id
),

windowed AS (
    SELECT
        merchant_id,
        snapshot_date,
        txn_count,
        gross_gpv_minor,
        net_gpv_minor,
        risk_event_count,
        block_count,
        flag_count,
        review_count,
        max_event_risk_score,
        total_risk_score_delta,
        current_risk_score,
        merchant_state,

        -- rolling 30-day risk event count (calendar-day window)
        SUM(risk_event_count) OVER (
            PARTITION BY merchant_id
            ORDER BY snapshot_date
            RANGE BETWEEN INTERVAL 29 DAY PRECEDING AND CURRENT ROW
        )                                                               AS risk_events_rolling_30d,

        -- rolling 30-day GPV for trend calculation
        SUM(gross_gpv_minor) OVER (
            PARTITION BY merchant_id
            ORDER BY snapshot_date
            RANGE BETWEEN INTERVAL 29 DAY PRECEDING AND CURRENT ROW
        )                                                               AS gpv_rolling_30d_minor

    FROM combined
)

SELECT
    merchant_id,
    snapshot_date,
    txn_count,
    gross_gpv_minor,
    net_gpv_minor,
    risk_event_count,
    block_count,
    flag_count,
    review_count,
    max_event_risk_score,
    total_risk_score_delta,
    current_risk_score,
    merchant_state,
    risk_events_rolling_30d,
    gpv_rolling_30d_minor,

    -- GPV trend: change in 30d rolling GPV vs 30 days prior
    -- NULL for the first 30 days of each merchant's history
    gpv_rolling_30d_minor - LAG(gpv_rolling_30d_minor, 30) OVER (
        PARTITION BY merchant_id
        ORDER BY snapshot_date
    )                                                                   AS gpv_30d_trend_minor

FROM windowed
