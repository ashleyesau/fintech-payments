{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='merge',
        partition_by={'field': 'snapshot_date', 'data_type': 'date'}
    )
}}

-- Daily portfolio quality mart. Grain: snapshot_date (one row per day).
-- Merge strategy: late repayments can shift a past date's PAR bucket, so past
-- rows may need updating. 30-day lookback ensures recent corrections are picked up.
-- Source: int_lending__portfolio_par (ephemeral CTE from int_lending__loan_daily_snapshot).

SELECT
    snapshot_date,
    active_loan_count,
    total_outstanding_minor,
    par30_balance_minor,
    par60_balance_minor,
    par90_balance_minor,
    par30_rate,
    par60_rate,
    par90_rate

FROM {{ ref('int_lending__portfolio_par') }}

{% if is_incremental() %}
WHERE snapshot_date >= (
    SELECT DATE_SUB(MAX(snapshot_date), INTERVAL 30 DAY) FROM {{ this }}
)
{% endif %}
