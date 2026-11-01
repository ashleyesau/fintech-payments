{{
    config(materialized='ephemeral')
}}

-- PAR30/60/90 aggregation at portfolio level by snapshot date.
-- Ephemeral: compiles to a CTE inline in fct_loan_portfolio_daily.
-- Written-off loans are excluded from both numerator and denominator
-- (standard PAR calculation convention).
--
-- Grain: one row per snapshot_date.

SELECT
    snapshot_date,

    COUNT(DISTINCT loan_id)                                             AS active_loan_count,

    SUM(outstanding_principal_minor)                                    AS total_outstanding_minor,

    -- PAR bucket balances
    SUM(
        CASE WHEN days_past_due >= 30
             THEN outstanding_principal_minor ELSE 0 END
    )                                                                   AS par30_balance_minor,

    SUM(
        CASE WHEN days_past_due >= 60
             THEN outstanding_principal_minor ELSE 0 END
    )                                                                   AS par60_balance_minor,

    SUM(
        CASE WHEN days_past_due >= 90
             THEN outstanding_principal_minor ELSE 0 END
    )                                                                   AS par90_balance_minor,

    -- PAR rates (NULL when total outstanding is zero)
    SAFE_DIVIDE(
        SUM(CASE WHEN days_past_due >= 30 THEN outstanding_principal_minor ELSE 0 END),
        SUM(outstanding_principal_minor)
    )                                                                   AS par30_rate,

    SAFE_DIVIDE(
        SUM(CASE WHEN days_past_due >= 60 THEN outstanding_principal_minor ELSE 0 END),
        SUM(outstanding_principal_minor)
    )                                                                   AS par60_rate,

    SAFE_DIVIDE(
        SUM(CASE WHEN days_past_due >= 90 THEN outstanding_principal_minor ELSE 0 END),
        SUM(outstanding_principal_minor)
    )                                                                   AS par90_rate

FROM {{ ref('int_lending__loan_daily_snapshot') }}
WHERE NOT is_written_off
GROUP BY snapshot_date
