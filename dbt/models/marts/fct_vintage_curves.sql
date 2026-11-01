{{
    config(
        materialized='incremental',
        unique_key=['vintage_month', 'month_on_book'],
        incremental_strategy='merge',
        partition_by={'field': 'vintage_month', 'data_type': 'date'}
    )
}}

-- Vintage curve analysis mart. Grain: vintage_month + month_on_book.
-- Aggregates all loans in a cohort at each month on book (MOB).
-- Source: int_lending__vintage_cohorts (ephemeral, month-end snapshots only).
-- Merge: MOB values for recent cohorts update as new month-end snapshots arrive.
--
-- vintage_month: DATE_TRUNC(disbursed_date, MONTH) -- the origination cohort.
-- month_on_book: months since disbursement (0 = disbursement month).
-- pct_outstanding: portfolio balance / original disbursement (cumulative repayment proxy).

SELECT
    vintage_month,
    month_on_book,
    COUNT(DISTINCT loan_id)                                                     AS loan_count,
    SUM(disbursement_amount_minor)                                              AS total_disbursed_minor,
    SUM(outstanding_principal_minor)                                            AS total_outstanding_minor,
    SAFE_DIVIDE(
        SUM(outstanding_principal_minor),
        SUM(disbursement_amount_minor)
    )                                                                           AS pct_outstanding,
    COUNTIF(days_past_due >= 30)                                                AS par30_loan_count,
    COUNTIF(days_past_due >= 60)                                                AS par60_loan_count,
    COUNTIF(days_past_due >= 90)                                                AS par90_loan_count,
    COUNTIF(is_written_off)                                                     AS written_off_count,
    -- cumulative PAR30 rate by balance
    SAFE_DIVIDE(
        SUM(CASE WHEN days_past_due >= 30 THEN outstanding_principal_minor ELSE 0 END),
        SUM(disbursement_amount_minor)
    )                                                                           AS par30_rate_by_balance

FROM {{ ref('int_lending__vintage_cohorts') }}

{% if is_incremental() %}
WHERE vintage_month >= (
    SELECT DATE_SUB(MAX(vintage_month), INTERVAL 3 MONTH) FROM {{ this }}
)
{% endif %}

GROUP BY vintage_month, month_on_book
