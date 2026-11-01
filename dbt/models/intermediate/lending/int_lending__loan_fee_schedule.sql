{{
    config(materialized='table')
}}

-- One row per disbursed loan per calendar month of the approved term.
-- Straight-line fee amortisation as per CALC-2 (session 30):
--   monthly_fee_minor = ROUND(total_cost_percent * disbursement_amount_minor
--                             / approved_term_months)
-- Month range: disbursement month (month_number = 1) through
--              disbursement month + approved_term_months - 1 (month_number = N).
-- Used by fct_revenue_daily and fct_loan_unit_economics.
--
-- Columns: loan_id, merchant_id, fee_month, month_number, monthly_fee_minor,
--          cohort_date (DATE_TRUNC of disbursement to month).

WITH loans AS (
    SELECT
        application_id                                                  AS loan_id,
        merchant_id,
        CAST(total_cost_percent AS FLOAT64)                             AS total_cost_pct,
        disbursement_amount_minor,
        approved_term_months,
        DATE_TRUNC(DATE(disbursed_at), MONTH)                           AS cohort_date
    FROM {{ ref('stg_loan_applications') }}
    WHERE disbursed_at IS NOT NULL
),

-- One row per (loan, month_offset) where month_offset runs 0 .. approved_term_months - 1
fee_months AS (
    SELECT
        l.loan_id,
        l.merchant_id,
        l.total_cost_pct,
        l.disbursement_amount_minor,
        l.approved_term_months,
        l.cohort_date,
        DATE_ADD(l.cohort_date, INTERVAL month_offset MONTH)            AS fee_month,
        month_offset + 1                                                AS month_number
    FROM loans l
    CROSS JOIN UNNEST(
        GENERATE_ARRAY(0, l.approved_term_months - 1)
    ) AS month_offset
)

SELECT
    loan_id,
    merchant_id,
    fee_month,
    month_number,
    CAST(
        ROUND(total_cost_pct * disbursement_amount_minor / approved_term_months)
        AS INT64
    )                                                                   AS monthly_fee_minor,
    cohort_date
FROM fee_months
