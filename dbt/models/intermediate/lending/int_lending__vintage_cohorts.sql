{{
    config(materialized='ephemeral')
}}

-- Vintage cohort structure for lending return analysis.
-- Ephemeral: compiles to a CTE inline in fct_vintage_curves.
-- Grain: one row per (loan_id, month_end) where month_end is the last day
-- of each calendar month in the loan's life. Month-end snapshots are standard
-- for vintage curve analysis.
--
-- month_on_book (MOB): how many full months the loan has been on book since
-- disbursement. MOB 0 = disbursement month; MOB 1 = first full month; etc.
-- vintage_month: DATE_TRUNC(disbursed_date, MONTH) -- the cohort the loan belongs to.
-- pct_outstanding: outstanding / disbursement -- how much principal is left.

SELECT
    loan_id,
    merchant_id,
    snapshot_date,
    disbursed_date,
    disbursement_amount_minor,
    outstanding_principal_minor,
    days_past_due,
    par_bucket,
    is_written_off,
    collection_status,

    DATE_TRUNC(disbursed_date, MONTH)                                   AS vintage_month,

    DATE_DIFF(snapshot_date, disbursed_date, MONTH)                     AS month_on_book,

    SAFE_DIVIDE(outstanding_principal_minor, disbursement_amount_minor) AS pct_outstanding

FROM {{ ref('int_lending__loan_daily_snapshot') }}
WHERE snapshot_date = LAST_DAY(snapshot_date, MONTH)
