{{
    config(materialized='table')
}}

-- One row per disbursed loan per calendar day from disbursement date through
-- the end of the approved term (plus a 60-day buffer for late collections),
-- capped at 2026-12-31.
--
-- CALC-1 (session 30): a loan is on book from disbursement.
-- Before first instalment due date (disbursed_date + 1 month):
--   days_past_due = 0, par_bucket = 'current',
--   outstanding_principal_minor = disbursement_amount_minor,
--   collection_status = 'not_yet_due', is_written_off = FALSE.
-- From first due date onwards: use the most recent repayment row whose
-- due_date <= snapshot_date (latest installment, latest retry attempt).
-- A loan with no repayment record after the first due date retains the
-- disbursement balance and collection_status = 'not_yet_due'.

WITH loans AS (
    SELECT
        application_id                                                  AS loan_id,
        merchant_id,
        disbursement_amount_minor,
        CAST(total_cost_percent AS FLOAT64)                             AS total_cost_pct,
        approved_term_months,
        DATE(disbursed_at)                                              AS disbursed_date,
        DATE_ADD(DATE(disbursed_at), INTERVAL 1 MONTH)                  AS first_due_date
    FROM {{ ref('stg_loan_applications') }}
    WHERE disbursed_at IS NOT NULL
),

-- Date spine: one row per (loan, day) from disbursement to end-of-term + 60d
loan_dates AS (
    SELECT
        l.loan_id,
        l.merchant_id,
        l.disbursement_amount_minor,
        l.approved_term_months,
        l.disbursed_date,
        l.first_due_date,
        snapshot_date
    FROM loans l
    CROSS JOIN UNNEST(
        GENERATE_DATE_ARRAY(
            l.disbursed_date,
            LEAST(
                DATE_ADD(l.disbursed_date, INTERVAL l.approved_term_months + 2 MONTH),
                DATE('2026-12-31')
            )
        )
    ) AS snapshot_date
),

-- For each (loan, snapshot_date) find the most recent repayment row
-- with due_date <= snapshot_date. Picks the latest installment first,
-- then the latest retry_attempt within that installment.
latest_repayment AS (
    SELECT
        ld.loan_id,
        ld.snapshot_date,
        r.installment_number,
        r.due_date,
        r.outstanding_after                                             AS outstanding_principal_minor,
        r.days_past_due,
        r.par_bucket,
        r.collection_status,
        r.is_cured,
        r.write_off_flag                                                AS is_written_off,
        ROW_NUMBER() OVER (
            PARTITION BY ld.loan_id, ld.snapshot_date
            ORDER BY r.due_date DESC, r.installment_number DESC, r.retry_attempt DESC
        )                                                               AS rn
    FROM loan_dates ld
    JOIN {{ ref('stg_loan_repayments') }} r
        ON  r.loan_id      = ld.loan_id
        AND r.due_date     <= ld.snapshot_date
)

SELECT
    ld.loan_id,
    ld.merchant_id,
    ld.snapshot_date,
    ld.disbursed_date,
    ld.disbursement_amount_minor,
    ld.approved_term_months,
    ld.first_due_date,

    -- DPD: 0 before first due date, 0 when no repayment record, else from latest record
    CASE
        WHEN ld.snapshot_date < ld.first_due_date       THEN 0
        WHEN lr.days_past_due IS NULL                   THEN 0
        ELSE lr.days_past_due
    END                                                                 AS days_past_due,

    -- PAR bucket: 'current' before first due date, from latest record when available
    CASE
        WHEN ld.snapshot_date < ld.first_due_date       THEN 'current'
        WHEN lr.par_bucket IS NULL                       THEN 'current'
        ELSE lr.par_bucket
    END                                                                 AS par_bucket,

    -- Outstanding principal: disbursement amount until first repayment record
    CASE
        WHEN ld.snapshot_date < ld.first_due_date       THEN ld.disbursement_amount_minor
        WHEN lr.outstanding_principal_minor IS NULL      THEN ld.disbursement_amount_minor
        ELSE lr.outstanding_principal_minor
    END                                                                 AS outstanding_principal_minor,

    -- Collection status: 'not_yet_due' before first due date or when no record
    CASE
        WHEN ld.snapshot_date < ld.first_due_date       THEN 'not_yet_due'
        WHEN lr.collection_status IS NULL               THEN 'not_yet_due'
        ELSE lr.collection_status
    END                                                                 AS collection_status,

    -- Written off: FALSE until a repayment row sets write_off_flag = TRUE
    COALESCE(lr.is_written_off, FALSE)                                  AS is_written_off

FROM loan_dates ld
LEFT JOIN latest_repayment lr
    ON  lr.loan_id       = ld.loan_id
    AND lr.snapshot_date = ld.snapshot_date
    AND lr.rn            = 1
