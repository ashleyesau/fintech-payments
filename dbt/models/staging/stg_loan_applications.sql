{{
    config(materialized='view')
}}

-- loan_applications is append-only in Bronze: both c (application INSERT) and
-- u (disbursement UPDATE) rows are present. Dedup to the latest row per
-- application_id by (commit_lsn DESC, transaction_position DESC) per SL-D4.
-- before_ columns are dropped; they are null on c rows and hold the pre-
-- disbursement state on u rows, which downstream models do not need.

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY application_id
            ORDER BY commit_lsn DESC, transaction_position DESC
        ) AS _row_number
    FROM {{ source('bronze', 'loan_applications') }}
)

SELECT
    -- primary key
    application_id,

    -- foreign keys
    merchant_id,
    applicant_user_id,
    business_id,

    -- application details
    application_channel,
    submission_ts,
    decision_ts,
    business_registration,
    months_trading,
    declared_monthly_revenue,
    requested_amount,
    requested_term_months,
    owner_credit_score,
    owner_bureau,
    actioned_by,
    decision,
    decline_reasons,

    -- approval outcome
    approved_amount,
    approved_term_months,
    total_cost_percent,

    -- disbursement (null on c rows, set on u rows)
    disbursed_at,
    disbursement_amount_minor,

    -- model metadata
    model_version_id,
    fica_status,

    -- envelope columns kept for dbt
    ingested_at,
    event_date

FROM ranked
WHERE _row_number = 1
