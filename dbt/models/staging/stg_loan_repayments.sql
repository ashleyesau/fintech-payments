{{
    config(materialized='view')
}}

SELECT
    -- primary key
    repayment_id,

    -- foreign keys
    loan_id,
    merchant_id,

    -- schedule
    installment_number,
    due_date,
    payment_ts,

    -- amounts (minor units = ZAR cents)
    principal_due,
    interest_due,
    total_due,
    amount_paid,
    outstanding_before,
    outstanding_after,

    -- collection
    debit_method,
    mandate_id,
    mandate_status,
    collection_status,
    response_code,
    retry_attempt,

    -- arrears state
    days_past_due,
    par_bucket,
    is_cured,
    write_off_flag,

    -- envelope columns kept for dbt
    ingested_at,
    event_date

FROM {{ source('bronze', 'loan_repayments') }}
WHERE op = 'c'
