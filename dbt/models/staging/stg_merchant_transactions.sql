{{
    config(materialized='view')
}}

SELECT
    -- primary key
    transaction_id,

    -- foreign keys
    merchant_id,

    -- event classification
    event_type,
    channel,
    status,
    entry_mode,
    payment_method,
    bank_selected,

    -- amounts (minor units = ZAR cents)
    amount_minor,
    currency,
    interchange_fee_minor,
    scheme_fee_minor,
    acquirer_markup_minor,

    -- payment metadata
    mcc,
    auth_code,
    decline_code,
    risk_score,
    risk_action,
    is_test,
    correlation_id,

    -- funnel timestamps
    created_at,
    bank_selected_at,
    login_succeeded_at,
    otp_requested_at,
    otp_entered_at,
    authorized_at,
    completed_at,
    refunded_at,

    -- settlement
    settlement_date,
    batch_id,

    -- device
    device_id,
    pos_version,

    -- envelope columns kept for dbt
    ingested_at,
    event_date

FROM {{ source('bronze', 'merchant_transactions') }}
WHERE op = 'c'
