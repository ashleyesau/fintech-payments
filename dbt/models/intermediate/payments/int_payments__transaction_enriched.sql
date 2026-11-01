{{
    config(materialized='table')
}}

-- Join every payment event to the merchant SCD2 record that was current on that
-- event's SAST date. Adds merchant-at-the-time context, pre-computed fee and GPV
-- flags, and funnel timing columns. Used by four mart models.
--
-- SCD2 join: event_date >= DATE(dbt_valid_from) AND
--            (dbt_valid_to IS NULL OR event_date < DATE(dbt_valid_to))
-- Both comparisons use DATE() which converts the stored UTC timestamp to a UTC date.
-- Merchant snapshots fire at 23:59 SAST = 21:59 UTC, so the UTC date equals the
-- SAST date for all rows in this dataset.

WITH transactions AS (
    SELECT * FROM {{ ref('stg_merchant_transactions') }}
),

merchants AS (
    SELECT * FROM {{ ref('merchants_scd') }}
)

SELECT
    -- transaction identity
    t.transaction_id,
    t.merchant_id,

    -- event classification
    t.event_type,
    t.channel,
    t.status,
    t.entry_mode,
    t.payment_method,
    t.bank_selected,

    -- amounts (minor units = ZAR cents)
    t.amount_minor,
    t.currency,
    t.interchange_fee_minor,
    t.scheme_fee_minor,
    t.acquirer_markup_minor,
    t.interchange_fee_minor
        + t.scheme_fee_minor
        + t.acquirer_markup_minor                       AS total_fee_minor,

    -- payment metadata
    t.mcc,
    t.auth_code,
    t.decline_code,
    t.risk_score                                        AS txn_risk_score,
    t.risk_action,
    t.is_test,
    t.correlation_id,

    -- GPV flags
    t.status = 'completed'
        AND t.event_type = 'payment'                    AS is_gpv,
    t.event_type = 'refund'                             AS is_refund,
    t.event_type = 'chargeback'                         AS is_chargeback,

    -- funnel timestamps
    t.created_at,
    t.bank_selected_at,
    t.login_succeeded_at,
    t.otp_requested_at,
    t.otp_entered_at,
    t.authorized_at,
    t.completed_at,
    t.refunded_at,

    -- funnel timing (seconds; NULL when step did not occur)
    TIMESTAMP_DIFF(t.bank_selected_at,    t.created_at,      SECOND) AS secs_to_bank_selection,
    TIMESTAMP_DIFF(t.otp_requested_at,    t.bank_selected_at, SECOND) AS secs_to_otp_request,
    TIMESTAMP_DIFF(t.otp_entered_at,      t.otp_requested_at, SECOND) AS secs_to_otp_entry,
    TIMESTAMP_DIFF(t.authorized_at,       t.created_at,       SECOND) AS secs_to_authorization,
    TIMESTAMP_DIFF(t.completed_at,        t.created_at,       SECOND) AS secs_to_completion,

    -- settlement
    t.settlement_date,
    t.batch_id,

    -- device
    t.device_id,
    t.pos_version,

    -- envelope
    t.event_date,
    t.ingested_at,

    -- merchant dimension at time of transaction (from SCD2)
    m.dbt_scd_id                                        AS merchant_scd_id,
    m.archetype,
    m.province,
    m.is_metro,
    m.merchant_state,
    m.risk_score                                        AS merchant_risk_score,
    m.loan_count,
    m.has_active_loan

FROM transactions t
LEFT JOIN merchants m
    ON  t.merchant_id = m.merchant_id
    AND t.event_date >= DATE(m.dbt_valid_from)
    AND (m.dbt_valid_to IS NULL OR t.event_date < DATE(m.dbt_valid_to))
