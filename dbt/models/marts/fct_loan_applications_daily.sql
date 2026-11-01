{{
    config(
        materialized='incremental',
        partition_by={'field': 'event_date', 'data_type': 'date'},
        incremental_strategy='insert_overwrite'
    )
}}

-- Daily loan origination mart. Grain: event_date + decision + application_channel.
-- event_date is the submission date (SAST) from stg_loan_applications.
-- Covers credit origination and approval funnel metrics.

SELECT
    event_date,
    decision,
    application_channel,

    COUNT(*)                                                                        AS application_count,
    COUNTIF(decision = 'approved')                                                  AS approved_count,
    COUNTIF(decision = 'declined')                                                  AS declined_count,
    COUNTIF(decision = 'manual_review')                                             AS manual_review_count,
    COUNTIF(decision = 'counter_offer')                                             AS counter_offer_count,
    COUNTIF(disbursed_at IS NOT NULL)                                               AS disbursed_count,

    -- approval rate among all applications
    SAFE_DIVIDE(COUNTIF(decision = 'approved'), COUNT(*))                           AS gross_approval_rate,

    -- disbursement rate: approved applications that were actually disbursed
    SAFE_DIVIDE(
        COUNTIF(disbursed_at IS NOT NULL),
        COUNTIF(decision = 'approved')
    )                                                                               AS disbursement_rate,

    -- loan amounts
    AVG(CASE WHEN decision = 'approved' THEN approved_amount END)                   AS avg_approved_amount,
    SUM(CASE WHEN disbursed_at IS NOT NULL THEN disbursement_amount_minor ELSE 0 END)
                                                                                    AS total_disbursed_minor

FROM {{ ref('stg_loan_applications') }}

{% if is_incremental() %}
WHERE event_date >= (
    SELECT DATE_SUB(MAX(event_date), INTERVAL 3 DAY) FROM {{ this }}
)
{% endif %}

GROUP BY event_date, decision, application_channel
