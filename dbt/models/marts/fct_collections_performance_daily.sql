{{
    config(
        materialized='incremental',
        partition_by={'field': 'event_date', 'data_type': 'date'},
        incremental_strategy='insert_overwrite'
    )
}}

-- Daily collections operations mart.
-- Grain: event_date + collection_status + par_bucket.
-- event_date is the payment attempt date (SAST); NULL payment_ts rows
-- fall back to due_date via the event_date column in stg_loan_repayments.
-- collection_status values: 'success', 'unpaid_nsf', 'unpaid_account_closed',
--   'unpaid_no_authority', 'disputed', 'reversed', 'partial'.

SELECT
    event_date,
    collection_status,
    par_bucket,

    COUNT(*)                                                                AS attempt_count,
    COUNT(DISTINCT loan_id)                                                 AS loan_count,
    COUNT(DISTINCT merchant_id)                                             AS merchant_count,

    -- amounts (minor units = ZAR cents)
    SUM(total_due)                                                          AS total_due_minor,
    SUM(amount_paid)                                                        AS total_collected_minor,

    -- collection rate: amount collected / amount due
    SAFE_DIVIDE(SUM(amount_paid), SUM(total_due))                           AS collection_rate,

    -- successful collections only
    SUM(CASE WHEN collection_status = 'success' THEN amount_paid ELSE 0 END)
                                                                            AS successful_collections_minor,

    -- write-off count
    COUNTIF(write_off_flag)                                                 AS write_off_count

FROM {{ ref('stg_loan_repayments') }}

{% if is_incremental() %}
WHERE event_date >= (
    SELECT DATE_SUB(MAX(event_date), INTERVAL 3 DAY) FROM {{ this }}
)
{% endif %}

GROUP BY event_date, collection_status, par_bucket
