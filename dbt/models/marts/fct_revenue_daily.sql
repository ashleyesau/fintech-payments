{{
    config(
        materialized='incremental',
        partition_by={'field': 'revenue_date', 'data_type': 'date'},
        incremental_strategy='insert_overwrite'
    )
}}

-- Daily platform revenue mart. Grain: revenue_date + revenue_stream + merchant_id.
-- CALC-4: two streams combined with UNION ALL; revenue_stream discriminates them.
--   'card_acquiring' -- posts on transaction date (event_date); amount = acquirer_markup.
--   'loan_fees'      -- posts on the first day of each fee month; amount = straight-line fee.
-- The two streams are independent: a merchant can have both on the same date.

WITH card_revenue AS (
    SELECT
        merchant_id,
        event_date                                                  AS revenue_date,
        'card_acquiring'                                            AS revenue_stream,
        SUM(acquirer_markup_minor)                                  AS revenue_minor
    FROM {{ ref('int_payments__transaction_enriched') }}
    WHERE is_gpv
    GROUP BY merchant_id, event_date
),

loan_revenue AS (
    SELECT
        merchant_id,
        -- CALC-4: loan fees post on the first day of each calendar fee month
        fee_month                                                   AS revenue_date,
        'loan_fees'                                                 AS revenue_stream,
        SUM(monthly_fee_minor)                                      AS revenue_minor
    FROM {{ ref('int_lending__loan_fee_schedule') }}
    GROUP BY merchant_id, fee_month
),

combined AS (
    SELECT * FROM card_revenue
    UNION ALL
    SELECT * FROM loan_revenue
)

SELECT * FROM combined

{% if is_incremental() %}
WHERE revenue_date >= (
    SELECT DATE_SUB(MAX(revenue_date), INTERVAL 3 DAY) FROM {{ this }}
)
{% endif %}
