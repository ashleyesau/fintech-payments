{{ config(tags=['periodic']) }}
-- Catches late arrivals outside the incremental window: events present in staging whose
-- event_date partition in enriched was never rebuilt. Row counts per event_date must match.
WITH s AS (
    SELECT event_date, COUNT(*) AS n FROM {{ ref('stg_merchant_transactions') }} GROUP BY 1
),
e AS (
    SELECT event_date, COUNT(*) AS n FROM {{ ref('int_payments__transaction_enriched') }} GROUP BY 1
)
SELECT
    COALESCE(s.event_date, e.event_date) AS event_date,
    s.n AS staging_rows,
    e.n AS enriched_rows
FROM s FULL OUTER JOIN e USING (event_date)
WHERE COALESCE(s.n, 0) != COALESCE(e.n, 0)
