-- The incremental lookback (9 days) = max settlement lag + 3-day mart lookback.
-- Fails if any recent event settles later than the assumed maximum lag.
-- Checks the last 30 days of event_date so each run validates newly arrived data.
SELECT
    transaction_id,
    event_date,
    settlement_date,
    DATE_DIFF(settlement_date, event_date, DAY) AS lag_days
FROM {{ ref('int_payments__transaction_enriched') }}
WHERE settlement_date IS NOT NULL
  AND event_date >= {{ incremental_cutoff(ref('int_payments__transaction_enriched'), 'event_date', 30) }}
  AND DATE_DIFF(settlement_date, event_date, DAY) > {{ var('max_settlement_lag_days', 6) }}
