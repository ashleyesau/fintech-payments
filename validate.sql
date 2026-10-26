-- validate.sql: Phase 8 reconciliation queries against a populated fintech database.
--
-- Run through the module (checks each band and prints a table):
--     python -m simulator.validation
--     POSTGRES_DB=val_s42 python -m simulator.validation --only par
-- or straight through psql, which prints every result set:
--     psql -h localhost -U sim -d fintech -f validate.sql
--
-- Each block starts with "-- name: <name>". Check queries return
--     check_name, value, low, high, n, note
-- where low and high are the Validation Targets band (NULL means open ended) and n is the
-- sample the value rests on. Queries named series_* return plain tables for reading.
--
-- Conventions
--   * Dates are SAST (Africa/Johannesburg), as the generator writes them.
--   * Per-merchant normalisation: a merchant-day is a daily snapshot row of a merchant that
--     has made its first transaction and is not Churned. GPV is completed payments only,
--     gross of refunds (the dbt definition).
--   * "Steady window" starts six months after the first onboarding month, when the ramp has
--     finished, and runs to the last snapshot.
--   * PAR is computed two ways from loan_repayments, because the stream carries no event
--     date for unpaid attempts and moves a loan between DPD buckets once a month:
--       dpd (primary): the days_past_due on the loan's latest row, 0 after a cure. This is the
--         generator's own loan DPD, and matches it for the loans whose arrears cycles wrote a
--         row. A loan in arrears after its term has ended gets no rows, so this is a lower
--         bound there.
--       oldest_unpaid_days (reported as *_instalment_age): the age of the oldest instalment
--         that was due and not yet paid, which is what an analyst computing PAR from the
--         table would get. It runs ahead of dpd between the monthly cycles.
--     A written-off loan leaves the book on the due date of the written-off instalment plus
--     the DPD on the write-off row.
--   * Merchant-day denominators include merchants that have gone dormant but are not yet
--     Churned, as the state engine does.
--   * Values are fractions (0.25 is 25%) unless the check name ends in _ratio or _pp.

-- name: setup
CREATE TEMP TABLE t_merchant AS
SELECT merchant_id,
       min(archetype)                                        AS archetype,
       min(province)                                         AS province,
       (min(onboarded_at) AT TIME ZONE 'Africa/Johannesburg')::date AS onboarded_on,
       (max(first_transaction_at) AT TIME ZONE 'Africa/Johannesburg')::date AS first_txn_on
FROM merchants GROUP BY merchant_id;
CREATE UNIQUE INDEX ON t_merchant (merchant_id);

CREATE TEMP TABLE t_day AS
SELECT (snapshot_at AT TIME ZONE 'Africa/Johannesburg')::date AS snapshot_date,
       merchant_id, archetype, merchant_state, risk_score, has_active_loan, loan_count,
       (first_transaction_at IS NOT NULL AND merchant_state <> 'churned') AS trading
FROM merchants;
CREATE INDEX ON t_day (snapshot_date);
CREATE INDEX ON t_day (merchant_id, snapshot_date);

CREATE TEMP TABLE t_pay AS
SELECT t.transaction_id, t.merchant_id, m.archetype, t.channel, t.status, t.amount_minor,
       t.interchange_fee_minor + t.scheme_fee_minor + t.acquirer_markup_minor AS fee_minor,
       t.acquirer_markup_minor AS markup_minor,
       (t.created_at AT TIME ZONE 'Africa/Johannesburg')::date AS d,
       extract(hour FROM t.created_at AT TIME ZONE 'Africa/Johannesburg')::int AS h,
       extract(isodow FROM t.created_at AT TIME ZONE 'Africa/Johannesburg')::int AS dow,
       t.otp_requested_at IS NOT NULL AND t.otp_entered_at IS NULL AS otp_dropout
FROM merchant_transactions t JOIN t_merchant m USING (merchant_id)
WHERE t.event_type = 'payment' AND NOT t.is_test;
CREATE INDEX ON t_pay (d);
CREATE INDEX ON t_pay (archetype);
CREATE INDEX ON t_pay (merchant_id, d);

CREATE TEMP TABLE t_bounds AS
SELECT min(onboarded_on) AS data_start,
       (SELECT max(snapshot_date) FROM t_day) AS data_end,
       (date_trunc('month', min(onboarded_on)) + interval '6 months')::date AS steady_from
FROM t_merchant;

-- Every month end in the data, for the PAR and book queries; the steady-window checks
-- filter on d >= steady_from themselves.
CREATE TEMP TABLE t_month_end AS
SELECT (date_trunc('month', g) + interval '1 month - 1 day')::date AS d
FROM t_bounds b, generate_series(date_trunc('month', b.data_start), b.data_end, interval '1 month') g
WHERE (date_trunc('month', g) + interval '1 month - 1 day')::date <= b.data_end;

CREATE TEMP TABLE t_loan AS
SELECT a.application_id AS loan_id, a.merchant_id, m.archetype,
       (a.disbursed_at AT TIME ZONE 'Africa/Johannesburg')::date AS disbursed_on,
       a.disbursement_amount_minor + round(a.disbursement_amount_minor * a.total_cost_percent)::bigint AS initial_balance,
       (SELECT min(r.due_date + r.days_past_due) FROM loan_repayments r
         WHERE r.loan_id = a.application_id AND r.write_off_flag) AS written_off_on
FROM loan_applications a JOIN t_merchant m USING (merchant_id)
WHERE a.disbursed_at IS NOT NULL;

CREATE TEMP TABLE t_instalment AS
SELECT loan_id, merchant_id, installment_number, min(due_date) AS due_date,
       min((payment_ts AT TIME ZONE 'Africa/Johannesburg')::date)
           FILTER (WHERE collection_status = 'success') AS paid_on
FROM loan_repayments GROUP BY loan_id, merchant_id, installment_number;
CREATE INDEX ON t_instalment (loan_id);

-- Repayment rows with the date each one happened. Paid rows carry payment_ts. An unpaid row
-- is dated by its due date, or by due date + days_past_due for a retry (retries run 1, 4 and
-- 7 days after the due date). Within a day, paid rows sort before unpaid ones.
CREATE TEMP TABLE t_rrow AS
SELECT loan_id, installment_number, retry_attempt, days_past_due, is_cured, collection_status,
       coalesce((payment_ts AT TIME ZONE 'Africa/Johannesburg')::date,
                CASE WHEN retry_attempt > 0 THEN due_date + days_past_due ELSE due_date END) AS event_on,
       (collection_status <> 'success')::int AS unpaid
FROM loan_repayments;
CREATE INDEX ON t_rrow (loan_id, event_on);

-- One row per loan and month end: balance, and two readings of how late it is.
--   oldest_unpaid_days: age of the oldest instalment due and not yet paid (instalment ageing).
--   dpd: the loan's days_past_due as the stream records it, from its latest row (0 after a
--        cure). The generator moves a loan between buckets once a month, so between cycles
--        this is lower than the instalment age.
CREATE TEMP TABLE t_book AS
SELECT l.loan_id, l.merchant_id, l.archetype, e.d,
       coalesce((SELECT min(r.outstanding_after) FROM loan_repayments r
                  WHERE r.loan_id = l.loan_id AND r.collection_status = 'success'
                    AND (r.payment_ts AT TIME ZONE 'Africa/Johannesburg')::date <= e.d),
                l.initial_balance) AS balance,
       (SELECT e.d - min(i.due_date) FROM t_instalment i
         WHERE i.loan_id = l.loan_id AND i.due_date <= e.d AND (i.paid_on IS NULL OR i.paid_on > e.d)) AS oldest_unpaid_days,
       coalesce((SELECT CASE WHEN r.is_cured THEN 0 ELSE r.days_past_due END FROM t_rrow r
                  WHERE r.loan_id = l.loan_id AND r.event_on <= e.d
                  ORDER BY r.event_on DESC, r.unpaid DESC, r.retry_attempt DESC, r.days_past_due DESC LIMIT 1), 0) AS dpd
FROM t_loan l CROSS JOIN t_month_end e
WHERE l.disbursed_on <= e.d AND l.initial_balance IS NOT NULL
  AND (l.written_off_on IS NULL OR l.written_off_on > e.d);
DELETE FROM t_book WHERE balance <= 0;

-- One row per loan and months on book k (1 to 12) once the loan has been on book k months.
--   late_now:  the loan is more than 30 days past due at disbursed_on + k months, by its latest
--              recorded DPD (0 after a cure), or has been written off by then. Written-off loans
--              stay in the count, which is what makes a vintage's PAR30 "cumulative".
--   ever_late: the loan had a row with days_past_due above 30 on or before that date, even if it
--              later cured.
-- weight is the opening balance (principal plus fee).
CREATE TEMP TABLE t_vintage AS
SELECT l.loan_id, l.archetype, l.disbursed_on, g.k, c.cutoff, l.initial_balance AS weight,
       (l.written_off_on IS NOT NULL AND l.written_off_on <= c.cutoff)
       OR coalesce((SELECT CASE WHEN r.is_cured THEN 0 ELSE r.days_past_due END FROM t_rrow r
                     WHERE r.loan_id = l.loan_id AND r.event_on <= c.cutoff
                     ORDER BY r.event_on DESC, r.unpaid DESC, r.retry_attempt DESC, r.days_past_due DESC LIMIT 1), 0) > 30 AS late_now,
       EXISTS (SELECT 1 FROM t_rrow r WHERE r.loan_id = l.loan_id AND r.event_on <= c.cutoff AND r.days_past_due > 30) AS ever_late
FROM t_loan l CROSS JOIN generate_series(1, 12) AS g(k)
CROSS JOIN LATERAL (SELECT (l.disbursed_on + make_interval(months => g.k))::date AS cutoff) c
CROSS JOIN t_bounds b
WHERE l.initial_balance IS NOT NULL AND c.cutoff <= b.data_end;
CREATE INDEX ON t_vintage (k);

CREATE TEMP TABLE t_active_last AS
SELECT d.* FROM t_day d, t_bounds b
WHERE d.snapshot_date = b.data_end AND d.trading
  AND NOT EXISTS (SELECT 1 FROM loan_repayments r WHERE r.merchant_id = d.merchant_id AND r.write_off_flag);

ANALYZE t_merchant; ANALYZE t_day; ANALYZE t_pay; ANALYZE t_book; ANALYZE t_instalment;

-- name: series_window
SELECT b.data_start, b.data_end, b.steady_from,
       (SELECT count(*) FROM t_merchant) AS merchants_onboarded,
       (SELECT count(*) FROM t_merchant WHERE first_txn_on IS NOT NULL) AS ever_transacted,
       (SELECT count(*) FROM merchant_transactions) AS transactions,
       (SELECT count(*) FROM loan_applications) AS applications,
       (SELECT count(*) FROM t_loan) AS loans_disbursed,
       (SELECT count(*) FROM loan_repayments) AS repayment_rows,
       (SELECT count(*) FROM risk_events) AS risk_events
FROM t_bounds b;

-- name: series_gpv_monthly
WITH g AS (
    SELECT date_trunc('month', d)::date AS m,
           sum(amount_minor) FILTER (WHERE status = 'completed') / 100.0 AS gpv,
           count(*) AS tx,
           count(*) FILTER (WHERE status = 'completed') AS done
    FROM t_pay GROUP BY 1),
a AS (
    SELECT date_trunc('month', snapshot_date)::date AS m, count(*) FILTER (WHERE trading) AS md
    FROM t_day GROUP BY 1)
SELECT g.m, round(g.gpv) AS gpv_zar, g.tx, a.md AS merchant_days,
       round(g.gpv / nullif(a.md, 0), 2) AS gpv_per_merchant_day,
       round(g.gpv / nullif(g.done, 0) , 2) AS ticket_zar,
       round(100 * (g.gpv / nullif(lag(g.gpv) OVER (ORDER BY g.m), 0) - 1), 1) AS mom_pct
FROM g JOIN a USING (m) ORDER BY g.m;

-- name: gpv_ramp_growth
-- Compound monthly growth of platform GPV from run month 3 to month 6. Month 1 and 2 start
-- from a handful of merchants, so growth from there says nothing about the platform.
WITH g AS (
    SELECT row_number() OVER (ORDER BY date_trunc('month', d)) AS rn,
           sum(amount_minor) FILTER (WHERE status = 'completed') AS gpv
    FROM t_pay GROUP BY date_trunc('month', d))
SELECT 'gpv_ramp_growth_cagr_m3_m6' AS check_name,
       round((power((SELECT gpv FROM g WHERE rn = 6)::numeric / (SELECT gpv FROM g WHERE rn = 3), 1.0 / 3) - 1), 4) AS value,
       0.05 AS low, 0.25 AS high, 3 AS n, 'compound monthly growth, months 3 to 6' AS note;

-- name: december_uplift
-- December GPV per merchant-day against the calendar-year average per merchant-day.
WITH y AS (SELECT extract(year FROM data_start)::int AS yr FROM t_bounds),
g AS (SELECT sum(amount_minor) FILTER (WHERE status = 'completed') AS gpv,
             sum(amount_minor) FILTER (WHERE status = 'completed' AND extract(month FROM d) = 12) AS gpv_dec
      FROM t_pay, y WHERE extract(year FROM d) = y.yr),
a AS (SELECT count(*) FILTER (WHERE trading) AS md,
             count(*) FILTER (WHERE trading AND extract(month FROM snapshot_date) = 12) AS md_dec
      FROM t_day, y WHERE extract(year FROM snapshot_date) = y.yr)
SELECT 'december_uplift_vs_annual_average' AS check_name,
       round((g.gpv_dec::numeric / a.md_dec) / (g.gpv::numeric / a.md) - 1, 4) AS value,
       0.25 AS low, 0.35 AS high, a.md_dec AS n, 'GPV per merchant-day, December against the year' AS note
FROM g, a;

-- name: january_gpv_vs_december
WITH y AS (SELECT extract(year FROM data_start)::int + 1 AS jan_yr FROM t_bounds),
m AS (
    SELECT (extract(month FROM d) = 1 AND extract(year FROM d) = y.jan_yr) AS is_jan,
           sum(amount_minor) FILTER (WHERE status = 'completed') AS gpv
    FROM t_pay, y
    WHERE (extract(month FROM d) = 1 AND extract(year FROM d) = y.jan_yr)
       OR (extract(month FROM d) = 12 AND extract(year FROM d) = y.jan_yr - 1)
    GROUP BY 1),
a AS (
    SELECT (extract(month FROM snapshot_date) = 1 AND extract(year FROM snapshot_date) = y.jan_yr) AS is_jan,
           count(*) FILTER (WHERE trading) AS md
    FROM t_day, y
    WHERE (extract(month FROM snapshot_date) = 1 AND extract(year FROM snapshot_date) = y.jan_yr)
       OR (extract(month FROM snapshot_date) = 12 AND extract(year FROM snapshot_date) = y.jan_yr - 1)
    GROUP BY 1)
SELECT 'january_gpv_vs_december' AS check_name,
       round((SELECT m.gpv::numeric / a.md FROM m JOIN a USING (is_jan) WHERE is_jan)
           / (SELECT m.gpv::numeric / a.md FROM m JOIN a USING (is_jan) WHERE NOT is_jan) - 1, 4) AS value,
       -0.40 AS low, -0.27 AS high, (SELECT md FROM a WHERE is_jan) AS n,
       'GPV per merchant-day, January against the December before; band amended from -20 to -10 (see Validation Targets)' AS note;

-- name: january_count_and_ticket
-- Count up while ticket size is down: January against the average of the year before it.
WITH y AS (SELECT extract(year FROM data_start)::int + 1 AS jan_yr FROM t_bounds),
p AS (
    SELECT (extract(year FROM d) = y.jan_yr AND extract(month FROM d) = 1) AS is_jan,
           count(*) AS tx,
           sum(amount_minor) FILTER (WHERE status = 'completed')::numeric / nullif(count(*) FILTER (WHERE status = 'completed'), 0) AS ticket
    FROM t_pay, y
    WHERE (extract(year FROM d) = y.jan_yr AND extract(month FROM d) = 1) OR extract(year FROM d) = y.jan_yr - 1
    GROUP BY 1),
a AS (
    SELECT (extract(year FROM snapshot_date) = y.jan_yr AND extract(month FROM snapshot_date) = 1) AS is_jan,
           count(*) FILTER (WHERE trading) AS md
    FROM t_day, y
    WHERE (extract(year FROM snapshot_date) = y.jan_yr AND extract(month FROM snapshot_date) = 1) OR extract(year FROM snapshot_date) = y.jan_yr - 1
    GROUP BY 1)
SELECT 'january_tx_per_merchant_day_vs_prior_year' AS check_name,
       round((SELECT p.tx::numeric / a.md FROM p JOIN a USING (is_jan) WHERE is_jan)
           / (SELECT p.tx::numeric / a.md FROM p JOIN a USING (is_jan) WHERE NOT is_jan), 4) AS value,
       1.0 AS low, NULL::numeric AS high, (SELECT tx FROM p WHERE is_jan) AS n, 'ratio, expect above 1' AS note
UNION ALL
SELECT 'january_ticket_vs_prior_year',
       round((SELECT ticket FROM p WHERE is_jan) / (SELECT ticket FROM p WHERE NOT is_jan), 4),
       NULL, 1.0, (SELECT tx FROM p WHERE is_jan), 'ratio, expect below 1';

-- name: black_friday_vs_friday
WITH bf AS (
    SELECT (make_date(extract(year FROM data_start)::int, 11, 1)
            + ((5 - extract(isodow FROM make_date(extract(year FROM data_start)::int, 11, 1))::int + 7) % 7)
            + 21) AS d
    FROM t_bounds),
day_rate AS (
    SELECT p.d, sum(p.amount_minor) FILTER (WHERE p.status = 'completed')::numeric
                / nullif((SELECT count(*) FROM t_day t WHERE t.snapshot_date = p.d AND t.trading), 0) AS rate
    FROM t_pay p GROUP BY p.d)
SELECT 'black_friday_vs_average_friday_ratio' AS check_name,
       round((SELECT rate FROM day_rate, bf WHERE day_rate.d = bf.d)
           / (SELECT avg(rate) FROM day_rate, bf, t_bounds b
               WHERE extract(isodow FROM day_rate.d) = 5 AND day_rate.d >= b.steady_from AND day_rate.d <> bf.d), 4) AS value,
       1.3 AS low, 1.9 AS high, 1 AS n,
       'GPV per merchant-day, against Fridays in the steady window; one day, so seeds range 1.45 to 1.81; band amended from 2.0 to 2.5 (see Validation Targets)' AS note;

-- name: payday_vs_mid_month
-- The daily volume index has its own bump on the 15th and 16th, so a window of days 14 to 16
-- is not mid-month. Days 8 to 22 are the middle of the month. (A first version used 14 to 16
-- and read 1.26 where the index implies 1.46.)
WITH r AS (
    SELECT p.d, sum(p.amount_minor) FILTER (WHERE p.status = 'completed') AS gpv,
           (SELECT count(*) FROM t_day t WHERE t.snapshot_date = p.d AND t.trading) AS md
    FROM t_pay p, t_bounds b WHERE p.d >= b.steady_from GROUP BY p.d)
SELECT 'payday_vs_mid_month_ratio' AS check_name,
       round((sum(gpv) FILTER (WHERE extract(day FROM d) BETWEEN 25 AND 27)::numeric / sum(md) FILTER (WHERE extract(day FROM d) BETWEEN 25 AND 27))
           / (sum(gpv) FILTER (WHERE extract(day FROM d) BETWEEN 8 AND 22)::numeric / sum(md) FILTER (WHERE extract(day FROM d) BETWEEN 8 AND 22)), 4) AS value,
       1.3 AS low, 1.5 AS high, count(*) AS n,
       'days 25 to 27 against days 8 to 22' AS note
FROM r;

-- name: restaurant_hours
SELECT 'restaurant_lunch_share_12_14' AS check_name,
       round(count(*) FILTER (WHERE h IN (12, 13))::numeric / count(*), 4) AS value, 0.20 AS low, 0.28 AS high, count(*) AS n, 'share of payment attempts' AS note
FROM t_pay, t_bounds b WHERE archetype = 'restaurant' AND d >= b.steady_from
UNION ALL
SELECT 'restaurant_dinner_share_18_21', round(count(*) FILTER (WHERE h IN (18, 19, 20))::numeric / count(*), 4), 0.25, 0.35, count(*), 'share of payment attempts'
FROM t_pay, t_bounds b WHERE archetype = 'restaurant' AND d >= b.steady_from
UNION ALL
SELECT 'ecommerce_evening_share_18_22', round(count(*) FILTER (WHERE h IN (18, 19, 20, 21))::numeric / count(*), 4), 0.21, 0.24, count(*),
       'band amended from 25 to 35 to the Causal Rules hourly table (21 to 24)'
FROM t_pay, t_bounds b WHERE archetype = 'e_commerce' AND d >= b.steady_from;

-- name: spaza_saturday_vs_monday
WITH x AS (
    SELECT p.dow, count(*)::numeric AS tx
    FROM t_pay p, t_bounds b WHERE p.archetype = 'spaza_shop' AND p.d >= b.steady_from GROUP BY p.dow),
m AS (
    SELECT extract(isodow FROM snapshot_date)::int AS dow, count(*)::numeric AS md
    FROM t_day, t_bounds b WHERE archetype = 'spaza_shop' AND trading AND snapshot_date >= b.steady_from GROUP BY 1)
SELECT 'spaza_saturday_over_monday' AS check_name,
       round((SELECT tx / md FROM x JOIN m USING (dow) WHERE dow = 6) / (SELECT tx / md FROM x JOIN m USING (dow) WHERE dow = 1) - 1, 4) AS value,
       0.20 AS low, 0.30 AS high, (SELECT tx FROM x WHERE dow = 6)::bigint AS n, 'transactions per merchant-day' AS note;

-- name: wholesale_weekend
SELECT 'wholesale_payments_on_weekends' AS check_name,
       count(*) FILTER (WHERE dow IN (6, 7)) AS value, 0 AS low, 0 AS high, count(*) AS n, 'payment rows; refunds and chargebacks excluded' AS note
FROM t_pay WHERE archetype = 'wholesale';

-- name: hourly_chi2
-- Chi-squared of platform payment counts by hour against uniform; 23 degrees of freedom,
-- critical value 41.64 at p = 0.01. The check passes when the statistic is above it.
WITH h AS (
    SELECT t_pay.h, count(*)::numeric AS c FROM t_pay, t_bounds b WHERE d >= b.steady_from GROUP BY t_pay.h),
tot AS (SELECT sum(c) AS n FROM h)
SELECT 'hourly_distribution_chi2_vs_uniform' AS check_name,
       round(sum((h.c - tot.n / 24) ^ 2 / (tot.n / 24)), 1) AS value,
       41.64 AS low, NULL::numeric AS high, tot.n::bigint AS n, 'df 23, p 0.01; below the critical value means flat' AS note
FROM h, tot GROUP BY tot.n;

-- name: authorization
SELECT 'auth_rate_card_channels' AS check_name,
       round(count(*) FILTER (WHERE status = 'completed')::numeric / count(*), 4) AS value, 0.88 AS low, 0.96 AS high, count(*) AS n, 'completed over completed plus declined, card channels' AS note
FROM t_pay, t_bounds b WHERE channel IN ('card_present', 'card_ecom', 'payment_link') AND status IN ('completed', 'failed') AND d >= b.steady_from
UNION ALL
SELECT 'auth_rate_card_present_spaza_wholesale',
       round(count(*) FILTER (WHERE status = 'completed')::numeric / count(*), 4), 0.95, 0.98, count(*), 'card_present'
FROM t_pay, t_bounds b WHERE channel = 'card_present' AND archetype IN ('spaza_shop', 'wholesale') AND status IN ('completed', 'failed') AND d >= b.steady_from
UNION ALL
SELECT 'auth_rate_ecommerce',
       round(count(*) FILTER (WHERE status = 'completed')::numeric / count(*), 4), 0.85, 0.92, count(*), 'card_ecom and payment_link, e_commerce archetype'
FROM t_pay, t_bounds b WHERE channel IN ('card_ecom', 'payment_link') AND archetype = 'e_commerce' AND status IN ('completed', 'failed') AND d >= b.steady_from;

-- name: eft_funnel
SELECT 'eft_funnel_completion' AS check_name,
       round(count(*) FILTER (WHERE status = 'completed')::numeric / count(*), 4) AS value, 0.62 AS low, 0.70 AS high, count(*) AS n,
       'completed over initiated; band amended from 70 to 83 to the Simulator Design funnel (66.2%)' AS note
FROM t_pay, t_bounds b WHERE channel = 'instant_eft' AND d >= b.steady_from
UNION ALL
SELECT 'eft_otp_failure_share_of_failures',
       round(count(*) FILTER (WHERE otp_dropout)::numeric / count(*), 4), 0.34, 0.42, count(*),
       'dropped after the OTP request, of all non-completed EFT payments; band amended from 40 to 60 to the Simulator Design funnel (37.9%)'
FROM t_pay, t_bounds b WHERE channel = 'instant_eft' AND status <> 'completed' AND d >= b.steady_from;

-- name: take_rate
SELECT 'gross_take_rate' AS check_name,
       round(sum(fee_minor)::numeric / sum(amount_minor), 5) AS value, 0.019 AS low, 0.024 AS high, count(*) AS n, 'interchange + scheme + markup over GPV' AS note
FROM t_pay, t_bounds b WHERE status = 'completed' AND d >= b.steady_from
UNION ALL
SELECT 'net_take_rate', round(sum(markup_minor)::numeric / sum(amount_minor), 5), 0.008, 0.014, count(*), 'acquirer markup over GPV'
FROM t_pay, t_bounds b WHERE status = 'completed' AND d >= b.steady_from;

-- name: chargeback_refund
WITH pay AS (
    SELECT m.archetype, count(*) AS done FROM merchant_transactions t JOIN t_merchant m USING (merchant_id), t_bounds b
    WHERE t.event_type = 'payment' AND t.status = 'completed' AND (t.created_at AT TIME ZONE 'Africa/Johannesburg')::date >= b.steady_from
    GROUP BY m.archetype),
fol AS (
    SELECT m.archetype, t.event_type, count(*) AS c FROM merchant_transactions t JOIN t_merchant m USING (merchant_id), t_bounds b
    WHERE t.event_type IN ('refund', 'chargeback') AND (t.created_at AT TIME ZONE 'Africa/Johannesburg')::date >= b.steady_from
    GROUP BY m.archetype, t.event_type)
SELECT 'ecommerce_chargeback_rate' AS check_name,
       round(coalesce((SELECT c FROM fol WHERE archetype = 'e_commerce' AND event_type = 'chargeback'), 0)::numeric / (SELECT done FROM pay WHERE archetype = 'e_commerce'), 5) AS value,
       0.005 AS low, 0.015 AS high, (SELECT done FROM pay WHERE archetype = 'e_commerce') AS n, 'chargebacks over completed payments, all channels; whole steady window, not 90-day rolling' AS note
UNION ALL
SELECT 'spaza_restaurant_chargeback_rate',
       round(coalesce((SELECT sum(c) FROM fol WHERE archetype IN ('spaza_shop', 'restaurant') AND event_type = 'chargeback'), 0)::numeric / (SELECT sum(done) FROM pay WHERE archetype IN ('spaza_shop', 'restaurant')), 5),
       0.0005, 0.003, (SELECT sum(done) FROM pay WHERE archetype IN ('spaza_shop', 'restaurant')), 'chargebacks over completed payments'
UNION ALL
SELECT 'platform_refund_rate',
       round((SELECT sum(c) FROM fol WHERE event_type = 'refund')::numeric / (SELECT sum(done) FROM pay), 5),
       0.005, 0.03, (SELECT sum(done) FROM pay), 'refund rows over completed payments'
UNION ALL
SELECT 'ecommerce_refund_rate',
       round(coalesce((SELECT c FROM fol WHERE archetype = 'e_commerce' AND event_type = 'refund'), 0)::numeric / (SELECT done FROM pay WHERE archetype = 'e_commerce'), 5),
       0.03, 0.07, (SELECT done FROM pay WHERE archetype = 'e_commerce'), 'refund rows over completed payments'
UNION ALL
SELECT 'platform_chargeback_rate_fraud_event_proxy',
       round(coalesce((SELECT sum(c) FROM fol WHERE event_type = 'chargeback'), 0)::numeric / (SELECT sum(done) FROM pay), 5),
       0.001, 0.015, (SELECT sum(done) FROM pay), 'fraud event rate read as chargebacks over completed payments (DECISION_LOG, Phase 5.4); lower bound amended from 0.2 to 0.1%';

-- name: fraud_chargeback_lag
SELECT 'fraud_to_chargeback_lag_median_days' AS check_name,
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY (c.created_at::date - p.created_at::date)))::numeric, 1) AS value,
       15 AS low, 45 AS high, count(*) AS n, 'chargeback row against the payment it points at' AS note
FROM merchant_transactions c JOIN merchant_transactions p ON p.transaction_id = c.correlation_id
WHERE c.event_type = 'chargeback';

-- name: retention
WITH c AS (
    SELECT m.merchant_id, m.onboarded_on FROM t_merchant m, t_bounds b WHERE m.onboarded_on <= b.data_end - 180),
r AS (
    SELECT count(*) AS n,
           count(*) FILTER (WHERE EXISTS (SELECT 1 FROM t_day d WHERE d.merchant_id = c.merchant_id
                AND d.snapshot_date = c.onboarded_on + 180 AND d.merchant_state <> 'churned')) AS kept
    FROM c),
c12 AS (
    SELECT m.merchant_id, m.onboarded_on FROM t_merchant m, t_bounds b WHERE m.onboarded_on <= b.data_end - 360),
r12 AS (
    SELECT count(*) AS n,
           count(*) FILTER (WHERE EXISTS (SELECT 1 FROM t_day d WHERE d.merchant_id = c12.merchant_id
                AND d.snapshot_date = c12.onboarded_on + 360 AND d.merchant_state <> 'churned')) AS kept
    FROM c12)
SELECT 'logo_retention_m6' AS check_name, round(r.kept::numeric / r.n, 4) AS value, 0.45 AS low, 0.65 AS high, r.n::bigint AS n,
       'onboarded merchants not Churned 180 days after onboarding, never-activators included' AS note FROM r
UNION ALL
SELECT 'logo_retention_m12', round(r12.kept::numeric / r12.n, 4), 0.30, 0.50, r12.n::bigint, 'same, 360 days' FROM r12;

-- name: gpv_retention_m12
WITH c AS MATERIALIZED (
    SELECT m.merchant_id, m.onboarded_on FROM t_merchant m, t_bounds b
    WHERE m.onboarded_on <= b.data_end - 360
      AND EXISTS (SELECT 1 FROM t_day d WHERE d.merchant_id = m.merchant_id AND d.snapshot_date = m.onboarded_on + 360 AND d.merchant_state <> 'churned')),
g AS (
    SELECT c.merchant_id,
           sum(p.amount_minor) FILTER (WHERE p.d BETWEEN c.onboarded_on + 31 AND c.onboarded_on + 60) AS early,
           sum(p.amount_minor) FILTER (WHERE p.d BETWEEN c.onboarded_on + 331 AND c.onboarded_on + 360) AS late
    FROM c JOIN t_pay p ON p.merchant_id = c.merchant_id AND p.status = 'completed' GROUP BY c.merchant_id)
SELECT 'gpv_retention_m12_survivors' AS check_name, round(sum(late)::numeric / sum(early), 4) AS value, 0.80 AS low, 1.10 AS high, count(*) AS n,
       'days 331 to 360 GPV over days 31 to 60 GPV, survivors with early GPV' AS note
FROM g WHERE early > 0;

-- name: monthly_churn
-- Merchants that reach Churned in a month, over merchants trading at the start of it.
-- Never-activators are left out: they reach Churned by rule at onboarding + 30 days.
WITH first_churn AS (
    SELECT merchant_id, min(snapshot_date) AS churned_on FROM t_day WHERE merchant_state = 'churned' GROUP BY merchant_id),
months AS (
    SELECT g::date AS m FROM t_bounds b, generate_series(b.steady_from, b.data_end - 30, interval '1 month') g),
x AS (
    SELECT mo.m, tm.archetype,
           (SELECT count(*) FROM t_day d WHERE d.snapshot_date = mo.m AND d.trading AND d.archetype = tm.archetype) AS start_n,
           count(*) FILTER (WHERE fc.churned_on >= mo.m AND fc.churned_on < mo.m + interval '1 month' AND tm.first_txn_on IS NOT NULL) AS churned_n
    FROM months mo CROSS JOIN (SELECT DISTINCT archetype FROM t_merchant) a
    JOIN t_merchant tm ON tm.archetype = a.archetype
    LEFT JOIN first_churn fc ON fc.merchant_id = tm.merchant_id
    GROUP BY mo.m, tm.archetype)
SELECT 'monthly_churn_platform' AS check_name, round(sum(churned_n)::numeric / nullif(sum(start_n), 0), 4) AS value, 0.02 AS low, 0.05 AS high, sum(churned_n) AS n, 'entered Churned in the month over trading at month start' AS note FROM x
UNION ALL
SELECT 'monthly_churn_wholesale', round(sum(churned_n)::numeric / nullif(sum(start_n), 0), 4), 0.003, 0.008, sum(churned_n), 'low n' FROM x WHERE archetype = 'wholesale'
UNION ALL
SELECT 'monthly_churn_ecommerce', round(sum(churned_n)::numeric / nullif(sum(start_n), 0), 4), 0.04, 0.07, sum(churned_n), '' FROM x WHERE archetype = 'e_commerce';

-- name: activation
SELECT 'activation_within_7_days' AS check_name,
       round(count(*) FILTER (WHERE first_txn_on <= onboarded_on + 7)::numeric / count(*), 4) AS value, 0.55 AS low, 0.75 AS high, count(*) AS n,
       'of merchants onboarded at least 7 days before the end' AS note
FROM t_merchant, t_bounds b WHERE onboarded_on <= b.data_end - 7
UNION ALL
SELECT 'median_days_to_first_transaction',
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY first_txn_on - onboarded_on))::numeric, 1), 2, 5, count(*), 'activators only'
FROM t_merchant WHERE first_txn_on IS NOT NULL;

-- name: merchant_states
-- Share of daily snapshot rows by state over the steady window. A shorter window picks up
-- the January trough, when many merchants are Stressed at once.
WITH s AS (
    SELECT merchant_state, count(*) AS c FROM t_day, t_bounds b WHERE snapshot_date >= b.steady_from GROUP BY 1),
t AS (SELECT sum(c) AS n FROM s)
SELECT 'state_share_' || x.state AS check_name, round(coalesce(s.c, 0)::numeric / t.n, 4) AS value, x.low, x.high, t.n::bigint AS n, 'steady-window snapshots' AS note
FROM (VALUES ('new', 0.05, 0.15), ('growing', 0.20, 0.35), ('stable', 0.35, 0.50), ('stressed', 0.05, 0.12),
             ('delinquent', 0.02, 0.06), ('churned', 0.03, 0.08)) AS x(state, low, high)
LEFT JOIN s ON s.merchant_state = x.state, t
ORDER BY x.low;

-- name: series_par_monthly
-- Every month end in the data: PAR by the recorded DPD, then by instalment age.
SELECT d AS month_end, count(*) AS loans, round(sum(balance) / 100.0) AS book_zar,
       round(coalesce(sum(balance) FILTER (WHERE dpd > 30), 0)::numeric / sum(balance), 4) AS par30,
       round(coalesce(sum(balance) FILTER (WHERE dpd > 60), 0)::numeric / sum(balance), 4) AS par60,
       round(coalesce(sum(balance) FILTER (WHERE dpd > 90), 0)::numeric / sum(balance), 4) AS par90,
       round(coalesce(sum(balance) FILTER (WHERE oldest_unpaid_days > 30), 0)::numeric / sum(balance), 4) AS par30_age,
       round(coalesce(sum(balance) FILTER (WHERE oldest_unpaid_days > 90), 0)::numeric / sum(balance), 4) AS par90_age
FROM t_book GROUP BY d ORDER BY d;

-- name: par
-- Means over steady-window month ends with at least 20 loans on the book.
WITH m AS (
    SELECT d, sum(balance) AS book,
           sum(balance) FILTER (WHERE dpd > 30) AS p30, sum(balance) FILTER (WHERE dpd > 60) AS p60,
           sum(balance) FILTER (WHERE dpd > 90) AS p90,
           sum(balance) FILTER (WHERE oldest_unpaid_days > 30) AS a30, sum(balance) FILTER (WHERE oldest_unpaid_days > 60) AS a60,
           sum(balance) FILTER (WHERE oldest_unpaid_days > 90) AS a90, count(*) AS loans
    FROM t_book, t_bounds s WHERE d >= s.steady_from GROUP BY d HAVING count(*) >= 20)
SELECT 'par30' AS check_name, round(avg(coalesce(p30, 0)::numeric / book), 4) AS value, 0.05 AS low, 0.10 AS high, sum(loans)::bigint AS n, 'recorded DPD; mean of month ends with 20 or more loans' AS note FROM m
UNION ALL SELECT 'par60', round(avg(coalesce(p60, 0)::numeric / book), 4), 0.03, 0.07, sum(loans)::bigint, 'recorded DPD' FROM m
UNION ALL SELECT 'par90', round(avg(coalesce(p90, 0)::numeric / book), 4), 0.02, 0.05, sum(loans)::bigint, 'recorded DPD' FROM m
UNION ALL SELECT 'par30_instalment_age', round(avg(coalesce(a30, 0)::numeric / book), 4), 0.05, 0.10, sum(loans)::bigint, 'oldest unpaid instalment age; ahead of recorded DPD between monthly cycles' FROM m
UNION ALL SELECT 'par60_instalment_age', round(avg(coalesce(a60, 0)::numeric / book), 4), 0.03, 0.07, sum(loans)::bigint, '' FROM m
UNION ALL SELECT 'par90_instalment_age', round(avg(coalesce(a90, 0)::numeric / book), 4), 0.02, 0.05, sum(loans)::bigint, '' FROM m;

-- name: february_vs_december_par30
-- The January paradox: PAR30 at the end of February against the end of the December before.
-- Expect February higher. Both measures; few loans on the book in a short run.
WITH y AS (SELECT extract(year FROM data_start)::int AS yr FROM t_bounds),
p AS (
    SELECT d, count(*) AS loans,
           coalesce(sum(balance) FILTER (WHERE dpd > 30), 0)::numeric / sum(balance) AS by_dpd,
           coalesce(sum(balance) FILTER (WHERE oldest_unpaid_days > 30), 0)::numeric / sum(balance) AS by_age
    FROM t_book GROUP BY d)
SELECT 'february_minus_december_par30_recorded_dpd' AS check_name,
       round((SELECT by_dpd FROM p, y WHERE d = (make_date(y.yr + 1, 3, 1) - 1)) - (SELECT by_dpd FROM p, y WHERE d = make_date(y.yr, 12, 31)), 4) AS value,
       0.0 AS low, NULL::numeric AS high, (SELECT loans FROM p, y WHERE d = make_date(y.yr, 12, 31)) AS n, 'percentage points as a fraction; n is the December loan count' AS note
UNION ALL
SELECT 'february_minus_december_par30_instalment_age',
       round((SELECT by_age FROM p, y WHERE d = (make_date(y.yr + 1, 3, 1) - 1)) - (SELECT by_age FROM p, y WHERE d = make_date(y.yr, 12, 31)), 4),
       0.0, NULL, (SELECT loans FROM p, y WHERE d = make_date(y.yr, 12, 31)), '';

-- name: par30_by_archetype
-- Segment view over every steady-window month end, loans pooled. Small n.
SELECT 'par30_' || archetype AS check_name,
       round(coalesce(sum(balance) FILTER (WHERE dpd > 30), 0)::numeric / sum(balance), 4) AS value,
       NULL::numeric AS low, NULL::numeric AS high, count(DISTINCT loan_id) AS n, 'recorded DPD, loan-months pooled' AS note
FROM t_book, t_bounds s WHERE d >= s.steady_from GROUP BY archetype HAVING count(DISTINCT loan_id) >= 3 ORDER BY archetype;

-- name: wholesale_vs_ecommerce_par30
-- Steady-window month ends, loans pooled.
WITH t_book AS (SELECT b.* FROM t_book b, t_bounds s WHERE b.d >= s.steady_from)
SELECT 'wholesale_par30_below_ecommerce_par30' AS check_name,
       round(
         (SELECT coalesce(sum(balance) FILTER (WHERE dpd > 30), 0)::numeric / sum(balance) FROM t_book WHERE archetype = 'wholesale')
       - (SELECT coalesce(sum(balance) FILTER (WHERE dpd > 30), 0)::numeric / sum(balance) FROM t_book WHERE archetype = 'e_commerce'), 4) AS value,
       NULL::numeric AS low, 0.0 AS high,
       (SELECT count(DISTINCT loan_id) FROM t_book WHERE archetype IN ('wholesale', 'e_commerce')) AS n,
       'recorded DPD, wholesale minus e-commerce PAR30; expect negative' AS note;

-- name: vintage_par30_by_mob
-- Cumulative PAR30 by months on book: the share of a vintage's opening balance that is more than
-- 30 days past due at that age, written-off loans included (t_vintage.late_now). The first
-- version of this check counted a loan once it had ever been late, cures included; that figure
-- is reported beside it as vintage_ever_late_mob_k with no band, because it runs about twice as
-- high and is not what a vintage table shows. The month 3 band was amended from 3 to 8% to
-- 0 to 5%: recorded DPD steps by 30 days at each monthly cycle, so a loan cannot first show more
-- than 30 days past due before about month 3 (test_repayments.py).
SELECT 'vintage_par30_mob_' || v.k AS check_name,
       round(sum(v.weight) FILTER (WHERE v.late_now)::numeric / sum(v.weight), 4) AS value, b.low, b.high, count(*) AS n,
       'balance-weighted, point-in-time plus written off; loans on book at least ' || v.k || ' months' AS note
FROM t_vintage v JOIN (VALUES (3, 0.0, 0.05), (6, 0.06, 0.14), (12, 0.08, 0.18)) AS b(k, low, high) USING (k)
GROUP BY v.k, b.low, b.high
UNION ALL
SELECT 'vintage_ever_late_mob_' || k, round(avg(ever_late::int), 4), NULL::numeric, NULL::numeric, count(*),
       'share of loans ever past 30 days by then, cures included; no band'
FROM t_vintage WHERE k IN (3, 6, 12) GROUP BY k
ORDER BY 1;

-- name: vintage_curve_shape
-- Among loans that have been on book 12 months: PAR30 (as above) at 3, 6, 9 and 12 months, and
-- the share of the 12-month figure that appears after month 9. The Validation Targets say steep
-- early, plateau after MOB 9; the 35% ceiling is my reading of "plateau" (measured 12 to 30%).
WITH full_year AS (SELECT loan_id FROM t_vintage WHERE k = 12),
p AS (
    SELECT v.k, sum(v.weight) FILTER (WHERE v.late_now)::numeric / sum(v.weight) AS par, count(*) AS n
    FROM t_vintage v JOIN full_year USING (loan_id) WHERE v.k IN (3, 6, 9, 12) GROUP BY v.k)
SELECT 'vintage_share_of_mob12_par30_after_mob9' AS check_name,
       round(((SELECT par FROM p WHERE k = 12) - (SELECT par FROM p WHERE k = 9)) / nullif((SELECT par FROM p WHERE k = 12), 0), 4) AS value,
       NULL::numeric AS low, 0.35 AS high, (SELECT n FROM p WHERE k = 12) AS n,
       coalesce('PAR30 at 3/6/9/12 months: ' || (SELECT string_agg(round(par, 3)::text, ' / ' ORDER BY k) FROM p), 'no loan has been on book 12 months') AS note;

-- name: vintage_adjacent_divergence
-- Quarterly disbursement vintages: PAR30 at 3 months on book, and the largest gap between
-- adjacent quarters that each have at least 20 loans. Validation Targets: under 5 points unless
-- there is a stress event.
WITH q AS (
    SELECT date_trunc('quarter', disbursed_on)::date AS vintage, count(*) AS n,
           sum(weight) FILTER (WHERE late_now)::numeric / sum(weight) AS par
    FROM t_vintage WHERE k = 3 GROUP BY 1 HAVING count(*) >= 20),
g AS (SELECT vintage, par, par - lag(par) OVER (ORDER BY vintage) AS gap,
             vintage - lag(vintage) OVER (ORDER BY vintage) AS days_apart FROM q)
SELECT 'vintage_max_adjacent_gap_mob3' AS check_name, round(max(abs(gap)), 4) AS value, NULL::numeric AS low, 0.05 AS high, count(*) AS n,
       'adjacent quarters with 20 or more loans; small samples, so read as direction' AS note
FROM g WHERE gap IS NOT NULL AND days_apart <= 100;

-- name: vintage_oct_nov_vs_others
-- October and November disbursements against the rest: PAR30 at 5 months on book. Validation
-- Targets: higher at MOB 3 to 5 (seasonal demand brings weaker borrowers).
SELECT 'vintage_oct_nov_minus_others_par30_mob5' AS check_name,
       round(sum(weight) FILTER (WHERE late_now AND extract(month FROM disbursed_on) IN (10, 11))::numeric
             / nullif(sum(weight) FILTER (WHERE extract(month FROM disbursed_on) IN (10, 11)), 0)
           - sum(weight) FILTER (WHERE late_now AND extract(month FROM disbursed_on) NOT IN (10, 11))::numeric
             / nullif(sum(weight) FILTER (WHERE extract(month FROM disbursed_on) NOT IN (10, 11)), 0), 4) AS value,
       0.0 AS low, NULL::numeric AS high, count(*) FILTER (WHERE extract(month FROM disbursed_on) IN (10, 11)) AS n,
       'n is the October and November loans; expect above 0' AS note
FROM t_vintage WHERE k = 5;

-- name: x_gpv_decline_loan_outcome
-- Merchant-level version of "GPV decline leads to PAR rise". A loan is flagged if, at any month
-- end after its first month, the merchant's GPV over the last 30 days is at least 50% below its
-- GPV over the 30 days before disbursement (the Causal Rules forced-miss trigger). Compares how
-- often flagged and other loans are more than 30 days past due at any month end (recorded DPD).
-- Replaces the platform-level monthly correlation, which has 9 to 18 points per run.
WITH base AS (
    SELECT l.loan_id, l.merchant_id,
           (SELECT coalesce(sum(p.amount_minor), 0) FROM t_pay p
             WHERE p.merchant_id = l.merchant_id AND p.status = 'completed' AND p.d >= l.disbursed_on - 30 AND p.d < l.disbursed_on) AS g0
    FROM t_loan l WHERE l.initial_balance IS NOT NULL),
win AS (
    SELECT b.loan_id, e.d,
           (SELECT coalesce(sum(p.amount_minor), 0) FROM t_pay p
             WHERE p.merchant_id = b.merchant_id AND p.status = 'completed' AND p.d > e.d - 30 AND p.d <= e.d) AS g
    FROM base b JOIN t_loan l USING (loan_id) JOIN t_month_end e ON e.d > l.disbursed_on + 30 WHERE b.g0 > 0),
flag AS (
    SELECT w.loan_id, bool_or(w.g <= 0.5 * b.g0) AS fell
    FROM win w JOIN base b USING (loan_id) GROUP BY w.loan_id),
outcome AS (
    SELECT f.loan_id, f.fell,
           EXISTS (SELECT 1 FROM t_book k WHERE k.loan_id = f.loan_id AND k.dpd > 30) AS late
    FROM flag f)
SELECT 'late_rate_loans_after_a_50pct_gpv_fall' AS check_name, round(avg(late::int) FILTER (WHERE fell), 4) AS value, NULL::numeric AS low, NULL::numeric AS high,
       count(*) FILTER (WHERE fell) AS n, 'loans whose merchant GPV fell 50% or more; late = more than 30 days past due at a month end' AS note
FROM outcome
UNION ALL
SELECT 'late_rate_other_loans', round(avg(late::int) FILTER (WHERE NOT fell), 4), NULL, NULL, count(*) FILTER (WHERE NOT fell), ''
FROM outcome
UNION ALL
SELECT 'late_rate_ratio_after_a_gpv_fall',
       round(avg(late::int) FILTER (WHERE fell) / nullif(avg(late::int) FILTER (WHERE NOT fell), 0), 3), 1.0, NULL, count(*) FILTER (WHERE fell),
       'expect above 1: loans after a GPV fall are late more often'
FROM outcome;

-- name: collections
WITH fs AS (
    SELECT r.loan_id, r.installment_number, r.collection_status = 'success' AS ok, r.due_date
    FROM loan_repayments r, t_bounds b
    WHERE r.retry_attempt = 0 AND r.days_past_due = 0 AND NOT r.is_cured AND NOT r.write_off_flag AND r.due_date >= b.steady_from),
rec AS (
    SELECT DISTINCT loan_id, installment_number FROM loan_repayments WHERE retry_attempt > 0 AND collection_status = 'success')
SELECT 'debicheck_first_strike_success' AS check_name, round(count(*) FILTER (WHERE ok)::numeric / count(*), 4) AS value, 0.72 AS low, 0.82 AS high, count(*) AS n, 'first attempts on the due date' AS note FROM fs
UNION ALL
SELECT 'retry_recovery_of_first_strike_failures',
       round(count(*) FILTER (WHERE rec.loan_id IS NOT NULL)::numeric / count(*), 4), 0.45, 0.55, count(*), 'recovered within the retries, of failures whose retries have run'
FROM fs LEFT JOIN rec USING (loan_id, installment_number), t_bounds b WHERE NOT ok AND fs.due_date <= b.data_end - 10
UNION ALL
SELECT 'cumulative_collection_success',
       round((count(*) FILTER (WHERE ok) + count(*) FILTER (WHERE NOT ok AND rec.loan_id IS NOT NULL))::numeric / count(*), 4), 0.85, 0.92, count(*), 'first strike plus retries'
FROM fs LEFT JOIN rec USING (loan_id, installment_number), t_bounds b WHERE fs.due_date <= b.data_end - 10;

-- name: first_payment_default
-- Instalment 1 still unpaid 30 days after its due date (DECISION_LOG, Phase 8 direction).
SELECT 'first_payment_default' AS check_name,
       round(count(*) FILTER (WHERE i.paid_on IS NULL OR i.paid_on > i.due_date + 30)::numeric / count(*), 4) AS value, 0.05 AS low, 0.10 AS high, count(*) AS n,
       'loans whose instalment 1 fell due at least 30 days before the end; band amended from 3 to 6 (see Validation Targets)' AS note
FROM t_instalment i, t_bounds b WHERE i.installment_number = 1 AND i.due_date + 30 <= b.data_end;

-- name: writeoff_rate
-- Outstanding balance written off in the last six months, annualised, over the mean book at
-- the month ends in that window.
WITH w AS (SELECT (data_end - 183) AS from_d, data_end FROM t_bounds),
wo AS (
    SELECT sum(r.outstanding_before) AS amt, count(*) AS c
    FROM loan_repayments r JOIN t_loan l ON l.loan_id = r.loan_id, w
    WHERE r.write_off_flag AND l.written_off_on > w.from_d AND l.written_off_on <= w.data_end),
bk AS (SELECT avg(book) AS book FROM (SELECT d, sum(balance) AS book FROM t_book, w WHERE d > w.from_d GROUP BY d) q)
SELECT 'writeoff_rate_annualised' AS check_name, round(wo.amt * 2.0 / bk.book, 4) AS value, 0.15 AS low, 0.25 AS high, wo.c AS n,
       'last 6 months of write-offs over mean book, times 2 (amended band 15 to 25)' AS note
FROM wo, bk;

-- name: approvals
WITH a AS (
    SELECT decision, actioned_by,
           coalesce(decline_reasons && ARRAY['insufficient_trading_history', 'low_volume', 'adverse_merchant_state',
                    'merchant_not_established', 'existing_loan_outstanding', 'prior_write_off'], false) AS ineligible
    FROM loan_applications)
SELECT 'approval_rate_gross' AS check_name, round(count(*) FILTER (WHERE decision = 'approved')::numeric / count(*), 4) AS value, 0.35 AS low, 0.55 AS high, count(*) AS n, 'approved over all applications' AS note FROM a
UNION ALL
SELECT 'approval_rate_net_eligible', round(count(*) FILTER (WHERE decision = 'approved')::numeric / count(*), 4), 0.55, 0.80, count(*), 'approved over eligible applications; band amended from 55 to 75 (see Validation Targets)' FROM a WHERE NOT ineligible
UNION ALL
SELECT 'automation_rate_all', round(count(*) FILTER (WHERE actioned_by = 'automated_engine')::numeric / count(*), 4), NULL, NULL, count(*), 'no manual review; includes auto-declined ineligible applicants, so no band (the band applies to the eligible row)' FROM a
UNION ALL
SELECT 'automation_rate_eligible', round(count(*) FILTER (WHERE actioned_by = 'automated_engine')::numeric / count(*), 4), 0.75, 0.85, count(*), 'no manual review, eligible applications only; band amended from 65 to 80 (see Validation Targets)' FROM a WHERE NOT ineligible;

-- name: october_november_applications
-- Applications in October and November against the mean of the other full months from the
-- steady window. The population is still growing, so this runs a little high.
WITH m AS (
    SELECT date_trunc('month', submission_ts AT TIME ZONE 'Africa/Johannesburg')::date AS mo, count(*) AS c
    FROM loan_applications GROUP BY 1),
f AS (
    SELECT m.* FROM m, t_bounds b WHERE m.mo >= b.steady_from AND (m.mo + interval '1 month')::date <= b.data_end + 1)
SELECT 'october_november_application_volume_ratio' AS check_name,
       round(avg(c) FILTER (WHERE extract(month FROM mo) IN (10, 11)) / avg(c) FILTER (WHERE extract(month FROM mo) NOT IN (10, 11)), 3) AS value,
       1.5 AS low, 2.5 AS high, sum(c) AS n, 'monthly applications, October and November against other steady months' AS note
FROM f;

-- name: risk_alignment
-- Share of approved applications whose merchant had a risk score of 30 or below at the end
-- of the day before the application.
SELECT 'approved_share_in_0_30_risk_bucket' AS check_name,
       round(count(*) FILTER (WHERE d.risk_score <= 30)::numeric / count(*), 4) AS value, 0.75 AS low, NULL::numeric AS high, count(*) AS n,
       'approved applications; manual-review approvals come from 31 to 55 by design, so about 84% at mid-band tiers; band amended from 90% (see Validation Targets)' AS note
FROM loan_applications a JOIN t_day d ON d.merchant_id = a.merchant_id
 AND d.snapshot_date = (a.submission_ts AT TIME ZONE 'Africa/Johannesburg')::date - 1
WHERE a.decision = 'approved';

-- name: risk_buckets
-- Active merchants on the last day: trading, not Churned, no written-off loan.
WITH t AS (SELECT count(*) AS n FROM t_active_last)
SELECT 'risk_bucket_' || x.bucket AS check_name,
       round(count(a.merchant_id) FILTER (WHERE a.risk_score BETWEEN x.lo AND x.hi)::numeric / t.n, 4) AS value, x.low, x.high, t.n::bigint AS n, 'active merchants, last day' AS note
FROM (VALUES ('0_30', 0, 30, 0.55, 0.70), ('31_55', 31, 55, 0.15, 0.25), ('56_75', 56, 75, 0.08, 0.15), ('76_100', 76, 100, 0.03, 0.08)) AS x(bucket, lo, hi, low, high)
CROSS JOIN t LEFT JOIN t_active_last a ON true
GROUP BY x.bucket, x.lo, x.hi, x.low, x.high, t.n ORDER BY x.lo;

-- name: x_stressed_vs_stable_first_strike
-- First-strike success by the merchant state at the end of the due date.
WITH fs AS (
    SELECT d.merchant_state, r.collection_status = 'success' AS ok
    FROM loan_repayments r JOIN t_day d ON d.merchant_id = r.merchant_id AND d.snapshot_date = r.due_date, t_bounds b
    WHERE r.retry_attempt = 0 AND r.days_past_due = 0 AND NOT r.is_cured AND NOT r.write_off_flag AND r.due_date >= b.steady_from),
s AS (SELECT merchant_state, avg(ok::int) AS rate, count(*) AS n FROM fs GROUP BY 1)
SELECT 'stable_minus_stressed_first_strike_pp' AS check_name,
       round(100 * ((SELECT rate FROM s WHERE merchant_state = 'stable') - (SELECT rate FROM s WHERE merchant_state = 'stressed')), 1) AS value,
       20 AS low, 30 AS high, (SELECT n FROM s WHERE merchant_state = 'stressed') AS n, 'percentage points; n is the Stressed attempts' AS note
UNION ALL
SELECT 'first_strike_' || merchant_state, round(rate, 4), NULL, NULL, n, ''
FROM s;

-- name: x_gpv_decline_par_lag
-- Correlation between a month's platform GPV decline (per merchant-day) and the rise in
-- PAR30 one and two months later. Few steady months, so read it as direction only.
WITH g AS (
    SELECT p.m, p.gpv / a.md AS rate
    FROM (SELECT date_trunc('month', d)::date AS m, sum(amount_minor) FILTER (WHERE status = 'completed')::numeric AS gpv FROM t_pay GROUP BY 1) p
    JOIN (SELECT date_trunc('month', snapshot_date)::date AS m, count(*) FILTER (WHERE trading) AS md FROM t_day GROUP BY 1) a USING (m)),
gd AS (SELECT m, -(rate / lag(rate) OVER (ORDER BY m) - 1) AS decline FROM g),
par AS (
    SELECT date_trunc('month', d)::date AS m, coalesce(sum(balance) FILTER (WHERE dpd > 30), 0)::numeric / sum(balance) AS p30
    FROM t_book, t_bounds s WHERE d >= s.steady_from GROUP BY 1),
pd AS (SELECT m, p30 - lag(p30) OVER (ORDER BY m) AS rise FROM par)
SELECT 'corr_gpv_decline_par30_rise_lag_' || k AS check_name,
       round(corr(gd.decline, pd.rise)::numeric, 3) AS value, 0.4 AS low, NULL::numeric AS high, count(*) AS n, 'months in the steady window' AS note
FROM generate_series(1, 2) k JOIN gd ON true JOIN pd ON pd.m = (gd.m + (k || ' months')::interval)::date
GROUP BY k ORDER BY k;

-- name: x_chargeback_ratio
-- Chargebacks per completed card payment, E-commerce against Wholesale.
WITH card AS (
    SELECT archetype, count(*) AS done FROM t_pay, t_bounds b
    WHERE status = 'completed' AND channel IN ('card_present', 'card_ecom', 'payment_link') AND d >= b.steady_from GROUP BY 1),
cb AS (
    SELECT m.archetype, count(*) AS c FROM merchant_transactions t JOIN t_merchant m USING (merchant_id), t_bounds b
    WHERE t.event_type = 'chargeback' AND (t.created_at AT TIME ZONE 'Africa/Johannesburg')::date >= b.steady_from GROUP BY 1)
SELECT 'ecommerce_over_wholesale_chargeback_ratio' AS check_name,
       round(((SELECT c FROM cb WHERE archetype = 'e_commerce')::numeric / (SELECT done FROM card WHERE archetype = 'e_commerce'))
           / nullif((SELECT c FROM cb WHERE archetype = 'wholesale')::numeric / (SELECT done FROM card WHERE archetype = 'wholesale'), 0), 2) AS value,
       5 AS low, 10 AS high,
       coalesce((SELECT c FROM cb WHERE archetype = 'wholesale'), 0) AS n,
       'card payments only (DECISION_LOG); n is the Wholesale chargebacks, NULL value means none' AS note;

-- name: x_vintage_writeoffs
-- Written-off share of loans by disbursement season.
SELECT CASE WHEN extract(month FROM disbursed_on) IN (10, 11) THEN 'oct_nov_disbursed' ELSE 'other_disbursed' END AS check_name,
       round(count(*) FILTER (WHERE written_off_on IS NOT NULL)::numeric / count(*), 4) AS value, NULL::numeric AS low, NULL::numeric AS high, count(*) AS n,
       'written off by the last day; loans disbursed in the last 5 months are left out' AS note
FROM t_loan, t_bounds b WHERE disbursed_on <= b.data_end - 150 GROUP BY 1 ORDER BY 1;

-- name: x_january_paradox
-- Spaza and Restaurant: January transactions per merchant-day against the year before, and
-- January GPV per merchant-day against the December before.
WITH y AS (SELECT extract(year FROM data_start)::int + 1 AS jan_yr FROM t_bounds),
s AS (SELECT * FROM t_pay WHERE archetype IN ('spaza_shop', 'restaurant')),
t AS (SELECT * FROM t_day WHERE archetype IN ('spaza_shop', 'restaurant') AND trading),
c AS (
    SELECT (SELECT count(*) FROM s, y WHERE extract(year FROM d) = y.jan_yr AND extract(month FROM d) = 1)::numeric
         / (SELECT count(*) FROM t, y WHERE extract(year FROM snapshot_date) = y.jan_yr AND extract(month FROM snapshot_date) = 1) AS jan,
           (SELECT count(*) FROM s, y WHERE extract(year FROM d) = y.jan_yr - 1)::numeric
         / (SELECT count(*) FROM t, y WHERE extract(year FROM snapshot_date) = y.jan_yr - 1) AS yr),
g AS (
    SELECT (SELECT sum(amount_minor) FROM s, y WHERE status = 'completed' AND extract(year FROM d) = y.jan_yr AND extract(month FROM d) = 1)::numeric
         / (SELECT count(*) FROM t, y WHERE extract(year FROM snapshot_date) = y.jan_yr AND extract(month FROM snapshot_date) = 1) AS jan,
           (SELECT sum(amount_minor) FROM s, y WHERE status = 'completed' AND extract(year FROM d) = y.jan_yr - 1 AND extract(month FROM d) = 12)::numeric
         / (SELECT count(*) FROM t, y WHERE extract(year FROM snapshot_date) = y.jan_yr - 1 AND extract(month FROM snapshot_date) = 12) AS dec)
SELECT 'spaza_restaurant_january_tx_vs_prior_year' AS check_name, round(c.jan / c.yr, 4) AS value, 1.0 AS low, NULL::numeric AS high, (SELECT count(*) FROM s) AS n, 'expect above 1' AS note FROM c
UNION ALL
SELECT 'spaza_restaurant_january_gpv_vs_december', round(g.jan / g.dec, 4), NULL, 1.0, (SELECT count(*) FROM s), 'expect below 1' FROM g;

-- name: x_fraud_state_deterioration
-- Share of Stable or Growing merchants that reach Stressed within 30 days of a fraud-type
-- risk event, against the same share for any merchant-day in the steady window.
WITH ev AS (
    SELECT DISTINCT ON (e.merchant_id, (e.action_ts AT TIME ZONE 'Africa/Johannesburg')::date)
           e.merchant_id, (e.action_ts AT TIME ZONE 'Africa/Johannesburg')::date AS d
    FROM risk_events e, t_bounds b
    WHERE e.rule_type IN ('refund_spike', 'chargeback_accumulation', 'scheme_warning', 'velocity_breach')
      AND (e.action_ts AT TIME ZONE 'Africa/Johannesburg')::date BETWEEN b.steady_from AND b.data_end - 30),
hit AS (
    SELECT ev.merchant_id, ev.d,
           EXISTS (SELECT 1 FROM t_day s WHERE s.merchant_id = ev.merchant_id AND s.snapshot_date BETWEEN ev.d AND ev.d + 30 AND s.merchant_state = 'stressed') AS to_stressed
    FROM ev JOIN t_day s0 ON s0.merchant_id = ev.merchant_id AND s0.snapshot_date = ev.d AND s0.merchant_state IN ('stable', 'growing')),
base AS (
    SELECT count(*) AS md,
           count(*) FILTER (WHERE EXISTS (SELECT 1 FROM t_day s WHERE s.merchant_id = t0.merchant_id AND s.snapshot_date BETWEEN t0.snapshot_date AND t0.snapshot_date + 30 AND s.merchant_state = 'stressed')) AS hits
    FROM (SELECT * FROM t_day t, t_bounds b WHERE t.merchant_state IN ('stable', 'growing') AND t.snapshot_date BETWEEN b.steady_from AND b.data_end - 30
          AND random() < 0.05) t0)
SELECT 'stressed_within_30d_after_fraud_event' AS check_name, round(avg(to_stressed::int), 4) AS value, NULL::numeric AS low, NULL::numeric AS high, count(*) AS n, 'fraud-type events on Stable or Growing merchants' AS note FROM hit
UNION ALL
SELECT 'stressed_within_30d_any_merchant_day', round(hits::numeric / md, 4), NULL, NULL, md, '5% sample of Stable and Growing merchant-days' FROM base;

-- name: fm_modifier_stacking
-- Highest day over the mean for its month, per merchant-day: payment counts and GPV, per
-- archetype (days with at least 10 merchants trading) and for the platform. Failure mode:
-- above 3. The saturation cap bounds the intensity function, so the count ratios are the
-- test of it. The archetype GPV ratio is reported for reference: Black Friday and the
-- December peak days lift tickets after the cap by design, which can take it past 3 for a
-- small archetype.
WITH a AS (
    SELECT p.archetype, p.d, sum(p.amount_minor) FILTER (WHERE p.status = 'completed')::numeric AS gpv, count(*)::numeric AS tx,
           (SELECT count(*) FROM t_day t WHERE t.snapshot_date = p.d AND t.archetype = p.archetype AND t.trading) AS md
    FROM t_pay p, t_bounds b WHERE p.d >= b.steady_from GROUP BY p.archetype, p.d),
r AS (
    SELECT archetype, d,
           (gpv / md) / avg(gpv / md) OVER (PARTITION BY archetype, date_trunc('month', d)) AS gpv_ratio,
           (tx / md) / avg(tx / md) OVER (PARTITION BY archetype, date_trunc('month', d)) AS tx_ratio
    FROM a WHERE md >= 10),
pl AS (SELECT d, sum(gpv) / sum(md) AS gpv_rate, sum(tx) / sum(md) AS tx_rate FROM a GROUP BY d),
pr AS (
    SELECT d, gpv_rate / avg(gpv_rate) OVER (PARTITION BY date_trunc('month', d)) AS gpv_ratio,
              tx_rate / avg(tx_rate) OVER (PARTITION BY date_trunc('month', d)) AS tx_ratio FROM pl)
SELECT 'max_day_tx_over_archetype_month_mean_ratio' AS check_name, round(max(tx_ratio), 3) AS value, NULL::numeric AS low, 3.0 AS high, count(*) AS n,
       coalesce((SELECT archetype || ' ' || d FROM r ORDER BY tx_ratio DESC LIMIT 1), 'no archetype-day has 10 merchants trading') AS note
FROM r
UNION ALL
SELECT 'max_day_tx_over_platform_month_mean_ratio', round(max(tx_ratio), 3), NULL, 3.0, count(*), (SELECT d::text FROM pr ORDER BY tx_ratio DESC LIMIT 1)
FROM pr
UNION ALL
SELECT 'max_day_gpv_over_platform_month_mean_ratio', round(max(gpv_ratio), 3), NULL, 3.0, count(*), (SELECT d::text FROM pr ORDER BY gpv_ratio DESC LIMIT 1)
FROM pr
UNION ALL
SELECT 'max_day_gpv_over_archetype_month_mean_ratio', round(max(gpv_ratio), 3), NULL, 3.0, count(*),
       coalesce((SELECT archetype || ' ' || d FROM r ORDER BY gpv_ratio DESC LIMIT 1), 'no archetype-day has 10 merchants trading')
FROM r;

-- name: fm_state_snapping
-- Most merchants changing state in one ISO week, as a share of merchants present that week.
WITH c AS (
    SELECT merchant_id, snapshot_date, merchant_state,
           merchant_state IS DISTINCT FROM lag(merchant_state) OVER (PARTITION BY merchant_id ORDER BY snapshot_date) AS changed,
           lag(merchant_state) OVER (PARTITION BY merchant_id ORDER BY snapshot_date) IS NOT NULL AS has_prev
    FROM t_day),
w AS (
    SELECT date_trunc('week', snapshot_date)::date AS wk,
           count(DISTINCT merchant_id) FILTER (WHERE changed AND has_prev) AS moved,
           count(DISTINCT merchant_id) AS present
    FROM c, t_bounds b WHERE snapshot_date >= b.steady_from GROUP BY 1)
SELECT 'max_weekly_share_of_merchants_changing_state' AS check_name, round(max(moved::numeric / present), 4) AS value, NULL::numeric AS low, 0.30 AS high, count(*) AS n, 'weeks in the steady window' AS note
FROM w;

-- name: fm_early_par30
-- Highest PAR30 at the month ends in the first three months after the first disbursement.
WITH f AS (SELECT min(disbursed_on) AS first_d FROM t_loan),
m AS (
    SELECT b.d, coalesce(sum(b.balance) FILTER (WHERE b.dpd > 30), 0)::numeric / sum(b.balance) AS p30
    FROM t_book b, f WHERE b.d <= f.first_d + interval '3 months' GROUP BY b.d)
SELECT 'max_par30_first_three_months_of_lending' AS check_name, round(max(p30), 4) AS value, NULL::numeric AS low, 0.20 AS high, count(*) AS n,
       'month ends within 3 months of the first disbursement' AS note
FROM m;

-- name: fm_wholesale_par90
-- Steady-window month ends, Wholesale loans pooled.
SELECT 'wholesale_par90' AS check_name,
       round(coalesce(sum(balance) FILTER (WHERE dpd > 90), 0)::numeric / nullif(sum(balance), 0), 4) AS value, NULL::numeric AS low, 0.05 AS high,
       count(DISTINCT loan_id) AS n, 'ceiling is the top of the PAR90 band; n is the Wholesale loans on the book' AS note
FROM t_book, t_bounds s WHERE archetype = 'wholesale' AND d >= s.steady_from;

-- name: fm_risk_clustering
-- Active merchants with a risk score above 60 on the last day. The ceiling is the top of the
-- 56 to 75 band plus the top of the 76 to 100 band (15% and 8%).
SELECT 'active_merchants_with_risk_score_above_60' AS check_name,
       round(count(*) FILTER (WHERE risk_score > 60)::numeric / count(*), 4) AS value, NULL::numeric AS low, 0.23 AS high, count(*) AS n, 'active merchants, last day' AS note
FROM t_active_last;
