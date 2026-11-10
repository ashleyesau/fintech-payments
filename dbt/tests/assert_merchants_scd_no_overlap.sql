-- Validity windows for one merchant must not overlap.
SELECT
    a.merchant_id,
    a.dbt_valid_from,
    b.dbt_valid_from AS overlapping_valid_from
FROM {{ ref('merchants_scd') }} a
JOIN {{ ref('merchants_scd') }} b
    ON  a.merchant_id = b.merchant_id
    AND a.dbt_valid_from < b.dbt_valid_from
    AND (a.dbt_valid_to IS NULL OR a.dbt_valid_to > b.dbt_valid_from)
