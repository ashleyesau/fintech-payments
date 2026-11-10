-- Every merchant must have exactly one current SCD2 row.
SELECT
    merchant_id,
    COUNTIF(dbt_is_current) AS current_rows
FROM {{ ref('merchants_scd') }}
GROUP BY merchant_id
HAVING COUNTIF(dbt_is_current) != 1
