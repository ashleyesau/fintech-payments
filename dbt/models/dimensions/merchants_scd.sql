{{
    config(
        materialized='table',
    )
}}

WITH sorted AS (
    SELECT
        merchant_id,
        snapshot_at,
        archetype,
        province,
        is_metro,
        onboarded_at,
        first_transaction_at,
        merchant_state,
        risk_score,
        loan_count,
        has_active_loan,
        LAG(archetype)            OVER (PARTITION BY merchant_id ORDER BY snapshot_at) AS prev_archetype,
        LAG(province)             OVER (PARTITION BY merchant_id ORDER BY snapshot_at) AS prev_province,
        LAG(is_metro)             OVER (PARTITION BY merchant_id ORDER BY snapshot_at) AS prev_is_metro,
        LAG(onboarded_at)         OVER (PARTITION BY merchant_id ORDER BY snapshot_at) AS prev_onboarded_at,
        LAG(first_transaction_at) OVER (PARTITION BY merchant_id ORDER BY snapshot_at) AS prev_first_transaction_at,
        LAG(merchant_state)       OVER (PARTITION BY merchant_id ORDER BY snapshot_at) AS prev_merchant_state,
        LAG(risk_score)           OVER (PARTITION BY merchant_id ORDER BY snapshot_at) AS prev_risk_score,
        LAG(loan_count)           OVER (PARTITION BY merchant_id ORDER BY snapshot_at) AS prev_loan_count,
        LAG(has_active_loan)      OVER (PARTITION BY merchant_id ORDER BY snapshot_at) AS prev_has_active_loan
    FROM {{ ref('stg_merchants') }}
),

transitions AS (
    SELECT
        merchant_id,
        snapshot_at,
        archetype,
        province,
        is_metro,
        onboarded_at,
        first_transaction_at,
        merchant_state,
        risk_score,
        loan_count,
        has_active_loan
    FROM sorted
    WHERE
        prev_merchant_state      IS NULL
        OR merchant_state        IS DISTINCT FROM prev_merchant_state
        OR risk_score            IS DISTINCT FROM prev_risk_score
        OR loan_count            IS DISTINCT FROM prev_loan_count
        OR has_active_loan       IS DISTINCT FROM prev_has_active_loan
        OR first_transaction_at  IS DISTINCT FROM prev_first_transaction_at
        OR archetype             IS DISTINCT FROM prev_archetype
        OR province              IS DISTINCT FROM prev_province
        OR is_metro              IS DISTINCT FROM prev_is_metro
        OR onboarded_at          IS DISTINCT FROM prev_onboarded_at
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['merchant_id', 'snapshot_at']) }} AS dbt_scd_id,
        merchant_id,
        snapshot_at                                                             AS dbt_valid_from,
        LEAD(snapshot_at) OVER (PARTITION BY merchant_id ORDER BY snapshot_at) AS dbt_valid_to,
        archetype,
        province,
        is_metro,
        onboarded_at,
        first_transaction_at,
        merchant_state,
        risk_score,
        loan_count,
        has_active_loan
    FROM transitions
)

SELECT
    dbt_scd_id,
    merchant_id,
    dbt_valid_from,
    dbt_valid_to,
    dbt_valid_to IS NULL AS dbt_is_current,
    archetype,
    province,
    is_metro,
    onboarded_at,
    first_transaction_at,
    merchant_state,
    risk_score,
    loan_count,
    has_active_loan

FROM final
