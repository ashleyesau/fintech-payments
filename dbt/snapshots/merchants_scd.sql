{% snapshot merchants_scd %}

{{
    config(
        unique_key='merchant_id',
        strategy='check',
        updated_at='snapshot_at',
        check_cols=[
            'archetype', 'province', 'is_metro', 'onboarded_at',
            'first_transaction_at', 'merchant_state', 'risk_score',
            'loan_count', 'has_active_loan'
        ],
    )
}}

WITH latest AS (
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
        ROW_NUMBER() OVER (PARTITION BY merchant_id ORDER BY snapshot_at DESC) AS _rn
    FROM {{ ref('stg_merchants') }}
)

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
FROM latest
WHERE _rn = 1

{% endsnapshot %}
