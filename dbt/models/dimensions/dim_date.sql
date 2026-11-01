{{
    config(
        materialized='table',
    )
}}

WITH spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2025-01-01' as date)",
        end_date="cast('2027-01-01' as date)"
    ) }}
),

holidays AS (
    SELECT
        CAST(date_day AS DATE) AS holiday_date,
        holiday_name
    FROM {{ ref('sa_public_holidays') }}
),

final AS (
    SELECT
        CAST(spine.date_day AS DATE)                                                   AS date_day,
        EXTRACT(YEAR    FROM CAST(spine.date_day AS DATE))                             AS year,
        EXTRACT(MONTH   FROM CAST(spine.date_day AS DATE))                             AS month_number,
        EXTRACT(QUARTER FROM CAST(spine.date_day AS DATE))                             AS quarter,
        EXTRACT(DAY     FROM CAST(spine.date_day AS DATE))                             AS day_of_month,
        EXTRACT(DAYOFWEEK FROM CAST(spine.date_day AS DATE))                           AS day_of_week,
        holidays.holiday_name                                                          AS holiday_name,
        holidays.holiday_name IS NOT NULL                                              AS is_sa_holiday,
        CAST(spine.date_day AS DATE) = LAST_DAY(CAST(spine.date_day AS DATE), MONTH)  AS is_month_end,
        CAST(spine.date_day AS DATE) = LAST_DAY(CAST(spine.date_day AS DATE), QUARTER) AS is_quarter_end,
        EXTRACT(DAYOFWEEK FROM CAST(spine.date_day AS DATE)) NOT IN (1, 7)
            AND holidays.holiday_name IS NULL                                          AS is_business_day,
        EXTRACT(DAY FROM CAST(spine.date_day AS DATE)) IN (2, 3, 4, 25, 26, 27)       AS is_payday_period

    FROM spine
    LEFT JOIN holidays
        ON CAST(spine.date_day AS DATE) = holidays.holiday_date
)

SELECT * FROM final
