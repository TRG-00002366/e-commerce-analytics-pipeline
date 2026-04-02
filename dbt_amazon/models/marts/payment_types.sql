{{ config(materialized='table') }}

WITH base AS (
    SELECT
        payment_method,
        COUNT(*) AS payment_count
    FROM {{ ref('stg_orders') }}
    WHERE event_type = 'PAYMENT_COMPLETED'
    GROUP BY payment_method
)

SELECT
    payment_method,
    payment_count,
    payment_count * 1.0 / SUM(payment_count) OVER () AS pct_of_total
FROM base