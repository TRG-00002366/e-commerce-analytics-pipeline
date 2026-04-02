{{ config(materialized='table') }}

SELECT
    event_type,
    payment_method,
    COUNT(*) AS payment_count

FROM {{ ref('stg_orders') }}

WHERE event_type = 'PAYMENT_COMPLETED'

GROUP BY event_type