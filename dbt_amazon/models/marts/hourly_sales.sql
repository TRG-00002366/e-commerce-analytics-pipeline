{{ config(materialized='table') }}

SELECT
    CAST(event_timestamp AS DATE) AS date,
    EXTRACT(HOUR FROM event_timestamp) AS hour,

    COUNT(order_id) AS total_orders,

    SUM(
        COALESCE((unit_price - discount) * quantity, 0)
    ) AS total_revenue,

    AVG(
        COALESCE((unit_price - discount) * quantity, 0)
    ) AS avg_order_value

FROM {{ ref('stg_orders') }}

WHERE event_type = 'ORDER_CREATED'

GROUP BY 1, 2