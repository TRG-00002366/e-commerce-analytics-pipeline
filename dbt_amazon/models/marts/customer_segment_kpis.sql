{{ config(materialized='table') }}

SELECT
    customer_segment,

    SUM(
        COALESCE((unit_price - discount) * quantity, 0)
    ) AS revenue,

    AVG(COALESCE(quantity, 0)) AS avg_basket_size

FROM {{ ref('stg_orders') }}

WHERE event_type = 'ORDER_CREATED'

GROUP BY customer_segment