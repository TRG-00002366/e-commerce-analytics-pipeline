{{ config(materialized='table') }}

SELECT
    customer_segment,

    SUM(
        COALESCE((unit_price - discount) * quantity, 0)
    ) AS revenue,

    AVG(COALESCE(quantity, 0)) AS avg_basket_size,

    CASE 
        WHEN customer_segment = 'REGULAR' THEN 800000000
        WHEN customer_segment = 'PRIME' THEN 20000000
        WHEN customer_segment = 'VIP' THEN 90000000
        ELSE 0 
    END AS target

FROM {{ ref('stg_orders') }}

WHERE event_type = 'ORDER_CREATED'

GROUP BY customer_segment