{{ config(materialized='table') }}

SELECT
    region,

    SUM(
        COALESCE((unit_price - discount) * quantity, 0)
    ) AS revenue

FROM {{ ref('stg_orders') }}

WHERE event_type = 'ORDER_CREATED'

GROUP BY region