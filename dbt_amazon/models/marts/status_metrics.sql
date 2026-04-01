{{ config(materialized='table') }}

WITH base AS (

    SELECT
        category,
        event_type,
        order_id
    FROM {{ ref('stg_orders') }}

),

aggregated AS (

    SELECT
        category,

        COUNT(CASE WHEN event_type = 'ORDER_RETURNED' THEN 1 END) AS returns,

        COUNT(CASE WHEN event_type = 'ORDER_CANCELLED' THEN 1 END) AS cancellations,

        COUNT(order_id) AS total_orders

    FROM base

    GROUP BY category

)

SELECT
    category,
    returns,
    cancellations,
    total_orders,

    COALESCE(returns / NULLIF(total_orders, 0), 0) AS return_rate,

    COALESCE(cancellations / NULLIF(total_orders, 0), 0) AS cancellation_rate

FROM aggregated