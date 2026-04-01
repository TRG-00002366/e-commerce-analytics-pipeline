{{ config(materialized='table') }}

WITH product_agg AS (

    SELECT
        category,
        product_id,
        product_name,
        SUM(COALESCE(quantity, 0)) AS total_qty

    FROM {{ ref('stg_orders') }}

    WHERE event_type = 'ORDER_CREATED'

    GROUP BY 1, 2, 3

),

ranked AS (

    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY category
            ORDER BY total_qty DESC
        ) AS rank

    FROM product_agg

)

SELECT *
FROM ranked
WHERE rank <= 10