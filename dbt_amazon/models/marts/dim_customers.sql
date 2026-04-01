SELECT
    customer_id,
    ANY_VALUE(customer_segment) AS customer_segment
FROM {{ ref('stg_orders') }}
GROUP BY customer_id