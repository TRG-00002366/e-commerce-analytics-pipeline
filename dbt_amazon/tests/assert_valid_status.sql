SELECT *
FROM {{ ref('fact_orders') }}
WHERE event_type NOT IN ('ORDER_CREATED')