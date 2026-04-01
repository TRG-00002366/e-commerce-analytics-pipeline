SELECT DISTINCT
    customer_id,
    customer_segment
FROM {{ ref('stg_orders') }}