SELECT
    order_id,
    customer_id,
    product_id,
    region,
    event_type,

    quantity,
    unit_price,
    discount,

    (quantity * unit_price - discount) AS revenue,

    event_timestamp

FROM {{ ref('stg_orders') }}
WHERE event_type = 'ORDER_CREATED'