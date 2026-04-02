SELECT
    event_id,
    event_type,
    order_id,
    customer_id,
    customer_segment,

    product_id,
    product_name,
    category,

    quantity,
    unit_price,
    discount,

    payment_method,
    shipping_type,
    region,

    event_timestamp,
    hour

FROM {{ source('silver', 'cleaned_orders') }}