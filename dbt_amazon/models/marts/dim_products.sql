SELECT DISTINCT
    product_id,
    product_name,
    category
FROM {{ ref('stg_orders') }}