SELECT *
FROM {{ ref('fact_orders') }}
WHERE revenue < 0