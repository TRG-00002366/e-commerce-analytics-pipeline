SELECT DISTINCT
    o.region,
    r.region_name
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('regions') }} r
    ON o.region = r.region_code