SELECT DISTINCT
    CAST(event_timestamp AS DATE) AS date,
    YEAR(event_timestamp) AS year,
    MONTH(event_timestamp) AS month,
    DAY(event_timestamp) AS day,
    HOUR(event_timestamp) AS hour
FROM {{ ref('stg_orders') }}