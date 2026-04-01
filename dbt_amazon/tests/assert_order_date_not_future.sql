SELECT *
FROM {{ ref('fact_orders') }}
WHERE event_timestamp < DATEADD(minute, 1, CURRENT_TIMESTAMP)