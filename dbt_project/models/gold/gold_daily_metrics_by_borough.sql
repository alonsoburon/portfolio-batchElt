{{
  config(
    materialized='table',
    tags=['gold']
  )
}}

-- Gold layer: Daily metrics aggregated by pickup borough
-- Provides geographic analysis of trip patterns and revenue distribution

SELECT
  date_trunc('day', tpep_pickup_datetime) AS day,
  pickup_borough,
  COUNT(*) AS total_trips,
  SUM(total_amount) AS total_fare
FROM {{ ref('silver_taxi_trips') }}
WHERE pickup_borough IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2

