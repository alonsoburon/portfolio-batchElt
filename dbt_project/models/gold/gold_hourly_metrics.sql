{{
  config(
    materialized='table',
    tags=['gold']
  )
}}

-- Gold layer: Hourly aggregated metrics for time series analysis
-- Includes trip volume, fare averages, distance metrics, and weather data

-- Using MAX() for weather fields since they're constant within each hour
-- but we need to aggregate them for the GROUP BY

SELECT
  date_trunc('hour', tpep_pickup_datetime) AS hour,
  COUNT(*) AS total_trips,
  AVG(total_amount) AS avg_fare,
  AVG(trip_distance) AS avg_distance,
  MAX(temperature) AS temperature,
  MAX(precipitation) AS precipitation
FROM {{ ref('silver_taxi_trips') }}
GROUP BY 1
ORDER BY 1

