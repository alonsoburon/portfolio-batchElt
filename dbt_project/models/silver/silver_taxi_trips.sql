{{
  config(
    materialized='table',
    tags=['silver']
  )
}}

-- Silver layer: Cleaned and enriched taxi trip data
-- Joins taxi trips with zone lookups and weather data
-- Applies data quality filters and standardizes column names

-- Note: Weather data comes from dlt as separate tables due to JSON structure
-- We need to reconstruct the hourly time series by joining on _dlt_parent_id

WITH taxi_trips AS (
  SELECT * FROM {{ source('bronze_layer', 'yellow_taxi_trips') }}
),

taxi_zones AS (
  SELECT
    "Location ID" AS location_id,
    borough,
    zone
  FROM {{ source('bronze_layer', 'taxi_zones') }}
),

weather_data AS (
  SELECT
    t.value AS observation_time,
    temp.value AS temperature,
    prec.value AS precipitation
  FROM {{ source('bronze_layer', 'hourly_weather__hourly__time') }} AS t
  LEFT JOIN {{ source('bronze_layer', 'hourly_weather__hourly__temperature_2m') }} AS temp
    ON t._dlt_parent_id = temp._dlt_parent_id
    AND t._dlt_list_idx = temp._dlt_list_idx
  LEFT JOIN {{ source('bronze_layer', 'hourly_weather__hourly__precipitation') }} AS prec
    ON t._dlt_parent_id = prec._dlt_parent_id
    AND t._dlt_list_idx = prec._dlt_list_idx
)

SELECT
  tt.tpep_pickup_datetime,
  tt.tpep_dropoff_datetime,
  tt.passenger_count,
  tt.trip_distance,
  tt.total_amount,
  pickup_zone.borough AS pickup_borough,
  pickup_zone.zone AS pickup_zone,
  dropoff_zone.borough AS dropoff_borough,
  dropoff_zone.zone AS dropoff_zone,
  wd.temperature,
  wd.precipitation
FROM taxi_trips tt
LEFT JOIN taxi_zones AS pickup_zone
  ON tt."PULocationID" = pickup_zone.location_id
LEFT JOIN taxi_zones AS dropoff_zone
  ON tt."DOLocationID" = dropoff_zone.location_id
LEFT JOIN weather_data wd
  ON date_trunc('hour', CAST(tt.tpep_pickup_datetime AS TIMESTAMP)) = wd.observation_time
WHERE
  tt.total_amount >= 0  -- Filter out negative fares (data quality issue)
  AND pickup_zone.borough IS NOT NULL  -- Only include trips with valid pickup locations

