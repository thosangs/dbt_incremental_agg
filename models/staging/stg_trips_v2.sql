{{
  config(
    materialized='incremental',
    unique_key='trip_id',
    on_schema_change='append_new_columns'
  )
}}

-- Version 2: Incremental Event Processing
-- This staging model processes trips incrementally, only inserting new trips
-- that haven't been seen before (based on trip_id).
-- 
-- Use case: Event streams where new events arrive continuously,
-- and you want to avoid reprocessing existing events.

WITH raw_trips AS (
    SELECT *
    FROM parquet.`/data/partitioned`
    -- trip_date is already a partition column, so we can filter efficiently
),
source AS (
    SELECT
        -- Read columns as-is first (INT32 from Parquet), then cast
        -- trip_date is already a partition column in partitioned data
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        trip_date,
        VendorID,
        total_amount,
        fare_amount,
        tip_amount,
        tolls_amount,
        passenger_count,
        trip_distance
    FROM raw_trips
    WHERE tpep_pickup_datetime IS NOT NULL
      AND total_amount IS NOT NULL
      AND total_amount > 0
      AND trip_date IS NOT NULL
),
casted AS (
    SELECT
        -- Generate a unique trip_id from row number
        ROW_NUMBER() OVER (ORDER BY tpep_pickup_datetime, VendorID) AS trip_id,
        CAST(tpep_pickup_datetime AS TIMESTAMP) AS trip_ts,
        CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
        CAST(trip_date AS DATE) AS trip_date,
        CAST(VendorID AS INT) AS vendor_id,
        CAST(total_amount AS DOUBLE) AS total_amount,
        CAST(fare_amount AS DOUBLE) AS fare_amount,
        CAST(tip_amount AS DOUBLE) AS tip_amount,
        CAST(tolls_amount AS DOUBLE) AS tolls_amount,
        CAST(passenger_count AS INT) AS passenger_count,
        CAST(trip_distance AS DOUBLE) AS trip_distance
    FROM source
)
SELECT * FROM casted
{% if is_incremental() %}
WHERE trip_id NOT IN (SELECT trip_id FROM {{ this }})
{% endif %}

