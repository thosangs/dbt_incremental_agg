{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='trip_ts',
    on_schema_change='append_new_columns'
  )
}}

-- Version 2: Incremental Event Processing
-- This staging model processes trips incrementally using a rolling window.
-- On incremental runs, it reprocesses the last 7 days to catch late-arriving events.
-- Uses merge strategy to efficiently update only affected records.
-- 
-- Use case: Event streams where new events arrive continuously,
-- and you want to reprocess recent data to handle late-arriving events.

WITH params AS (
    {% if is_incremental() %}
    SELECT (
        COALESCE((SELECT MAX(trip_date) FROM {{ this }}), DATE '1900-01-01')
        - INTERVAL 7 DAYS
    ) AS reprocess_from
    {% else %}
    SELECT DATE '1900-01-01' AS reprocess_from
    {% endif %}
),
raw_trips AS (
    SELECT *
    FROM {{ ref('partition_trips_v1') }}
    -- trip_date is already a partition column, so we can filter efficiently
    {% if is_incremental() %}
    WHERE trip_date >= (SELECT reprocess_from FROM params)
    {% endif %}
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

