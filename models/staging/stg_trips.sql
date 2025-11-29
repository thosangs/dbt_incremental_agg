{{
  config(
    materialized='view'
  )
}}

-- Staging model for NYC Yellow Taxi trips
-- Reads from partitioned Parquet files in data/partitioned/ directory
-- Data is partitioned by trip_date for efficient Spark operations
-- This is a view (always fresh) since incremental logic is handled
-- at the aggregation level in agg_daily_revenue.

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
        trip_distance,
        PULocationID,
        DOLocationID,
        payment_type,
        RatecodeID
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
        CAST(trip_distance AS DOUBLE) AS trip_distance,
        CAST(PULocationID AS INT) AS pickup_location_id,
        CAST(DOLocationID AS INT) AS dropoff_location_id,
        CAST(payment_type AS INT) AS payment_type,
        CAST(RatecodeID AS INT) AS rate_code_id
    FROM source
),
final AS (
    SELECT
        trip_id,
        trip_ts,
        dropoff_ts,
        trip_date,
        vendor_id,
        total_amount,
        fare_amount,
        tip_amount,
        tolls_amount,
        passenger_count,
        trip_distance,
        pickup_location_id,
        dropoff_location_id,
        payment_type,
        rate_code_id
    FROM casted
)
SELECT * FROM final
