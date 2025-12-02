{{
    config(
        materialized='table',
        file_format='parquet',
        partition_by=['trip_date']
    )
}}

-- Partition NYC Taxi Trip Data by trip_date
-- Reads monthly Parquet files from /data/raw and writes daily-partitioned Parquet to /data/partitioned
-- This model extracts trip_date from tpep_pickup_datetime and partitions the data by day
-- The post-hook writes the partitioned data to /data/partitioned

WITH raw_trips AS (
    SELECT *
    FROM parquet.`/data/raw`
    WHERE tpep_pickup_datetime IS NOT NULL
      AND total_amount IS NOT NULL
      AND total_amount > 0
),
with_trip_date AS (
    SELECT
        *,
        DATE(tpep_pickup_datetime) AS trip_date
    FROM raw_trips
    WHERE DATE(tpep_pickup_datetime) IS NOT NULL
)
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
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
    RatecodeID,
    trip_date
FROM with_trip_date

