{{
  config(
    materialized='view'
  )
}}

-- Staging model for NYC Yellow Taxi trips
-- Reads from parquet files in data/raw/ directory
-- This is a view (always fresh) since incremental logic is handled
-- at the aggregation level in agg_daily_revenue.

with raw_trips as (
    select *
    from parquet.`/data/raw/yellow_tripdata_*.parquet`
),
source as (
    select
        -- Generate a unique trip_id from row number
        row_number() over (order by tpep_pickup_datetime, VendorID) as trip_id,
        cast(tpep_pickup_datetime as timestamp) as trip_ts,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_ts,
        cast(VendorID as int) as vendor_id,
        cast(total_amount as double) as total_amount,
        cast(fare_amount as double) as fare_amount,
        cast(tip_amount as double) as tip_amount,
        cast(tolls_amount as double) as tolls_amount,
        cast(passenger_count as int) as passenger_count,
        cast(trip_distance as double) as trip_distance,
        cast(PULocationID as int) as pickup_location_id,
        cast(DOLocationID as int) as dropoff_location_id,
        cast(payment_type as int) as payment_type,
        cast(RatecodeID as int) as rate_code_id
    from raw_trips
    where tpep_pickup_datetime is not null
      and total_amount is not null
      and total_amount > 0
),
final as (
    select
        trip_id,
        trip_ts,
        dropoff_ts,
        cast(date_trunc('day', trip_ts) as date) as trip_date,
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
    from source
)
select * from final

