{{
  config(
    materialized='incremental',
    unique_key='trip_id',
    on_schema_change='append_new_columns'
  )
}}

-- Demo 02: Incremental Event Processing
-- This model processes trips incrementally, only inserting new trips
-- that haven't been seen before (based on trip_id).
-- 
-- Use case: Event streams where new events arrive continuously,
-- and you want to avoid reprocessing existing events.

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
        cast(date_trunc('day', tpep_pickup_datetime) as date) as trip_date,
        cast(VendorID as int) as vendor_id,
        cast(total_amount as double) as total_amount,
        cast(fare_amount as double) as fare_amount,
        cast(tip_amount as double) as tip_amount,
        cast(tolls_amount as double) as tolls_amount,
        cast(passenger_count as int) as passenger_count,
        cast(trip_distance as double) as trip_distance
    from raw_trips
    where tpep_pickup_datetime is not null
      and total_amount is not null
      and total_amount > 0
)
select * from source
{% if is_incremental() %}
where trip_id not in (select trip_id from {{ this }})
{% endif %}

