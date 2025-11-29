{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['trip_date'],
    on_schema_change='append_new_columns'
  )
}}

-- Demo 03: Incremental Aggregation with Partition Overwrite
-- This model demonstrates Spark's insert_overwrite strategy, which
-- efficiently overwrites only affected partitions while preserving
-- older, stable partitions.
-- 
-- Strategy:
-- 1. Reprocess a rolling window (e.g., last 14 days) to catch late-arriving events
-- 2. Preserve older partitions that are unlikely to change
-- 3. Use partition overwrite for efficient updates
-- 
-- Use case: Aggregations where late-arriving events need to be handled,
-- and you want to balance freshness with compute efficiency.

with params as (
    select (
        coalesce((select max(trip_date) from {{ this }}), date '1900-01-01')
        - interval {{ var('reprocess_window_days', 14) }} days
    ) as reprocess_from
),
base as (
    select
        trip_date,
        sum(total_amount) as daily_revenue,
        count(*) as daily_trips,
        sum(passenger_count) as daily_passengers
    from {{ ref('stg_trips') }}
    {% if is_incremental() %}
    where trip_date >= (select reprocess_from from params)
    {% endif %}
    group by 1
),
existing as (
    {% if is_incremental() %}
    select trip_date, daily_revenue, daily_trips, daily_passengers
    from {{ this }}
    where trip_date < (select reprocess_from from params)
    {% else %}
    select cast(null as date) as trip_date, 
           cast(null as double) as daily_revenue,
           cast(null as bigint) as daily_trips,
           cast(null as bigint) as daily_passengers
    where false
    {% endif %}
),
unioned as (
    select * from existing
    union all
    select * from base
)
select
    trip_date,
    daily_revenue,
    daily_trips,
    daily_passengers,
    sum(daily_revenue) over (
        order by trip_date rows between unbounded preceding and current row
    ) as running_revenue
from unioned
order by trip_date
