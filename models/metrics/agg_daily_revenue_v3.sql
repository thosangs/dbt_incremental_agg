{{
  config(
    materialized='incremental',
    file_format='parquet',
    partition_by=['trip_date'],
    incremental_strategy='insert_overwrite',
    on_schema_change='append_new_columns'
  )
}}

-- Version 3: Incremental Aggregation with Partition Overwrite
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

WITH
{% if is_incremental() %}
params AS (
    SELECT (
        COALESCE((SELECT MAX(trip_date) FROM {{ this }}), DATE '1900-01-01')
        - INTERVAL {{ var('reprocess_window_days', 14) }} DAYS
    ) AS reprocess_from
),
{% endif %}

base AS (
    SELECT
        trip_date,
        SUM(total_amount) AS daily_revenue,
        COUNT(*) AS daily_trips,
        SUM(passenger_count) AS daily_passengers
    FROM {{ ref('stg_trips_v2') }}
    {% if is_incremental() %}
    WHERE trip_date >= (SELECT reprocess_from FROM params)
    {% endif %}
    GROUP BY 1
),

unioned AS (
    SELECT trip_date, daily_revenue, daily_trips, daily_passengers FROM base

    {% if is_incremental() %}
    UNION ALL

    SELECT trip_date, daily_revenue, daily_trips, daily_passengers
    FROM {{ this }}
    WHERE trip_date < (SELECT reprocess_from FROM params)
    {% endif %}
)
SELECT
    trip_date,
    daily_revenue,
    daily_trips,
    daily_passengers,
    SUM(daily_revenue) OVER (
        ORDER BY trip_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_revenue
FROM unioned
ORDER BY trip_date

