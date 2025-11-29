{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['trip_date'],
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

WITH params AS (
    SELECT (
        COALESCE((SELECT MAX(trip_date) FROM {{ this }}), DATE '1900-01-01')
        - INTERVAL {{ var('reprocess_window_days', 14) }} DAYS
    ) AS reprocess_from
),
base AS (
    SELECT
        trip_date,
        SUM(total_amount) AS daily_revenue,
        COUNT(*) AS daily_trips,
        SUM(passenger_count) AS daily_passengers
    FROM {{ ref('stg_trips') }}
    {% if is_incremental() %}
    WHERE trip_date >= (SELECT reprocess_from FROM params)
    {% endif %}
    GROUP BY 1
),
existing AS (
    {% if is_incremental() %}
    SELECT trip_date, daily_revenue, daily_trips, daily_passengers
    FROM {{ this }}
    WHERE trip_date < (SELECT reprocess_from FROM params)
    {% else %}
    SELECT CAST(NULL AS DATE) AS trip_date, 
           CAST(NULL AS DOUBLE) AS daily_revenue,
           CAST(NULL AS BIGINT) AS daily_trips,
           CAST(NULL AS BIGINT) AS daily_passengers
    WHERE FALSE
    {% endif %}
),
unioned AS (
    SELECT * FROM existing
    UNION ALL
    SELECT * FROM base
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

