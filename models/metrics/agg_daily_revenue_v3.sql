{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='trip_date',
    on_schema_change='append_new_columns'
  )
}}

-- Version 3: Incremental Aggregation with Sliding Window
-- This model demonstrates DuckDB's merge strategy with a sliding window,
-- which efficiently updates only affected date ranges while preserving
-- older, stable data.
-- 
-- Strategy:
-- 1. Reprocess a rolling window (e.g., last 14 days) to catch late-arriving events
-- 2. Preserve older data that is unlikely to change
-- 3. Use MERGE to update/insert records in the sliding window
-- 4. Calculate running revenue over the complete dataset
-- 
-- Use case: Aggregations where late-arriving events need to be handled,
-- and you want to balance freshness with compute efficiency.

{% if is_incremental() %}
WITH params AS (
    SELECT (
        COALESCE((SELECT MAX(trip_date) FROM {{ this }}), DATE '1900-01-01')
        - INTERVAL {{ var('reprocess_window_days', 14) }} DAYS
    ) AS reprocess_from
),

new_aggregates AS (
    SELECT
        trip_date,
        SUM(total_amount) AS daily_revenue,
        COUNT(*) AS daily_trips,
        SUM(passenger_count) AS daily_passengers
    FROM {{ ref('stg_trips_v2') }}
    WHERE trip_date >= (SELECT reprocess_from FROM params)
    GROUP BY 1
),

existing_data AS (
    SELECT
        trip_date,
        daily_revenue,
        daily_trips,
        daily_passengers
    FROM {{ this }}
    WHERE trip_date < (SELECT reprocess_from FROM params)
),

combined AS (
    SELECT * FROM new_aggregates
    UNION ALL
    SELECT * FROM existing_data
)

SELECT
    trip_date,
    daily_revenue,
    daily_trips,
    daily_passengers,
    SUM(daily_revenue) OVER (
        ORDER BY trip_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_revenue
FROM combined
ORDER BY trip_date

{% else %}

WITH base AS (
    SELECT
        trip_date,
        SUM(total_amount) AS daily_revenue,
        COUNT(*) AS daily_trips,
        SUM(passenger_count) AS daily_passengers
    FROM {{ ref('stg_trips_v2') }}
    GROUP BY 1
)
SELECT
    trip_date,
    daily_revenue,
    daily_trips,
    daily_passengers,
    SUM(daily_revenue) OVER (
        ORDER BY trip_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_revenue
FROM base
ORDER BY trip_date

{% endif %}

