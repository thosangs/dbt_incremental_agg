{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_date',
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
WITH new_aggregates AS (
    SELECT
        order_date,
        SUM(revenue) AS daily_revenue,
        SUM(revenue) AS running_revenue,
        COUNT(DISTINCT order_id) AS daily_orders,
        COUNT(DISTINCT buyer_id) AS daily_buyers
    FROM {{ ref('stg_orders_v2') }}
    WHERE order_date >= '{{ var("from_date") }}'
    GROUP BY 1
),

existing_data AS (
    SELECT
        order_date,
        daily_revenue,
        daily_orders,
        daily_buyers,
        running_revenue
    FROM {{ this }}
    WHERE order_date = DATE('{{ var("from_date") }}') - INTERVAL '1' DAY
),

combined AS (
    SELECT * FROM new_aggregates
    UNION ALL
    SELECT * FROM existing_data
)

SELECT
    order_date,
    daily_revenue,
    daily_orders,
    daily_buyers,
    SUM(running_revenue) OVER (
        ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_revenue
FROM combined
ORDER BY order_date

{% else %}

WITH base AS (
    SELECT
        order_date,
        SUM(revenue) AS daily_revenue,
        COUNT(DISTINCT order_id) AS daily_orders,
        COUNT(DISTINCT buyer_id) AS daily_buyers
    FROM {{ ref('stg_orders_v2') }}
    GROUP BY 1
)
SELECT
    order_date,
    daily_revenue,
    daily_orders,
    daily_buyers,
    SUM(daily_revenue) OVER (
        ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_revenue
FROM base
ORDER BY order_date

{% endif %}

