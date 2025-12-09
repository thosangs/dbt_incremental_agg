{{
  config(
    materialized='table'
  )
}}

-- Version 2: Incremental Event Processing
-- This aggregation reads from the incremental staging table (stg_orders_v2).
-- Since stg_orders_v2 is incremental, we can rebuild the aggregation
-- from the full staging table each time (which is now efficient
-- because staging only contains new orders on each run).
-- 
-- Note: This is a hybrid approach - incremental staging, full aggregation.

SELECT
    order_date,
    SUM(revenue) AS daily_revenue,
    COUNT(DISTINCT order_id) AS daily_orders,
    SUM(SUM(revenue)) OVER (
        ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_revenue
FROM {{ ref('stg_orders_v2') }}
GROUP BY order_date
ORDER BY order_date

