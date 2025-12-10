{{
  config(
    materialized='table'
  )
}}

-- Version 1: Full Batch Processing
-- This aggregation model uses full table refresh.
-- Every run rebuilds the entire daily revenue aggregation from scratch.
-- 
-- Use case: Small datasets, infrequent updates, or when you need
-- complete consistency and don't mind the cost of full rebuilds.
-- 
-- Note: This is simple but inefficient for large datasets or frequent updates.

SELECT
    order_date,
    SUM(revenue) AS daily_revenue,
    COUNT(DISTINCT order_id) AS daily_orders,
    SUM(SUM(revenue)) OVER (
        ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_revenue
FROM {{ ref('stg_orders_v1') }}
GROUP BY order_date
ORDER BY order_date

