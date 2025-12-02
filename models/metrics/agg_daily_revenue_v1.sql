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
    trip_date,
    SUM(total_amount) AS daily_revenue,
    COUNT(*) AS daily_trips,
    SUM(SUM(total_amount)) OVER (
        ORDER BY trip_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_revenue
FROM {{ ref('stg_trips_v1') }}
GROUP BY trip_date
ORDER BY trip_date

