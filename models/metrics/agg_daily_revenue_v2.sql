{{
  config(
    materialized='table'
  )
}}

-- Version 2: Incremental Event Processing
-- This aggregation reads from the incremental staging table (stg_trips_v2).
-- Since stg_trips_v2 is incremental, we can rebuild the aggregation
-- from the full staging table each time (which is now efficient
-- because staging only contains new trips on each run).
-- 
-- Note: This is a hybrid approach - incremental staging, full aggregation.

SELECT
    trip_date,
    SUM(total_amount) AS daily_revenue,
    COUNT(*) AS daily_trips,
    SUM(SUM(total_amount)) OVER (
        ORDER BY trip_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_revenue
FROM {{ ref('stg_trips_v2') }}
GROUP BY trip_date
ORDER BY trip_date

