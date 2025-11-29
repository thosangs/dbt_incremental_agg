{{
  config(
    materialized='table'
  )
}}

-- Demo 02: Incremental Event Processing
-- This aggregation reads from the incremental staging table.
-- Since stg_trips is incremental, we can rebuild the aggregation
-- from the full staging table each time (which is now efficient
-- because staging only contains new trips on each run).
-- 
-- Note: This is a hybrid approach - incremental staging, full aggregation.

select
    trip_date,
    sum(total_amount) as daily_revenue,
    count(*) as daily_trips,
    sum(passenger_count) as daily_passengers,
    sum(sum(total_amount)) over (
        order by trip_date rows between unbounded preceding and current row
    ) as running_revenue
from {{ ref('stg_trips') }}
group by trip_date
order by trip_date
