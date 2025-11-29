{{
  config(
    materialized='table'
  )
}}

-- Demo 01: Full Batch Processing
-- This aggregation model also uses full table refresh.
-- Every run rebuilds the entire daily revenue aggregation from scratch.
-- 
-- Note: This is simple but inefficient for large datasets or frequent updates.

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
