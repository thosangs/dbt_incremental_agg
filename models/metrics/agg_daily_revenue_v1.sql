{{
  config(
    materialized='table'
  )
}}

SELECT
    order_date,
    SUM(revenue) AS daily_revenue,
    COUNT(DISTINCT order_id) AS daily_orders
FROM {{ ref('stg_orders_v1') }}
GROUP BY order_date
ORDER BY order_date

