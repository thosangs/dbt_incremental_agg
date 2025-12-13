{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE(order_timestamp) AS order_date,
    order_id,
    buyer_id,
    order_timestamp,
    product_id,
    quantity,
    unit_price,
    revenue,
    payment_method
FROM {{ source('partitioned', 'store_transactions') }}
