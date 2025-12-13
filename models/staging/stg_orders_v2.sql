{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_id',
    on_schema_change='append_new_columns'
  )
}}

SELECT
    order_id,
    buyer_id,
    order_date,
    order_timestamp,
    product_id,
    quantity,
    unit_price,
    revenue,
    payment_method,
    DATE(order_timestamp) AS order_date
FROM {{ source('partitioned', 'store_transactions') }}
WHERE order_timestamp IS NOT NULL
{% if is_incremental() %}
    AND DATE(order_timestamp) >= DATE '{{ var("from_date") }}'
{% endif %}
