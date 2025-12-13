{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_id',
    on_schema_change='append_new_columns'
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
WHERE order_timestamp IS NOT NULL
{% if is_incremental() %}
    AND DATE(order_timestamp) >= DATE '{{ var("from_date") }}'
{% endif %}
