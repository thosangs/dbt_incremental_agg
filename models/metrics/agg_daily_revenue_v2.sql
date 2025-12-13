{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_date',
    on_schema_change='append_new_columns'
  )
}}

SELECT
    order_date,
    SUM(revenue) AS daily_revenue,
    COUNT(DISTINCT order_id) AS daily_orders
FROM {{ ref('stg_orders_v2') }}
GROUP BY order_date
{% if is_incremental() %}
AND order_date >= DATE '{{ var("from_date") }}'
{% endif %}

