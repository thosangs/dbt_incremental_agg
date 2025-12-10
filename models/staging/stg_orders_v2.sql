{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_id',
    on_schema_change='append_new_columns'
  )
}}

-- Version 2: Incremental Event Processing
-- This staging model processes orders incrementally using a rolling window.
-- On incremental runs, it reprocesses the last 7 days to catch late-arriving events.
-- Uses delete+insert strategy to efficiently update only affected records.
-- 
-- Use case: Event streams where new events arrive continuously,
-- and you want to reprocess recent data to handle late-arriving events.

WITH raw_orders AS (
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
        -- Extract order_date from timestamp (partition columns year/month/date available for filtering)
        DATE(order_timestamp) AS order_date_extracted
    FROM {{ source('partitioned', 'store_transactions') }}
    WHERE order_timestamp IS NOT NULL
      AND revenue IS NOT NULL
      AND revenue > 0
    {% if is_incremental() %}
      AND DATE(order_timestamp) >= DATE '{{ var("from_date") }}'
    {% endif %}
),
source AS (
    SELECT
        order_id,
        buyer_id,
        order_date,
        order_timestamp,
        product_id,
        quantity,
        unit_price,
        revenue,
        payment_method
    FROM raw_orders
    WHERE order_date IS NOT NULL
),

casted AS (
    SELECT
        CAST(order_id AS VARCHAR) AS order_id,
        CAST(buyer_id AS VARCHAR) AS buyer_id,
        CAST(order_date AS DATE) AS order_date,
        CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
        CAST(product_id AS INT) AS product_id,
        CAST(quantity AS INT) AS quantity,
        CAST(unit_price AS DOUBLE) AS unit_price,
        CAST(revenue AS DOUBLE) AS revenue,
        CAST(payment_method AS VARCHAR) AS payment_method
    FROM source
)
SELECT * FROM casted
