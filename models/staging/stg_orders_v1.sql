{{
    config(
        materialized='view'
    )
}}

-- Staging model for Store Transactions
-- Reads from Hive-partitioned Parquet files in /data/partitioned/ directory
-- Data is partitioned by year=YYYY/month=MM/date=DD for efficient filtering
-- DuckDB automatically discovers partitions and includes partition columns (year, month, date)
-- This is a view (always fresh) since incremental logic is handled
-- at the aggregation level in agg_daily_revenue.

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
        payment_method,
        order_date_extracted
    FROM raw_orders
    WHERE order_date IS NOT NULL
      AND order_date_extracted IS NOT NULL
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
),
final AS (
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
    FROM casted
)
SELECT * FROM final
