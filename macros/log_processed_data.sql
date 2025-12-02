{% macro log_processed_data(model) %}
  {% if execute %}
    {% set model_name = model.identifier %}
    {% set materialization = 'view' %}
    {% set is_incremental = false %}
    
    {# Try to get model config from graph #}
    {% for node in graph.nodes.values() %}
      {% if node.name == model.identifier %}
        {% set materialization = node.config.materialized %}
        {% set is_incremental = materialization == 'incremental' %}
        {% break %}
      {% endif %}
    {% endfor %}
    
    {# For staging models - log row count directly from Parquet files #}
    {% if 'stg_trips' in model_name %}
      {% set count_query %}
        SELECT COUNT(*) AS row_count
        FROM read_parquet('/data/partitioned/**/*.parquet')
        WHERE tpep_pickup_datetime IS NOT NULL
          AND total_amount IS NOT NULL
          AND total_amount > 0
      {% endset %}
      
      {% set results = run_query(count_query) %}
      {% if results and results.columns[0].values() %}
        {% set row_count = results.columns[0].values()[0] %}
        {{ log("📊 Processed: " ~ row_count ~ " rows from Parquet files", info=True) }}
      {% endif %}
    {% endif %}
    
    {# For aggregation models #}
    {% if 'agg_daily_revenue' in model_name %}
      {% if is_incremental %}
        {# For incremental: get max date from the built model table #}
        {% set reprocess_query %}
          SELECT 
            COALESCE((SELECT MAX(trip_date) FROM {{ model }}), DATE '1900-01-01')
            - INTERVAL {{ var('reprocess_window_days', 14) }} DAYS AS reprocess_from
        {% endset %}
        
        {% set date_result = run_query(reprocess_query) %}
        {% if date_result and date_result.columns[0].values() %}
          {% set reprocess_from = date_result.columns[0].values()[0] %}
          
          {# Check if this is initial run (reprocess_from is very old) #}
          {% if reprocess_from < '2000-01-01' %}
            {# Initial run - count all rows from Parquet files #}
            {% set count_query %}
              SELECT COUNT(*) AS row_count
              FROM read_parquet('/data/partitioned/**/*.parquet')
              WHERE tpep_pickup_datetime IS NOT NULL
                AND total_amount IS NOT NULL
                AND total_amount > 0
            {% endset %}
            
            {% set results = run_query(count_query) %}
            {% if results and results.columns[0].values() %}
              {% set row_count = results.columns[0].values()[0] %}
              {{ log("📊 Processed: " ~ row_count ~ " rows from Parquet files (initial)", info=True) }}
            {% endif %}
          {% else %}
            {# Incremental run - count rows in reprocess window from Parquet files #}
            {% set count_query %}
              SELECT 
                COUNT(*) AS row_count,
                COUNT(DISTINCT DATE(tpep_pickup_datetime)) AS day_count
              FROM read_parquet('/data/partitioned/**/*.parquet')
              WHERE tpep_pickup_datetime IS NOT NULL
                AND total_amount IS NOT NULL
                AND total_amount > 0
                AND DATE(tpep_pickup_datetime) >= DATE '{{ reprocess_from }}'
            {% endset %}
            
            {% set results = run_query(count_query) %}
            {% if results and results.columns[0].values() %}
              {% set row_count = results.columns[0].values()[0] %}
              {% set day_count = results.columns[1].values()[0] %}
              {{ log("📊 Processed: " ~ row_count ~ " rows (" ~ day_count ~ " days) from Parquet files", info=True) }}
            {% endif %}
          {% endif %}
        {% endif %}
      {% else %}
        {# Full refresh (v1, v2) - count rows from Parquet files #}
        {% set count_query %}
          SELECT COUNT(*) AS row_count
          FROM read_parquet('/data/partitioned/**/*.parquet')
          WHERE tpep_pickup_datetime IS NOT NULL
            AND total_amount IS NOT NULL
            AND total_amount > 0
        {% endset %}
        
        {% set results = run_query(count_query) %}
        {% if results and results.columns[0].values() %}
          {% set row_count = results.columns[0].values()[0] %}
          {{ log("📊 Processed: " ~ row_count ~ " rows from Parquet files (full)", info=True) }}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}
