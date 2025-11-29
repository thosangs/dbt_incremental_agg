{% macro log_processed_data(model) %}
  {% if execute %}
    {% set model_name = model.identifier %}
    {% set is_incremental = 'v3' in model_name %}
    
    {# For staging models - calculate size of Parquet files read #}
    {% if 'stg_trips' in model_name %}
      {# Get partition count and row count from partitioned data #}
      {% set partition_query %}
        SELECT COUNT(DISTINCT trip_date) AS partition_count
        FROM parquet.`/data/partitioned`
      {% endset %}
      
      {% set partition_result = run_query(partition_query) %}
      {% set partition_count = 0 %}
      {% if partition_result and partition_result.columns[0].values() %}
        {% set partition_count = partition_result.columns[0].values()[0] %}
      {% endif %}
      
      {# Get row count for estimation #}
      {% set row_query %}
        SELECT COUNT(*) AS total_rows
        FROM parquet.`/data/partitioned`
        WHERE tpep_pickup_datetime IS NOT NULL
          AND total_amount IS NOT NULL
          AND total_amount > 0
      {% endset %}
      
      {% set row_result = run_query(row_query) %}
      {% if row_result and row_result.columns[0].values() %}
        {% set total_rows = row_result.columns[0].values()[0] %}
        {# Estimate: ~500 bytes per row for NYC taxi data #}
        {% set size_mb = (total_rows * 500.0 / 1024.0 / 1024.0) | round(2) %}
        {{ log("📊 Processed: ~" ~ size_mb ~ " MB from " ~ partition_count ~ " partitions (" ~ total_rows ~ " rows)", info=True) }}
      {% endif %}
    {% endif %}
    
    {# For aggregation models #}
    {% if 'agg_daily_revenue' in model_name %}
      {% if is_incremental %}
        {# For incremental: try to get max date from the table (now exists after build) #}
        {# If its the first run, max_date will be NULL and we process all #}
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
            {# Initial run - process all data #}
            {% set size_query %}
              SELECT COUNT(*) AS row_count
              FROM {{ ref('stg_trips') }}
            {% endset %}
            
            {% set results = run_query(size_query) %}
            {% if results and results.columns[0].values() %}
              {% set row_count = results.columns[0].values()[0] %}
              {% set size_mb = (row_count * 500.0 / 1024.0 / 1024.0) | round(2) %}
              {{ log("📊 Processed: ~" ~ size_mb ~ " MB (initial, " ~ row_count ~ " rows)", info=True) }}
            {% endif %}
          {% else %}
            {# Incremental run - calculate reprocess window #}
            {% set size_query %}
              SELECT 
                COUNT(*) AS row_count,
                COUNT(DISTINCT trip_date) AS day_count
              FROM {{ ref('stg_trips') }}
              WHERE trip_date >= DATE '{{ reprocess_from }}'
            {% endset %}
            
            {% set results = run_query(size_query) %}
            {% if results and results.columns[0].values() %}
              {% set row_count = results.columns[0].values()[0] %}
              {% set day_count = results.columns[1].values()[0] %}
              {% set size_mb = (row_count * 500.0 / 1024.0 / 1024.0) | round(2) %}
              {{ log("📊 Processed: ~" ~ size_mb ~ " MB (" ~ day_count ~ " days, " ~ row_count ~ " rows)", info=True) }}
            {% endif %}
          {% endif %}
        {% endif %}
      {% else %}
        {# Full refresh (v1, v2) - get all data size #}
        {% set size_query %}
          SELECT COUNT(*) AS row_count
          FROM {{ ref('stg_trips') }}
        {% endset %}
        
        {% set results = run_query(size_query) %}
        {% if results and results.columns[0].values() %}
          {% set row_count = results.columns[0].values()[0] %}
          {% set size_mb = (row_count * 500.0 / 1024.0 / 1024.0) | round(2) %}
          {{ log("📊 Processed: ~" ~ size_mb ~ " MB (full, " ~ row_count ~ " rows)", info=True) }}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}

