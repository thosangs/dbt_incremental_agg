{% macro log_processed_data(model) %}
  {% if execute %}
    {% set model_name = model.name %}
    {% set materialization = model.config.materialized %}
    {% set is_incremental = materialization == 'incremental' %}
    
    {# For staging models - calculate size of Parquet files read #}
    {% if 'stg_trips' in model_name %}
      {% set size_query %}
        SELECT 
          SUM(input_file_length) / 1024.0 / 1024.0 AS size_mb,
          COUNT(DISTINCT input_file_name) AS file_count
        FROM (
          SELECT DISTINCT input_file_name, input_file_length
          FROM parquet.`/data/raw/yellow_tripdata_*.parquet`
        )
      {% endset %}
      
      {% set results = run_query(size_query) %}
      {% if results and results.columns[0].values() %}
        {% set size_mb = results.columns[0].values()[0] | round(2) %}
        {% set file_count = results.columns[1].values()[0] %}
        {{ log("📊 Processed: " ~ size_mb ~ " MB from " ~ file_count ~ " files", info=True) }}
      {% endif %}
    {% endif %}
    
    {# For aggregation models #}
    {% if 'agg_daily_revenue' in model_name %}
      {% if is_incremental %}
        {# For incremental: try to get max date from the table (now exists after build) #}
        {# If it's the first run, max_date will be NULL and we process all #}
        {% set reprocess_query %}
          SELECT 
            COALESCE((SELECT MAX(trip_date) FROM {{ model.schema }}.{{ model.alias }}), DATE '1900-01-01')
            - INTERVAL {{ var('reprocess_window_days', 14) }} DAYS AS reprocess_from
        {% endset %}
        
        {% set date_result = run_query(reprocess_query) %}
        {% if date_result and date_result.columns[0].values() %}
          {% set reprocess_from = date_result.columns[0].values()[0] %}
          
          {# Check if this is initial run (reprocess_from is very old) #}
          {% if reprocess_from < '2000-01-01' %}
            {# Initial run - process all data #}
            {% set size_query %}
              SELECT 
                SUM(input_file_length) / 1024.0 / 1024.0 AS size_mb
              FROM (
                SELECT DISTINCT input_file_length
                FROM {{ ref('stg_trips') }}
              )
            {% endset %}
            
            {% set results = run_query(size_query) %}
            {% if results and results.columns[0].values() %}
              {% set size_mb = results.columns[0].values()[0] | round(2) %}
              {{ log("📊 Processed: " ~ size_mb ~ " MB (initial)", info=True) }}
            {% endif %}
          {% else %}
            {# Incremental run - calculate reprocess window #}
            {% set size_query %}
              SELECT 
                SUM(input_file_length) / 1024.0 / 1024.0 AS size_mb,
                COUNT(DISTINCT trip_date) AS day_count
              FROM (
                SELECT DISTINCT trip_date, input_file_length
                FROM {{ ref('stg_trips') }}
                WHERE trip_date >= DATE '{{ reprocess_from }}'
              )
            {% endset %}
            
            {% set results = run_query(size_query) %}
            {% if results and results.columns[0].values() %}
              {% set size_mb = results.columns[0].values()[0] | round(2) %}
              {% set day_count = results.columns[1].values()[0] %}
              {{ log("📊 Processed: " ~ size_mb ~ " MB (" ~ day_count ~ " days)", info=True) }}
            {% endif %}
          {% endif %}
        {% endif %}
      {% else %}
        {# Full refresh (v1, v2) - get all data size #}
        {% set size_query %}
          SELECT 
            SUM(input_file_length) / 1024.0 / 1024.0 AS size_mb
          FROM (
            SELECT DISTINCT input_file_length
            FROM {{ ref('stg_trips') }}
          )
        {% endset %}
        
        {% set results = run_query(size_query) %}
        {% if results and results.columns[0].values() %}
          {% set size_mb = results.columns[0].values()[0] | round(2) %}
          {{ log("📊 Processed: " ~ size_mb ~ " MB (full)", info=True) }}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}

