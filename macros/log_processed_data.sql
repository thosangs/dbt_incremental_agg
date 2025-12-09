{# Helper macro: Check if a table exists #}
{% macro check_table_exists(model) %}
  {% set table_exists_query %}
    SELECT COUNT(*) > 0 AS table_exists
    FROM information_schema.tables
    WHERE table_schema = '{{ model.schema }}'
      AND table_name = '{{ model.identifier }}'
  {% endset %}
  
  {% set exists_result = run_query(table_exists_query) %}
  {% if exists_result and exists_result.columns[0].values() %}
    {{ return(exists_result.columns[0].values()[0]) }}
  {% else %}
    {{ return(false) }}
  {% endif %}
{% endmacro %}


{# Helper macro: Get max date from a model #}
{% macro get_max_date(model, date_column) %}
  {% set max_date_query %}
    SELECT MAX({{ date_column }}) AS max_date FROM {{ model }}
  {% endset %}
  
  {% set max_date_result = run_query(max_date_query) %}
  {% if max_date_result and max_date_result.columns[0].values() %}
    {{ return(max_date_result.columns[0].values()[0]) }}
  {% else %}
    {{ return(none) }}
  {% endif %}
{% endmacro %}


{# Helper macro: Calculate reprocess_from date #}
{% macro calculate_reprocess_from(max_date, window_days) %}
  {% if max_date and max_date >= '2000-01-01' %}
    {% set reprocess_query %}
      SELECT DATE '{{ max_date }}' - INTERVAL {{ window_days }} DAYS AS reprocess_from
    {% endset %}
    
    {% set date_result = run_query(reprocess_query) %}
    {% if date_result and date_result.columns[0].values() %}
      {{ return(date_result.columns[0].values()[0]) }}
    {% endif %}
  {% endif %}
  {{ return(none) }}
{% endmacro %}


{# Helper macro: Count and log rows from Parquet files #}
{% macro count_and_log_parquet(reprocess_from=none, run_type='full') %}
  {% if reprocess_from %}
    {% set count_query %}
      SELECT 
        COUNT(*) AS row_count,
        COUNT(DISTINCT DATE(order_timestamp)) AS day_count
      FROM read_parquet('/data/partitioned/**/*.parquet')
      WHERE order_timestamp IS NOT NULL
        AND revenue IS NOT NULL
        AND revenue > 0
        AND DATE(order_timestamp) >= DATE '{{ reprocess_from }}'
    {% endset %}
    
    {% set results = run_query(count_query) %}
    {% if results and results.columns[0].values() %}
      {% set row_count = results.columns[0].values()[0] %}
      {% set day_count = none %}
      {% if results.columns|length > 1 and results.columns[1].values() %}
        {% set day_count = results.columns[1].values()[0] %}
      {% endif %}
      {{ log("📊 Will process: " ~ row_count ~ " rows (" ~ day_count ~ " days) from Parquet files", info=True) }}
    {% endif %}
  {% else %}
    {% set count_query %}
      SELECT COUNT(*) AS row_count
      FROM read_parquet('/data/partitioned/**/*.parquet')
      WHERE order_timestamp IS NOT NULL
        AND revenue IS NOT NULL
        AND revenue > 0
    {% endset %}
    
    {% set results = run_query(count_query) %}
    {% if results and results.columns[0].values() %}
      {% set row_count = results.columns[0].values()[0] %}
      {{ log("📊 Will process: " ~ row_count ~ " rows from Parquet files (" ~ run_type ~ ")", info=True) }}
    {% endif %}
  {% endif %}
{% endmacro %}


{# Helper macro: Count and log rows from staging model #}
{% macro count_and_log_staging(staging_model, reprocess_from=none, run_type='full') %}
  {% if reprocess_from %}
    {% set count_query %}
      SELECT 
        COUNT(*) AS row_count,
        COUNT(DISTINCT order_date) AS day_count
      FROM {{ ref(staging_model) }}
      WHERE order_date >= DATE '{{ reprocess_from }}'
    {% endset %}
    
    {% set results = run_query(count_query) %}
    {% if results and results.columns[0].values() %}
      {% set row_count = results.columns[0].values()[0] %}
      {% set day_count = none %}
      {% if results.columns|length > 1 and results.columns[1].values() %}
        {% set day_count = results.columns[1].values()[0] %}
      {% endif %}
      {{ log("📊 Will process: " ~ row_count ~ " rows (" ~ day_count ~ " days) from staging model", info=True) }}
    {% endif %}
  {% else %}
    {% set count_query %}
      SELECT COUNT(*) AS row_count FROM {{ ref(staging_model) }}
    {% endset %}
    
    {% set results = run_query(count_query) %}
    {% if results and results.columns[0].values() %}
      {% set row_count = results.columns[0].values()[0] %}
      {{ log("📊 Will process: " ~ row_count ~ " rows from staging model (" ~ run_type ~ ")", info=True) }}
    {% endif %}
  {% endif %}
{% endmacro %}


{# Helper macro: Determine which staging model to use #}
{% macro get_staging_model(model_name) %}
  {% if 'v1' in model_name %}
    {{ return('stg_orders_v1') }}
  {% elif 'v2' in model_name %}
    {{ return('stg_orders_v2') }}
  {% elif 'v3' in model_name %}
    {{ return('stg_orders_v2') }}
  {% else %}
    {{ return('stg_orders_v1') }}
  {% endif %}
{% endmacro %}


{# Main macro: Log processed data for staging and aggregation models #}
{% macro log_processed_data(model) %}
  {% if execute %}
    {% set model_name = model.identifier %}
    
    {# Find the model node and process all logic inside the loop to avoid scoping issues #}
    {% for node in graph.nodes.values() %}
      {% if node.name == model.identifier %}
        {% set materialization = node.config.materialized %}
        {% set is_incremental = (materialization == 'incremental') %}
        
        {# For staging models #}
        {% if 'stg_orders' in model_name %}
          {% if is_incremental %}
            {% set table_exists = check_table_exists(model) %}
            
            {% if not table_exists %}
              {# Initial run - count all rows from Parquet files #}
              {{ count_and_log_parquet(run_type='full') }}
            {% else %}
              {# Incremental run - count rows in reprocess window (7 days) #}
              {% set max_date = get_max_date(model, 'order_date') %}
              {% set reprocess_from = calculate_reprocess_from(max_date, 7) %}
              
              {% if reprocess_from %}
                {{ count_and_log_parquet(reprocess_from) }}
              {% endif %}
            {% endif %}
          {% else %}
            {# Full refresh staging model - count all rows from Parquet files #}
            {{ count_and_log_parquet(run_type='full') }}
          {% endif %}
        {% endif %}
        
        {# For aggregation models #}
        {% if 'agg_daily_revenue' in model_name %}
          {% set staging_model = get_staging_model(model_name) %}
          
          {% if is_incremental %}
            {% set table_exists = check_table_exists(model) %}
            
            {% if not table_exists %}
              {# Initial run - count all rows from staging model #}
              {{ count_and_log_staging(staging_model, run_type='full') }}
            {% else %}
              {# Incremental run - count rows in reprocess window (14 days) #}
              {% set max_date = get_max_date(model, 'order_date') %}
              {% set reprocess_window = var('reprocess_window_days', 14) %}
              {% set reprocess_from = calculate_reprocess_from(max_date, reprocess_window) %}
              
              {% if reprocess_from %}
                {{ count_and_log_staging(staging_model, reprocess_from) }}
              {% endif %}
            {% endif %}
          {% else %}
            {# Full refresh - count rows from staging model #}
            {{ count_and_log_staging(staging_model, run_type='full') }}
          {% endif %}
        {% endif %}
        
        {% break %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
