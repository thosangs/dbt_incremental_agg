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
    
    {# For staging models - log row count #}
    {% if 'stg_trips' in model_name %}
      {% set count_query %}
        SELECT COUNT(*) AS row_count
        FROM {{ model }}
      {% endset %}
      
      {% set results = run_query(count_query) %}
      {% if results and results.columns[0].values() %}
        {% set row_count = results.columns[0].values()[0] %}
        {{ log("📊 Processed: " ~ row_count ~ " rows", info=True) }}
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
            {% set count_query %}
              SELECT COUNT(*) AS row_count
              FROM {{ ref('stg_trips_v2') }}
            {% endset %}
            
            {% set results = run_query(count_query) %}
            {% if results and results.columns[0].values() %}
              {% set row_count = results.columns[0].values()[0] %}
              {{ log("📊 Processed: " ~ row_count ~ " rows (initial)", info=True) }}
            {% endif %}
          {% else %}
            {# Incremental run - calculate reprocess window #}
            {% set count_query %}
              SELECT 
                COUNT(*) AS row_count,
                COUNT(DISTINCT trip_date) AS day_count
              FROM {{ ref('stg_trips_v2') }}
              WHERE trip_date >= DATE '{{ reprocess_from }}'
            {% endset %}
            
            {% set results = run_query(count_query) %}
            {% if results and results.columns[0].values() %}
              {% set row_count = results.columns[0].values()[0] %}
              {% set day_count = results.columns[1].values()[0] %}
              {{ log("📊 Processed: " ~ row_count ~ " rows (" ~ day_count ~ " days)", info=True) }}
            {% endif %}
          {% endif %}
        {% endif %}
      {% else %}
        {# Full refresh (v1, v2) - get row count #}
        {% set count_query %}
          SELECT COUNT(*) AS row_count
          FROM {{ ref('stg_trips_v2') }}
        {% endset %}
        
        {% set results = run_query(count_query) %}
        {% if results and results.columns[0].values() %}
          {% set row_count = results.columns[0].values()[0] %}
          {{ log("📊 Processed: " ~ row_count ~ " rows (full)", info=True) }}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}
