# Demo 01: Full Batch Processing

## Overview

This demo demonstrates the simplest data processing pattern: **full batch refresh**. Every time dbt runs, the entire table is rebuilt from scratch.

## When to Use Full Batch

- **Small datasets** (< 1GB)
- **Infrequent updates** (daily or less)
- **Simple transformations** that don't require complex incremental logic
- **Development/testing** environments where simplicity matters more than efficiency

## Trade-offs

✅ **Pros:**
- Simple to understand and maintain
- No need to track state or handle late-arriving data
- Guaranteed consistency (no partial updates)

❌ **Cons:**
- Inefficient for large datasets
- Slow for frequent updates
- Wastes compute resources on unchanged data

## Models

- `stg_trips.sql`: Full refresh staging table (reads from NYC taxi parquet files)
- `agg_daily_revenue.sql`: Full refresh aggregation table

## Running This Demo

```bash
# Download NYC taxi data first (if not already downloaded)
make download-data

# Copy models to main models directory
cp demos/01_full_batch/models/stg_trips.sql models/staging/stg_trips.sql
cp demos/01_full_batch/models/agg_daily_revenue.sql models/metrics/agg_daily_revenue.sql

# Run dbt
make run

# Or run specific models
docker compose exec -T dbt dbt --profiles-dir profiles run --select stg_trips agg_daily_revenue
```

## Expected Behavior

- First run: Creates tables from scratch, reading all NYC taxi parquet files
- Subsequent runs: Drops and recreates entire tables
- No incremental logic - always full refresh
- Processes all trips from all downloaded parquet files each time

