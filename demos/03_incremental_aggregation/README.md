# Demo 03: Incremental Aggregation with Partition Overwrite

## Overview

This demo demonstrates **incremental aggregation with partition overwrite**, the most sophisticated pattern. It uses Spark's `insert_overwrite` strategy to efficiently handle late-arriving events by reprocessing only affected date partitions.

## When to Use Incremental Aggregation

- **Time-series aggregations** (daily, hourly, etc.)
- **Late-arriving events** are common and need to be handled
- **Large datasets** where full refresh is too expensive
- **Balance between freshness and compute cost** is important

## How It Works

1. **Reprocessing Window**: On each incremental run, reprocess a rolling window (default: last 14 days) to catch late-arriving events
2. **Partition Overwrite**: Spark's `insert_overwrite` strategy efficiently overwrites only the affected partitions
3. **Preserve Stable Data**: Older partitions (outside the window) are preserved, avoiding unnecessary recomputation

## Trade-offs

✅ **Pros:**

- Handles late-arriving events correctly
- Efficient - only reprocesses recent partitions
- Scales well for large datasets
- Uses Spark's native partition overwrite (unlike DuckDB)

❌ **Cons:**

- More complex logic
- Requires partitioning by date
- Window function for running totals needs full union (can't be purely incremental)

## Models

- `stg_trips.sql`: View (always fresh, reads from parquet files, no incremental logic here)
- `agg_daily_revenue.sql`: Incremental aggregation with `insert_overwrite` and partition overwrite

## Running This Demo

```bash
# Download NYC taxi data first (if not already downloaded)
make download-data

# Copy models to main models directory
cp demos/03_incremental_aggregation/models/stg_trips.sql models/staging/stg_trips.sql
cp demos/03_incremental_aggregation/models/agg_daily_revenue.sql models/metrics/agg_daily_revenue.sql

# First run - processes all trips from parquet files
make run

# Download older months to simulate late-arriving trips (with old timestamps)
make demo-late-data
make run

# Only affected date partitions will be reprocessed
```

## Expected Behavior

- First run: Processes all trips from downloaded parquet files and creates initial aggregation
- Subsequent runs: Only reprocesses trips within the reprocessing window (last 14 days by default)
- Late-arriving trips: Will update the correct historical date partition when older months are downloaded
- Older partitions: Preserved and not recomputed (outside the reprocessing window)

## Configuration

The reprocessing window is controlled by the `reprocess_window_days` variable in `dbt_project.yml` (default: 14 days). Adjust this based on your late-arriving trip patterns.

## NYC Taxi Data Notes

NYC taxi data is published monthly with a ~2-month delay, making it perfect for demonstrating late-arriving data patterns. When you download older months (e.g., 2021 Q1) after processing 2022-2023 data, those trips will have old timestamps and will trigger reprocessing of the affected date partitions.

## Spark-Specific Features

This pattern leverages Spark's `insert_overwrite` incremental strategy, which:

- Supports partition-level overwrites (unlike DuckDB)
- Efficiently updates only affected partitions
- Preserves partition metadata and structure
