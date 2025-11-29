# Demo 02: Incremental Event Processing

## Overview

This demo demonstrates **incremental event processing**. New events are added to the staging table incrementally, avoiding reprocessing of existing events.

## When to Use Incremental Events

- **Event streams** with continuous new data
- **Large event volumes** where full refresh is too expensive
- **Append-only** event patterns (events don't change after creation)
- **Real-time or near-real-time** processing requirements

## Trade-offs

✅ **Pros:**
- Efficient for large event volumes
- Fast incremental runs (only processes new events)
- Handles continuous data streams well

❌ **Cons:**
- Doesn't handle late-arriving events well (events with old timestamps)
- Requires unique key (event_id) to deduplicate
- Aggregation still needs full refresh (unless you make it incremental too)

## Models

- `stg_trips.sql`: Incremental staging table (only new trips)
- `agg_daily_revenue.sql`: Full refresh aggregation (reads from incremental staging)

## Running This Demo

```bash
# Download NYC taxi data first (if not already downloaded)
make download-data

# Copy models to main models directory
cp demos/02_incremental_events/models/stg_trips.sql models/staging/stg_trips.sql
cp demos/02_incremental_events/models/agg_daily_revenue.sql models/metrics/agg_daily_revenue.sql

# First run - processes all trips from parquet files
make run

# Download additional older months to simulate new data, then run again
make demo-late-data
make run

# Only new trips (with trip_id not already in table) will be processed
```

## Expected Behavior

- First run: Processes all trips from downloaded parquet files
- Subsequent runs: Only processes trips with `trip_id` not already in the table
- Late-arriving trips: Will be inserted if they have new trip_ids, but won't update historical aggregations if they have old timestamps

## Handling Late-Arriving Events

This pattern doesn't handle late-arriving events well. If a trip arrives with an old timestamp but a new trip_id, it will be inserted but won't update historical aggregations. See Demo 03 for a better approach that handles late-arriving data correctly.

