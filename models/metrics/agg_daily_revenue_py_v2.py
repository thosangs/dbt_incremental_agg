import pandas as pd
import pyarrow as pa


def model(dbt, session):
    """
    Version 2: Incremental Event Processing (Python/PyArrow)

    This aggregation reads from the incremental staging table (stg_orders_v2).
    Uses incremental materialization with delete+insert strategy to update
    only affected date ranges.

    Demonstrates PyArrow batch processing for efficient incremental aggregation.
    """

    # Get from_date from dbt variables
    from_date_str = dbt.config.get("from_date")

    dbt.config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key="order_date",
        on_schema_change="append_new_columns",
    )

    # ============================================================================
    # PyArrow UDF - Batch processing for aggregation
    # ============================================================================
    def aggregate_daily_revenue_pyarrow(batch_reader: pa.RecordBatchReader):
        """
        PyArrow UDF: Process data in batches and aggregate daily revenue.
        This function processes RecordBatches and aggregates by date.
        """
        # Collect all DataFrames for aggregation (since we need to aggregate by date)
        all_dfs = []
        for batch in batch_reader:
            # Convert batch to pandas DataFrame
            df = batch.to_pandas()
            all_dfs.append(df)

        # If we have DataFrames, combine and aggregate
        if all_dfs:
            # Combine all DataFrames into a single DataFrame for aggregation
            combined_df = pd.concat(all_dfs, ignore_index=True)

            # Aggregate daily revenue
            daily_agg = (
                combined_df.groupby("order_date")
                .agg(
                    {
                        "revenue": "sum",
                        "order_id": "nunique",
                    }
                )
                .reset_index()
            )
            daily_agg.columns = [
                "order_date",
                "daily_revenue",
                "daily_orders",
            ]

            # Sort by order_date
            daily_agg = daily_agg.sort_values("order_date")

            # Select final columns (no running revenue)
            final_df = daily_agg[
                [
                    "order_date",
                    "daily_revenue",
                    "daily_orders",
                ]
            ].copy()

            # Convert to RecordBatch with explicit schema
            schema = pa.schema(
                [
                    pa.field("order_date", pa.date32()),
                    pa.field("daily_revenue", pa.float64()),
                    pa.field("daily_orders", pa.int64()),
                ]
            )

            # Convert to RecordBatch with explicit schema
            table = pa.Table.from_pandas(final_df, schema=schema)
            yield table.to_batches()[0]

    # ============================================================================
    # Main processing logic using PyArrow batch processing
    # ============================================================================
    # Get upstream model as RecordBatchReader
    orders_relation = dbt.ref("stg_orders_v2")
    batch_reader = orders_relation.record_batch(
        100_000
    )  # Process in batches of 100k rows

    if dbt.is_incremental and from_date_str:
        # Incremental run: filter by from_date
        from_date = pd.to_datetime(from_date_str).date()
        
        # Filter batches by from_date
        filtered_dfs = []
        for batch in batch_reader:
            df = batch.to_pandas()
            # Filter to only include dates >= from_date
            df = df[df["order_date"] >= pd.Timestamp(from_date)]
            if len(df) > 0:
                filtered_dfs.append(df)
        
        if filtered_dfs:
            # Create a new batch reader from filtered data
            combined_df = pd.concat(filtered_dfs, ignore_index=True)
            # Aggregate
            daily_agg = (
                combined_df.groupby("order_date")
                .agg(
                    {
                        "revenue": "sum",
                        "order_id": "nunique",
                    }
                )
                .reset_index()
            )
            daily_agg.columns = [
                "order_date",
                "daily_revenue",
                "daily_orders",
            ]
            daily_agg = daily_agg.sort_values("order_date")
            
            schema = pa.schema(
                [
                    pa.field("order_date", pa.date32()),
                    pa.field("daily_revenue", pa.float64()),
                    pa.field("daily_orders", pa.int64()),
                ]
            )
            table = pa.Table.from_pandas(daily_agg, schema=schema)
            batch_reader = pa.RecordBatchReader.from_batches(schema, table.to_batches())
        else:
            # Empty result
            schema = pa.schema(
                [
                    pa.field("order_date", pa.date32()),
                    pa.field("daily_revenue", pa.float64()),
                    pa.field("daily_orders", pa.int64()),
                ]
            )
            empty_df = pd.DataFrame(columns=["order_date", "daily_revenue", "daily_orders"])
            table = pa.Table.from_pandas(empty_df, schema=schema)
            batch_reader = pa.RecordBatchReader.from_batches(schema, table.to_batches())
    else:
        # Full refresh: process all data
        batch_iter = aggregate_daily_revenue_pyarrow(batch_reader)
        
        # Create RecordBatchReader from processed batches
        # We need to get the schema from the first batch
        first_batch = next(batch_iter, None)
        if first_batch:
            schema = first_batch.schema
            
            # Create a new reader with the first batch and remaining batches
            def batch_generator():
                yield first_batch
                yield from batch_iter
            
            batch_reader = pa.RecordBatchReader.from_batches(schema, batch_generator())
        else:
            # Empty result
            schema = pa.schema(
                [
                    pa.field("order_date", pa.date32()),
                    pa.field("daily_revenue", pa.float64()),
                    pa.field("daily_orders", pa.int64()),
                ]
            )
            empty_df = pd.DataFrame(columns=["order_date", "daily_revenue", "daily_orders"])
            table = pa.Table.from_pandas(empty_df, schema=schema)
            batch_reader = pa.RecordBatchReader.from_batches(schema, table.to_batches())

    return batch_reader
