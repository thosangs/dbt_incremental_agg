import pandas as pd
import pyarrow as pa


def model(dbt, session):
    """
    Version 2: Incremental Event Processing (Python/PyArrow)

    This aggregation reads from the incremental staging table (stg_orders_v2).
    Since stg_orders_v2 is incremental, we can rebuild the aggregation
    from the full staging table each time (which is now efficient
    because staging only contains new orders on each run).

    This is a hybrid approach - incremental staging, full aggregation.

    Demonstrates PyArrow batch processing for efficient aggregation.
    """

    dbt.config(
        materialized="table",
    )

    # ============================================================================
    # PyArrow UDF - Batch processing for aggregation
    # ============================================================================
    def aggregate_daily_revenue_pyarrow(batch_reader: pa.RecordBatchReader):
        """
        PyArrow UDF: Process data in batches and aggregate daily revenue.
        This function processes RecordBatches, aggregates by date,
        and calculates running revenue.
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

            # Calculate running revenue
            daily_agg = daily_agg.sort_values("order_date")
            daily_agg["running_revenue"] = daily_agg["daily_revenue"].cumsum()

            # Select final columns
            final_df = daily_agg[
                [
                    "order_date",
                    "daily_revenue",
                    "daily_orders",
                    "running_revenue",
                ]
            ].copy()

            # Convert to RecordBatch with explicit schema
            schema = pa.schema(
                [
                    pa.field("order_date", pa.date32()),
                    pa.field("daily_revenue", pa.float64()),
                    pa.field("daily_orders", pa.int64()),
                    pa.field("running_revenue", pa.float64()),
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

    # Process batches through PyArrow UDF
    batch_iter = aggregate_daily_revenue_pyarrow(batch_reader)

    # Create RecordBatchReader from processed batches
    # We need to get the schema from the first batch
    first_batch = next(batch_iter)
    schema = first_batch.schema

    # Create a new reader with the first batch and remaining batches
    def batch_generator():
        yield first_batch
        yield from batch_iter

    return pa.RecordBatchReader.from_batches(schema, batch_generator())
