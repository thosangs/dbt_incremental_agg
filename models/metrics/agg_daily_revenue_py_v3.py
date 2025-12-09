import pandas as pd
import pyarrow as pa


def model(dbt, session):
    """
    Version 3: Incremental Aggregation with Sliding Window (Python/PyArrow)

    This model demonstrates incremental aggregation with a sliding window,
    which efficiently updates only affected date ranges while preserving
    older, stable data.

    Strategy:
    1. Reprocess a rolling window (e.g., last 14 days) to catch late-arriving events
    2. Preserve older data that is unlikely to change
    3. Use MERGE to update/insert records in the sliding window
    4. Calculate running revenue over the complete dataset

    Use case: Aggregations where late-arriving events need to be handled,
    and you want to balance freshness with compute efficiency.

    Demonstrates PyArrow batch processing with incremental logic.
    """

    # Get from_date from dbt variables
    from_date_str = dbt.config.get("from_date")

    # Parse date from string format 'YYYY-MM-DD'
    from_date = pd.to_datetime(from_date_str).date()

    dbt.config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="order_date",
        on_schema_change="append_new_columns",
    )

    # ============================================================================
    # PyArrow UDF - Batch processing for incremental aggregation
    # ============================================================================
    def aggregate_daily_revenue_incremental_pyarrow(
        batch_reader: pa.RecordBatchReader, reprocess_from_date: pd.Timestamp
    ):
        """
        PyArrow UDF: Process data in batches and aggregate daily revenue
        for the sliding window (reprocess_from_date onwards).
        """
        # Collect all DataFrames for aggregation
        all_dfs = []
        for batch in batch_reader:
            # Convert batch to pandas DataFrame
            df = batch.to_pandas()

            # Filter to only include dates in the reprocess window
            df = df[df["order_date"] >= pd.Timestamp(reprocess_from_date)]

            if len(df) > 0:
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
                        "buyer_id": "nunique",
                    }
                )
                .reset_index()
            )
            daily_agg.columns = [
                "order_date",
                "daily_revenue",
                "daily_orders",
                "daily_buyers",
            ]

            # Convert to RecordBatch with explicit schema
            schema = pa.schema(
                [
                    pa.field("order_date", pa.date32()),
                    pa.field("daily_revenue", pa.float64()),
                    pa.field("daily_orders", pa.int64()),
                    pa.field("daily_buyers", pa.int64()),
                ]
            )

            # Convert to RecordBatch with explicit schema
            table = pa.Table.from_pandas(daily_agg, schema=schema)
            yield table.to_batches()[0]

    # ============================================================================
    # Main processing logic
    # ============================================================================
    orders_relation = dbt.ref("stg_orders_v2")
    batch_reader = orders_relation.record_batch(100_000)

    if dbt.is_incremental:
        # Incremental run: reprocess sliding window using from_date variable
        existing_table = dbt.this

        # Use from_date from dbt variables
        reprocess_from = pd.Timestamp(from_date)

        # Get new aggregates for sliding window
        new_agg_iter = aggregate_daily_revenue_incremental_pyarrow(
            batch_reader, reprocess_from
        )

        # Get new aggregates DataFrame
        new_agg_batch = next(new_agg_iter, None)
        if new_agg_batch:
            new_agg_df = new_agg_batch.to_pandas()
            # Set running_revenue = daily_revenue for new aggregates (will be recalculated)
            new_agg_df["running_revenue"] = new_agg_df["daily_revenue"]
        else:
            new_agg_df = pd.DataFrame(
                columns=[
                    "order_date",
                    "daily_revenue",
                    "daily_orders",
                    "daily_buyers",
                    "running_revenue",
                ]
            )

        # Get existing data for the day BEFORE from_date (to get previous running_revenue)
        from_date_ts = pd.Timestamp(from_date)
        day_before_from_date = from_date_ts - pd.Timedelta(days=1)

        try:
            existing_df = existing_table.to_df()
            if not existing_df.empty:
                # Filter to only the day before from_date
                existing_df = existing_df[
                    pd.to_datetime(existing_df["order_date"]).dt.date
                    == day_before_from_date.date()
                ]
                existing_df = existing_df[
                    [
                        "order_date",
                        "daily_revenue",
                        "daily_orders",
                        "daily_buyers",
                        "running_revenue",
                    ]
                ]
            else:
                existing_df = pd.DataFrame(
                    columns=[
                        "order_date",
                        "daily_revenue",
                        "daily_orders",
                        "daily_buyers",
                        "running_revenue",
                    ]
                )
        except Exception:
            # Table doesn't exist or error reading it
            existing_df = pd.DataFrame(
                columns=[
                    "order_date",
                    "daily_revenue",
                    "daily_orders",
                    "daily_buyers",
                    "running_revenue",
                ]
            )

        # Combine new aggregates and existing data
        if not existing_df.empty:
            combined_df = pd.concat([new_agg_df, existing_df], ignore_index=True)
        else:
            combined_df = new_agg_df

        # Recalculate running_revenue using window function (matching SQL pattern)
        combined_df = combined_df.sort_values("order_date")
        combined_df["running_revenue"] = combined_df["running_revenue"].cumsum()

        # Select final columns
        final_df = combined_df[
            [
                "order_date",
                "daily_revenue",
                "daily_orders",
                "daily_buyers",
                "running_revenue",
            ]
        ].copy()

    else:
        # Full refresh: process all data
        all_dfs = []
        for batch in batch_reader:
            df = batch.to_pandas()
            all_dfs.append(df)

        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)

            # Aggregate daily revenue
            daily_agg = (
                combined_df.groupby("order_date")
                .agg(
                    {
                        "revenue": "sum",
                        "order_id": "nunique",
                        "buyer_id": "nunique",
                    }
                )
                .reset_index()
            )
            daily_agg.columns = [
                "order_date",
                "daily_revenue",
                "daily_orders",
                "daily_buyers",
            ]

            # Calculate running revenue
            daily_agg = daily_agg.sort_values("order_date")
            daily_agg["running_revenue"] = daily_agg["daily_revenue"].cumsum()

            final_df = daily_agg[
                [
                    "order_date",
                    "daily_revenue",
                    "daily_orders",
                    "daily_buyers",
                    "running_revenue",
                ]
            ].copy()
        else:
            # Empty result
            final_df = pd.DataFrame(
                columns=[
                    "order_date",
                    "daily_revenue",
                    "daily_orders",
                    "daily_buyers",
                    "running_revenue",
                ]
            )

    # Convert to RecordBatch with explicit schema
    schema = pa.schema(
        [
            pa.field("order_date", pa.date32()),
            pa.field("daily_revenue", pa.float64()),
            pa.field("daily_orders", pa.int64()),
            pa.field("daily_buyers", pa.int64()),
            pa.field("running_revenue", pa.float64()),
        ]
    )

    table = pa.Table.from_pandas(final_df, schema=schema)
    batch_reader = pa.RecordBatchReader.from_batches(schema, table.to_batches())

    return batch_reader
