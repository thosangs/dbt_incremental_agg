from datetime import date, timedelta

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

    reprocess_window_days = dbt.config.get("reprocess_window_days", 14)

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
        batch_reader: pa.RecordBatchReader, reprocess_from_date: date
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

    if dbt.is_incremental():
        # Incremental run: reprocess sliding window
        # Get max date from existing table
        existing_table = dbt.this()

        # Query max date from existing table
        try:
            max_date_df = existing_table.to_df(columns=["order_date"])
            if not max_date_df.empty:
                max_date = max_date_df["order_date"].max()
                if isinstance(max_date, str):
                    max_date = pd.to_datetime(max_date).date()
                elif hasattr(max_date, "date"):
                    max_date = max_date.date()
                elif pd.isna(max_date):
                    max_date = None
            else:
                max_date = None
        except Exception:
            # Table might not exist yet (first incremental run)
            max_date = None

        if max_date:
            # Calculate reprocess_from date
            reprocess_from = max_date - timedelta(days=reprocess_window_days)
        else:
            # No existing data, process everything
            reprocess_from = date(1900, 1, 1)

        # Get new aggregates for sliding window
        new_agg_iter = aggregate_daily_revenue_incremental_pyarrow(
            batch_reader, reprocess_from
        )

        # Get existing data (older than reprocess window)
        try:
            existing_df = existing_table.to_df()
            if not existing_df.empty:
                # Filter to dates older than reprocess window
                existing_df = existing_df[
                    pd.to_datetime(existing_df["order_date"]).dt.date < reprocess_from
                ]
                existing_df = existing_df[
                    ["order_date", "daily_revenue", "daily_orders", "daily_buyers"]
                ].sort_values("order_date")
            else:
                existing_df = pd.DataFrame(
                    columns=[
                        "order_date",
                        "daily_revenue",
                        "daily_orders",
                        "daily_buyers",
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
                ]
            )

        # Combine new aggregates and existing data
        new_agg_batch = next(new_agg_iter, None)
        if new_agg_batch:
            new_agg_df = new_agg_batch.to_pandas()
        else:
            # No new data, just return existing
            new_agg_df = pd.DataFrame(
                columns=[
                    "order_date",
                    "daily_revenue",
                    "daily_orders",
                    "daily_buyers",
                ]
            )

        # Combine DataFrames
        if not existing_df.empty:
            combined_df = pd.concat([new_agg_df, existing_df], ignore_index=True)
        else:
            combined_df = new_agg_df

        # Calculate running revenue over combined dataset
        combined_df = combined_df.sort_values("order_date")
        combined_df["running_revenue"] = combined_df["daily_revenue"].cumsum()

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
