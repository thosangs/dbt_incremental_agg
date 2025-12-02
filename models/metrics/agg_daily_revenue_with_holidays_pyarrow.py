from datetime import date

import pandas as pd
import pyarrow as pa
from holidays import US


def model(dbt, session):
    """
    Python model demonstrating PyArrow UDFs with DuckDB.
    This model enriches daily revenue aggregation with US holiday information.

    Demonstrates Type 2: PyArrow UDF
    - Uses PyArrow RecordBatchReader for batch processing
    - Memory-efficient for large datasets
    - Processes data in chunks (batches)

    Uses:
    - Holidays package to identify US holidays
    - PyArrow for efficient batch processing
    - Pandas UDFs applied within batches
    - References upstream SQL model (stg_trips_v1)
    """

    dbt.config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="trip_date",
        on_schema_change="append_new_columns",
    )

    # Create US holidays dictionary for a reasonable date range
    # NYC taxi data typically spans 2020-2025, so we'll cover that range
    years = range(2020, 2026)
    us_holidays = US(years=years)
    holiday_dates_set = set(us_holidays.keys())
    holiday_names_dict = us_holidays.copy()

    # ============================================================================
    # Pandas UDF functions (used within PyArrow batches)
    # ============================================================================
    def is_holiday_pandas(date_val):
        """
        Pandas UDF: Check if a date is a US holiday.
        This function is applied row-by-row within each batch.
        """
        if pd.isna(date_val) or date_val is None:
            return False
        try:
            if isinstance(date_val, date):
                return date_val in holiday_dates_set
            if isinstance(date_val, str):
                date_obj = pd.to_datetime(date_val).date()
                return date_obj in holiday_dates_set
            # Handle pandas Timestamp
            if hasattr(date_val, "date"):
                return date_val.date() in holiday_dates_set
            return False
        except Exception:
            return False

    def get_holiday_name_pandas(date_val):
        """
        Pandas UDF: Get the name of the holiday if it's a holiday.
        This function is applied row-by-row within each batch.
        """
        if pd.isna(date_val) or date_val is None:
            return None
        try:
            if isinstance(date_val, date):
                return holiday_names_dict.get(date_val)
            if isinstance(date_val, str):
                date_obj = pd.to_datetime(date_val).date()
                return holiday_names_dict.get(date_obj)
            # Handle pandas Timestamp
            if hasattr(date_val, "date"):
                return holiday_names_dict.get(date_val.date())
            return None
        except Exception:
            return None

    # ============================================================================
    # PyArrow UDF - Batch processing using RecordBatchReader
    # ============================================================================
    def enrich_with_holidays_pyarrow(batch_reader: pa.RecordBatchReader):
        """
        PyArrow UDF: Process data in batches for efficient memory usage.
        This function processes RecordBatches, applies UDFs per batch,
        and accumulates results for final aggregation.

        Note: For aggregations, we collect all batches first, then aggregate.
        For transformations, we can process and yield batches directly.
        """
        # Collect all DataFrames for aggregation (since we need to aggregate by date)
        # Work with pandas DataFrames to avoid schema mismatches
        all_dfs = []
        for batch in batch_reader:
            # Convert batch to pandas DataFrame
            df = batch.to_pandas()

            # Apply Pandas UDFs within the batch: Process each row in the batch
            # This demonstrates PyArrow batch processing with row-level UDFs
            df["is_holiday"] = df["trip_date"].apply(is_holiday_pandas)
            df["holiday_name"] = df["trip_date"].apply(get_holiday_name_pandas)

            # Ensure holiday_name is object type (string) to handle None values consistently
            df["holiday_name"] = df["holiday_name"].astype("object")

            # Collect DataFrame (not RecordBatch) to avoid schema issues
            all_dfs.append(df)

        # If we have DataFrames, combine and aggregate
        if all_dfs:
            # Combine all DataFrames into a single DataFrame for aggregation
            combined_df = pd.concat(all_dfs, ignore_index=True)

            # Aggregate daily revenue (now we have all data)
            daily_agg = (
                combined_df.groupby("trip_date")
                .agg(
                    {
                        "total_amount": "sum",
                        "trip_id": "count",
                        "passenger_count": "sum",
                        "is_holiday": "first",  # Take first value (all same for a date)
                        "holiday_name": "first",
                    }
                )
                .reset_index()
            )
            daily_agg.columns = [
                "trip_date",
                "daily_revenue",
                "daily_trips",
                "daily_passengers",
                "is_holiday",
                "holiday_name",
            ]

            # Calculate running revenue
            daily_agg = daily_agg.sort_values("trip_date")
            daily_agg["running_revenue"] = daily_agg["daily_revenue"].cumsum()

            # Select final columns
            final_df = daily_agg[
                [
                    "trip_date",
                    "is_holiday",
                    "holiday_name",
                    "daily_revenue",
                    "daily_trips",
                    "daily_passengers",
                    "running_revenue",
                ]
            ].copy()

            # Ensure consistent data types
            final_df["holiday_name"] = final_df["holiday_name"].astype("object")

            # Convert to RecordBatch with explicit schema to avoid mismatches
            # Define schema explicitly to ensure consistency
            schema = pa.schema(
                [
                    pa.field("trip_date", pa.date32()),
                    pa.field("is_holiday", pa.bool_()),
                    pa.field("holiday_name", pa.string()),  # Explicitly nullable string
                    pa.field("daily_revenue", pa.float64()),
                    pa.field("daily_trips", pa.int64()),
                    pa.field("daily_passengers", pa.float64()),
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
    trips_relation = dbt.ref("stg_trips_v1")
    batch_reader = trips_relation.record_batch(
        100_000
    )  # Process in batches of 100k rows

    # Process batches through PyArrow UDF
    batch_iter = enrich_with_holidays_pyarrow(batch_reader)

    # Create RecordBatchReader from processed batches
    # We need to get the schema from the first batch
    first_batch = next(batch_iter)
    schema = first_batch.schema

    # Create a new reader with the first batch and remaining batches
    def batch_generator():
        yield first_batch
        yield from batch_iter

    return pa.RecordBatchReader.from_batches(schema, batch_generator())
