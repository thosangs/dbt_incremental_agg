from datetime import date

import pandas as pd
from holidays import US


def model(dbt, session):
    """
    Python model demonstrating Pandas UDFs with DuckDB.
    This model enriches daily revenue aggregation with US holiday information.

    Demonstrates Type 1: Pandas UDF
    - Uses pandas .apply() for row-level transformations
    - Simple and straightforward for smaller datasets
    - Good for row-by-row operations

    Uses:
    - Holidays package to identify US holidays
    - Pandas DataFrames for data manipulation
    - Pandas .apply() for UDF transformations
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
    # Pandas UDF - Row-level transformations using .apply()
    # ============================================================================
    def is_holiday_pandas(date_val):
        """
        Pandas UDF: Check if a date is a US holiday.
        This function is applied row-by-row using pandas .apply()
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
        This function is applied row-by-row using pandas .apply()
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
    # Main processing logic using Pandas DataFrames
    # ============================================================================
    # Reference upstream SQL model and convert to Pandas DataFrame
    trips_df = dbt.ref("stg_trips_v1").df()

    # Aggregate daily revenue using Pandas
    daily_agg = (
        trips_df.groupby("trip_date")
        .agg(
            {
                "total_amount": "sum",
                "trip_id": "count",
                "passenger_count": "sum",
            }
        )
        .reset_index()
    )
    daily_agg.columns = [
        "trip_date",
        "daily_revenue",
        "daily_trips",
        "daily_passengers",
    ]

    # Apply Pandas UDFs row-by-row using .apply()
    daily_agg["is_holiday"] = daily_agg["trip_date"].apply(is_holiday_pandas)
    daily_agg["holiday_name"] = daily_agg["trip_date"].apply(get_holiday_name_pandas)

    # Calculate running revenue using Pandas cumsum
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

    return final_df
