from datetime import datetime

import pyspark.sql.functions as F
import pyspark.sql.types as T


def model(dbt, session):
    """
    Python model demonstrating Holidays package integration with PySpark.
    This model enriches daily revenue aggregation with US holiday information.

    Uses:
    - Holidays package to identify US holidays
    - PySpark UDFs for distributed holiday checking
    - References upstream SQL model (stg_trips)
    """

    dbt.config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by=["trip_date"],
        on_schema_change="append_new_columns",
        # Note: packages not supported with Thrift method on standalone Spark
        # holidays package must be installed in the dbt container
    )

    # Import holidays after package installation
    from holidays import US

    # Reference upstream SQL model
    trips_df = dbt.ref("stg_trips_v1")

    # Create US holidays dictionary for a reasonable date range
    # NYC taxi data typically spans 2020-2025, so we'll cover that range
    # In production, you might want to make this configurable or derive from data
    years = range(2020, 2026)
    us_holidays = US(years=years)

    # Convert holidays to formats suitable for UDF lookup
    # Spark dates come as date objects, so we'll compare date objects directly
    holiday_dates_set = set(us_holidays.keys())
    holiday_names_dict = us_holidays.copy()

    # Create UDFs for holiday checking
    @F.udf(returnType=T.BooleanType())
    def is_holiday(date_val):
        """Check if a date is a US holiday"""
        if date_val is None:
            return False
        try:
            # Convert Spark date to Python date object for comparison
            from datetime import date

            if isinstance(date_val, date):
                return date_val in holiday_dates_set
            # Handle string dates
            if isinstance(date_val, str):
                date_obj = datetime.strptime(date_val, "%Y-%m-%d").date()
                return date_obj in holiday_dates_set
            return False
        except Exception:
            return False

    @F.udf(returnType=T.StringType())
    def get_holiday_name(date_val):
        """Get the name of the holiday if it's a holiday"""
        if date_val is None:
            return None
        try:
            from datetime import date

            # Convert Spark date to Python date object for lookup
            if isinstance(date_val, date):
                return holiday_names_dict.get(date_val)
            # Handle string dates
            if isinstance(date_val, str):
                date_obj = datetime.strptime(date_val, "%Y-%m-%d").date()
                return holiday_names_dict.get(date_obj)
            return None
        except Exception:
            return None

    # Aggregate daily revenue
    daily_agg = trips_df.groupBy("trip_date").agg(
        F.sum("total_amount").alias("daily_revenue"),
        F.count("*").alias("daily_trips"),
        F.sum("passenger_count").alias("daily_passengers"),
    )

    # Add holiday information
    # Spark date columns are passed as Python date objects to UDFs
    df_with_holidays = daily_agg.withColumn(
        "is_holiday", is_holiday(F.col("trip_date"))
    ).withColumn("holiday_name", get_holiday_name(F.col("trip_date")))

    # Calculate running revenue
    from pyspark.sql.window import Window

    window_spec = Window.orderBy("trip_date").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    final_df = (
        df_with_holidays.withColumn(
            "running_revenue", F.sum("daily_revenue").over(window_spec)
        )
        .select(
            "trip_date",
            "is_holiday",
            "holiday_name",
            "daily_revenue",
            "daily_trips",
            "daily_passengers",
            "running_revenue",
        )
        .orderBy("trip_date")
    )

    return final_df
