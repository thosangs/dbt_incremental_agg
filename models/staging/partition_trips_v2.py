"""
Partition NYC Taxi Trip Data by trip_date

Reads monthly Parquet files from /data/raw and writes daily-partitioned Parquet to /data/partitioned.
This Python model does exactly the same thing as partition_trips.sql but using PySpark DataFrame API.
"""

import pyspark.sql.functions as F


def model(dbt, session):
    """
    Partition NYC Taxi Trip Data by trip_date using PySpark.

    Reads from /data/raw (monthly Parquet files) and writes to /data/partitioned
    partitioned by trip_date (daily partitions).
    """
    dbt.config(
        materialized="table",
        location="/data/partitioned",
        file_format="parquet",
        partition_by=["trip_date"],
    )

    # Read monthly Parquet files from /data/raw
    raw_df = session.read.parquet("/data/raw")

    # Filter valid records and extract trip_date
    df_with_date = (
        raw_df.filter(
            F.col("tpep_pickup_datetime").isNotNull()
            & F.col("total_amount").isNotNull()
            & (F.col("total_amount") > 0)
        )
        .withColumn("trip_date", F.to_date(F.col("tpep_pickup_datetime")))
        .filter(F.col("trip_date").isNotNull())
    )

    # Select all columns including trip_date
    # trip_date will be used for partitioning
    final_df = df_with_date.select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "VendorID",
        "total_amount",
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "RatecodeID",
        "trip_date",
    )

    return final_df
