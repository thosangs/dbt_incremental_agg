#!/usr/bin/env python3
"""
Partition NYC Taxi Trip Data by trip_date for efficient Spark queries.

Reads raw Parquet files and repartitions them by date (extracted from tpep_pickup_datetime).
This enables Spark's partition-aware operations and efficient partition overwrites.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


def partition_data(spark, input_dir, output_dir, delete_source=False):
    """
    Read raw Parquet files and partition by trip_date.

    Args:
        spark: SparkSession
        input_dir: Directory containing raw Parquet files
        output_dir: Directory to write partitioned Parquet files
        delete_source: If True, delete source files after partitioning
    """
    print(f"Reading Parquet files from: {input_dir}")

    # Read all Parquet files
    df = spark.read.parquet(f"{input_dir}/*.parquet")

    # Extract trip_date from tpep_pickup_datetime
    # Filter out nulls and invalid dates
    df_with_date = (
        df.filter(
            col("tpep_pickup_datetime").isNotNull()
            & col("total_amount").isNotNull()
            & (col("total_amount") > 0)
        )
        .withColumn("trip_date", to_date(col("tpep_pickup_datetime")))
        .filter(col("trip_date").isNotNull())
    )

    # Get row count before partitioning
    total_rows = df_with_date.count()
    print(f"Total rows to partition: {total_rows:,}")

    # Get date range
    date_stats = df_with_date.agg(min("trip_date"), max("trip_date")).collect()[0]
    min_date = date_stats["min(trip_date)"]
    max_date = date_stats["max(trip_date)"]
    print(f"Date range: {min_date} to {max_date}")

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Write partitioned by trip_date
    print(f"Writing partitioned Parquet files to: {output_dir}")
    print("Partitioning by trip_date...")

    df_with_date.write.mode("overwrite").partitionBy("trip_date").parquet(
        str(output_dir), compression="snappy"
    )

    print("✓ Partitioning complete!")

    # Count partitions created
    partition_dirs = list(Path(output_dir).glob("trip_date=*"))
    print(f"Created {len(partition_dirs)} date partitions")

    # Delete source files if requested
    if delete_source:
        print(f"Deleting source files from: {input_dir}")
        input_path = Path(input_dir)
        deleted_count = 0
        for parquet_file in input_path.glob("*.parquet"):
            parquet_file.unlink()
            deleted_count += 1
        print(f"✓ Deleted {deleted_count} source files")

    return total_rows, len(partition_dirs)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Partition NYC Taxi Trip Data by trip_date",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default="/data/raw",
        help="Input directory with raw Parquet files (default: /data/raw)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/data/partitioned",
        help="Output directory for partitioned Parquet files (default: /data/partitioned)",
    )
    parser.add_argument(
        "--delete-source",
        action="store_true",
        help="Delete source files after partitioning",
    )

    args = parser.parse_args()

    # Create Spark session
    # Connect to Spark cluster via master URL
    import os

    spark_master = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

    print(f"Initializing Spark session (master: {spark_master})...")
    spark = (
        SparkSession.builder.appName("PartitionNYCTaxiData")
        .master(spark_master)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .getOrCreate()
    )

    try:
        total_rows, partition_count = partition_data(
            spark, args.input_dir, args.output_dir, delete_source=args.delete_source
        )

        print("\n" + "=" * 60)
        print("Partitioning complete!")
        print(f"  Total rows: {total_rows:,}")
        print(f"  Partitions: {partition_count}")
        print(f"  Output: {args.output_dir}")
        print("=" * 60)

        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
