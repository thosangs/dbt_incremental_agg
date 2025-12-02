#!/usr/bin/env python3
"""
Download and Repartition NYC Taxi Trip Data using DuckDB.

This script:
1. Downloads monthly Parquet files from NYC TLC
2. Repartitions them into daily Parquet files using DuckDB
3. Deletes the original monthly files to save space

Usage:
    python scripts/download_and_repartition.py [--start-year YEAR] [--start-month MONTH]
                                               [--end-year YEAR] [--end-month MONTH]
                                               [--raw-dir DIR] [--partitioned-dir DIR]

Example:
    # Download and repartition Sept-Oct 2025 data (default)
    python scripts/download_and_repartition.py

    # Download and repartition specific range
    python scripts/download_and_repartition.py --start-year 2025 --start-month 9 --end-year 2025 --end-month 10
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

import aiohttp
import duckdb

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_TYPE = "yellow"  # yellow, green, fhv, fhvhv


def generate_date_range(start_year, start_month, end_year, end_month):
    """Generate list of (year, month) tuples for the date range."""
    dates = []
    current = datetime(start_year, start_month, 1)
    end = datetime(end_year, end_month, 1)

    while current <= end:
        dates.append((current.year, current.month))
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return dates


def get_filename(data_type, year, month):
    """Generate filename for NYC taxi data."""
    return f"{data_type}_tripdata_{year}-{month:02d}.parquet"


def get_url(data_type, year, month):
    """Generate download URL for NYC taxi data."""
    filename = get_filename(data_type, year, month)
    return f"{BASE_URL}/{filename}"


async def download_file(session, semaphore, url, output_path, year, month):
    """Download a single file asynchronously with progress indication."""
    filename = os.path.basename(output_path)

    async with semaphore:  # Limit concurrent downloads
        print(f"Downloading {filename}...", end=" ", flush=True)

        try:
            async with session.get(url) as response:
                if response.status == 404:
                    print("✗ Not found (404)")
                    return False

                if response.status != 200:
                    print(f"✗ HTTP Error {response.status}")
                    return False

                # Write file in chunks
                with open(output_path, "wb") as f:
                    async for chunk in response.content.iter_chunked(8192):
                        f.write(chunk)

                file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
                print(f"✓ ({file_size:.1f} MB)")
                return True

        except asyncio.TimeoutError:
            print("✗ Timeout")
            return False
        except aiohttp.ClientError as e:
            print(f"✗ Network Error: {e}")
            return False
        except Exception as e:
            print(f"✗ Error: {e}")
            return False


def repartition_with_duckdb(raw_dir, partitioned_dir, year, month):
    """
    Repartition monthly Parquet file into daily Parquet files using DuckDB.
    Deletes the original monthly file after successful repartitioning.
    """
    filename = get_filename(DATA_TYPE, year, month)
    raw_file = Path(raw_dir) / filename

    if not raw_file.exists():
        print(f"  ⚠ Skipping repartition: {filename} not found")
        return False

    print(f"  Repartitioning {filename}...", end=" ", flush=True)

    try:
        # Connect to DuckDB (in-memory)
        conn = duckdb.connect()

        # Create partitioned directory structure: partitioned/YYYY/MM/DD/*.parquet
        partitioned_base = Path(partitioned_dir)
        partitioned_base.mkdir(parents=True, exist_ok=True)

        # Get distinct dates from the file
        dates_query = f"""
        SELECT DISTINCT DATE(tpep_pickup_datetime) AS trip_date
        FROM read_parquet('{raw_file}')
        WHERE tpep_pickup_datetime IS NOT NULL
          AND DATE(tpep_pickup_datetime) IS NOT NULL
        ORDER BY trip_date
        """
        dates_df = conn.execute(dates_query).df()

        if dates_df.empty:
            print("✗ No valid dates found")
            conn.close()
            return False

        # Repartition by date - write each day to a separate file
        files_written = 0
        for _, row in dates_df.iterrows():
            trip_date = row["trip_date"]
            date_str = trip_date.strftime("%Y-%m-%d")

            # Create directory structure: partitioned/YYYY/MM/DD/
            date_dir = (
                partitioned_base
                / trip_date.strftime("%Y")
                / trip_date.strftime("%m")
                / trip_date.strftime("%d")
            )
            date_dir.mkdir(parents=True, exist_ok=True)

            # Write daily partition
            daily_file = date_dir / f"{DATA_TYPE}_tripdata_{date_str}.parquet"
            daily_query = f"""
            COPY (
                SELECT
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime,
                    VendorID,
                    total_amount,
                    fare_amount,
                    tip_amount,
                    tolls_amount,
                    passenger_count,
                    trip_distance,
                    PULocationID,
                    DOLocationID,
                    payment_type,
                    RatecodeID,
                    trip_date
                FROM read_parquet('{raw_file}')
                WHERE DATE(tpep_pickup_datetime) = DATE '{trip_date}'
                  AND tpep_pickup_datetime IS NOT NULL
                  AND total_amount IS NOT NULL
                  AND total_amount > 0
            ) TO '{daily_file}' (FORMAT PARQUET)
            """
            conn.execute(daily_query)
            files_written += 1

        conn.close()

        # Delete original monthly file after successful repartitioning
        raw_file.unlink()
        print(f"✓ ({files_written} daily files, original deleted)")

        return True

    except Exception as e:
        print(f"✗ Error: {e}")
        return False


async def main():
    parser = argparse.ArgumentParser(
        description="Download and repartition NYC Taxi Trip Data using DuckDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--start-year", type=int, default=2025, help="Start year (default: 2025)"
    )
    parser.add_argument(
        "--start-month",
        type=int,
        default=9,
        choices=range(1, 13),
        metavar="MONTH",
        help="Start month 1-12 (default: 9)",
    )
    parser.add_argument(
        "--end-year", type=int, default=2025, help="End year (default: 2025)"
    )
    parser.add_argument(
        "--end-month",
        type=int,
        default=10,
        choices=range(1, 13),
        metavar="MONTH",
        help="End month 1-12 (default: 10)",
    )
    parser.add_argument(
        "--raw-dir",
        type=str,
        default="data/raw",
        help="Directory for raw monthly files (default: data/raw)",
    )
    parser.add_argument(
        "--partitioned-dir",
        type=str,
        default="data/partitioned",
        help="Directory for daily partitioned files (default: data/partitioned)",
    )
    parser.add_argument(
        "--skip-existing", action="store_true", help="Skip files that already exist"
    )
    parser.add_argument(
        "--keep-monthly",
        action="store_true",
        help="Keep monthly files after repartitioning (default: delete them)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Number of concurrent downloads (default: 4)",
    )

    args = parser.parse_args()

    # Validate date range
    if args.start_year > args.end_year:
        print("Error: start-year must be <= end-year", file=sys.stderr)
        sys.exit(1)

    if args.start_year == args.end_year and args.start_month > args.end_month:
        print(
            "Error: start-month must be <= end-month when years are equal",
            file=sys.stderr,
        )
        sys.exit(1)

    # Create directories
    raw_dir = Path(args.raw_dir)
    partitioned_dir = Path(args.partitioned_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    partitioned_dir.mkdir(parents=True, exist_ok=True)

    # Generate date range
    dates = generate_date_range(
        args.start_year, args.start_month, args.end_year, args.end_month
    )

    print("=" * 70)
    print("NYC Taxi Data Download and Repartition")
    print("=" * 70)
    print(
        f"Date range: {args.start_year}-{args.start_month:02d} to {args.end_year}-{args.end_month:02d}"
    )
    print(f"Raw directory: {raw_dir}")
    print(f"Partitioned directory: {partitioned_dir}")
    print(f"Concurrent downloads: {args.concurrency}")
    print(f"Total months: {len(dates)}")
    print(f"Keep monthly files: {args.keep_monthly}")
    print()

    # Step 1: Download monthly files
    print("Step 1: Downloading monthly Parquet files...")
    print("-" * 70)

    semaphore = asyncio.Semaphore(args.concurrency)
    tasks = []
    skipped = 0

    # Set timeout for downloads (5 minutes per file)
    timeout = aiohttp.ClientTimeout(total=300)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for year, month in dates:
            filename = get_filename(DATA_TYPE, year, month)
            output_path = raw_dir / filename

            # Skip if exists
            if args.skip_existing and output_path.exists():
                print(f"Skipping {filename} (already exists)")
                skipped += 1
                continue

            url = get_url(DATA_TYPE, year, month)
            task = download_file(session, semaphore, url, output_path, year, month)
            tasks.append(task)

        # Execute all downloads concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Count results
    downloaded = sum(1 for r in results if r is True)
    failed = sum(1 for r in results if r is False or isinstance(r, Exception))

    print()
    print(f"Downloaded: {downloaded}, Skipped: {skipped}, Failed: {failed}")

    if downloaded == 0 and skipped == 0:
        print("No files to process. Exiting.")
        return 0 if failed == 0 else 1

    # Step 2: Repartition using DuckDB
    print()
    print("Step 2: Repartitioning monthly files into daily partitions...")
    print("-" * 70)

    repartitioned = 0
    repartition_failed = 0

    for year, month in dates:
        filename = get_filename(DATA_TYPE, year, month)
        raw_file = raw_dir / filename

        # Only repartition if file exists (downloaded or skipped)
        if raw_file.exists():
            success = repartition_with_duckdb(raw_dir, partitioned_dir, year, month)
            if success:
                repartitioned += 1
            else:
                repartition_failed += 1
        elif not args.skip_existing:
            # File should exist but doesn't - might have failed to download
            repartition_failed += 1

    # Step 3: Summary
    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Downloaded: {downloaded} files")
    print(f"Skipped: {skipped} files")
    print(f"Download failed: {failed} files")
    print(f"Repartitioned: {repartitioned} files")
    print(f"Repartition failed: {repartition_failed} files")

    if repartitioned > 0:
        # Calculate total size of partitioned data
        total_size = sum(
            f.stat().st_size for f in partitioned_dir.rglob("*.parquet")
        ) / (1024**3)
        print(f"Total partitioned data size: {total_size:.2f} GB")

        if not args.keep_monthly:
            remaining_monthly = len(list(raw_dir.glob("*.parquet")))
            print(f"Remaining monthly files: {remaining_monthly}")

    print("=" * 70)

    return 0 if (failed == 0 and repartition_failed == 0) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
