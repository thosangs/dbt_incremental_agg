#!/usr/bin/env python3
"""
Download NYC Taxi Trip Data from TLC (Taxi & Limousine Commission).

Downloads Yellow Taxi trip records in Parquet format from:
https://d37ci6vzurychx.cloudfront.net/trip-data/

Usage:
    python scripts/download_nyc_taxi_data.py [--start-year YEAR] [--start-month MONTH]
                                            [--end-year YEAR] [--end-month MONTH]
                                            [--output-dir DIR]

Example:
    # Download 2022-2023 data (default)
    python scripts/download_nyc_taxi_data.py

    # Download specific range
    python scripts/download_nyc_taxi_data.py --start-year 2020 --start-month 1 --end-year 2020 --end-month 12

    # Download for late-arriving data demo (older months)
    python scripts/download_nyc_taxi_data.py --start-year 2021 --start-month 1 --end-year 2021 --end-month 3
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlretrieve

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


def download_file(url, output_path, year, month):
    """Download a single file with progress indication."""
    filename = os.path.basename(output_path)
    print(f"Downloading {filename}...", end=" ", flush=True)

    try:
        urlretrieve(url, output_path)
        file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
        print(f"✓ ({file_size:.1f} MB)")
        return True
    except HTTPError as e:
        if e.code == 404:
            print("✗ Not found (404)")
            return False
        else:
            print(f"✗ HTTP Error {e.code}")
            return False
    except URLError as e:
        print(f"✗ Network Error: {e.reason}")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Download NYC Taxi Trip Data in Parquet format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--start-year", type=int, default=2022, help="Start year (default: 2022)"
    )
    parser.add_argument(
        "--start-month",
        type=int,
        default=1,
        choices=range(1, 13),
        metavar="MONTH",
        help="Start month 1-12 (default: 1)",
    )
    parser.add_argument(
        "--end-year", type=int, default=2023, help="End year (default: 2023)"
    )
    parser.add_argument(
        "--end-month",
        type=int,
        default=12,
        choices=range(1, 13),
        metavar="MONTH",
        help="End month 1-12 (default: 12)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/raw",
        help="Output directory (default: data/raw)",
    )
    parser.add_argument(
        "--skip-existing", action="store_true", help="Skip files that already exist"
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

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate date range
    dates = generate_date_range(
        args.start_year, args.start_month, args.end_year, args.end_month
    )

    print(f"Downloading NYC {DATA_TYPE.upper()} Taxi Trip Data")
    print(
        f"Date range: {args.start_year}-{args.start_month:02d} to {args.end_year}-{args.end_month:02d}"
    )
    print(f"Output directory: {output_dir}")
    print(f"Total files to download: {len(dates)}\n")

    # Download files
    downloaded = 0
    skipped = 0
    failed = 0

    for year, month in dates:
        filename = get_filename(DATA_TYPE, year, month)
        output_path = output_dir / filename

        # Skip if exists
        if args.skip_existing and output_path.exists():
            print(f"Skipping {filename} (already exists)")
            skipped += 1
            continue

        url = get_url(DATA_TYPE, year, month)
        if download_file(url, output_path, year, month):
            downloaded += 1
        else:
            failed += 1

    # Summary
    print("\n" + "=" * 60)
    print("Download complete!")
    print(f"  Downloaded: {downloaded}")
    print(f"  Skipped: {skipped}")
    print(f"  Failed: {failed}")
    print("=" * 60)

    if downloaded > 0:
        total_size = sum(f.stat().st_size for f in output_dir.glob("*.parquet")) / (
            1024**3
        )
        print(f"\nTotal data size: {total_size:.2f} GB")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
