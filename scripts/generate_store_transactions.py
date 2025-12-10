#!/usr/bin/env python3
"""
Generate Store Transaction Data for DuckDB + dbt Demo.

This script:
1. Generates realistic store transaction data (orders, buyers, products) for the last 90 days
2. Exports data as Hive-partitioned Parquet files using DuckDB
3. Simulates realistic patterns: variable volume (5K-50K orders/day), weekends lower, weekdays higher

Usage:
    python scripts/generate_store_transactions.py [--days DAYS] [--partitioned-dir DIR]

Example:
    # Generate last 90 days (default)
    python scripts/generate_store_transactions.py

    # Generate last 30 days
    python scripts/generate_store_transactions.py --days 30
"""

import argparse
import sys
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from multiprocessing import cpu_count
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

# Payment methods
PAYMENT_METHODS = [
    "credit_card",
    "debit_card",
    "cash",
    "paypal",
    "apple_pay",
    "google_pay",
]


# Product categories and price ranges (simplified)
# Generate products with different categories and price ranges
def generate_products():
    """Generate a list of products with categories and base prices."""
    products = []
    for i in range(1000):
        if i < 100:
            category = "electronics"
            base_price = np.random.uniform(50, 500)
        elif i < 300:
            category = "clothing"
            base_price = np.random.uniform(10, 150)
        elif i < 500:
            category = "food"
            base_price = np.random.uniform(5, 50)
        elif i < 700:
            category = "books"
            base_price = np.random.uniform(8, 30)
        else:
            category = "home"
            base_price = np.random.uniform(20, 200)
        products.append({"id": i, "category": category, "base_price": base_price})
    return products


PRODUCTS = generate_products()


def generate_orders_for_date(target_date: date, base_orders: int) -> pd.DataFrame:
    """
    Generate orders for a specific date with realistic patterns (VECTORIZED VERSION).

    Args:
        target_date: Date to generate orders for
        base_orders: Base number of orders (adjusted for weekday/weekend patterns)

    Returns:
        DataFrame with columns: order_id, buyer_id, order_date, order_timestamp,
                                product_id, quantity, unit_price, revenue, payment_method
    """
    # Adjust volume based on day of week (weekends lower)
    weekday = target_date.weekday()  # 0=Monday, 6=Sunday
    if weekday >= 5:  # Saturday or Sunday
        volume_multiplier = 0.4  # 40% of weekday volume
    else:
        volume_multiplier = 1.0

    # Add some randomness
    volume_multiplier *= np.random.uniform(0.8, 1.2)

    num_orders = int(base_orders * volume_multiplier)
    num_orders = max(1000, num_orders)  # Minimum 1000 orders per day

    # Generate buyer pool (mix of returning and new customers)
    # 30% returning customers, 70% one-time/new
    num_returning_buyers = int(num_orders * 0.3)
    num_new_buyers = num_orders - num_returning_buyers

    # Generate buyer IDs using vectorized operations
    returning_buyer_ids = np.array(
        [f"buyer_{x}" for x in np.random.randint(1, 10000, size=num_returning_buyers)]
    )
    new_buyer_ids = np.array(
        [f"buyer_{x}" for x in np.random.randint(10000, 99999, size=num_new_buyers)]
    )
    buyer_ids = np.concatenate([returning_buyer_ids, new_buyer_ids])

    # Generate number of products per order (1-5, weighted towards 1-2)
    # Using vectorized choice
    products_per_order = np.random.choice(
        [1, 2, 3, 4, 5], size=num_orders, p=[0.4, 0.3, 0.15, 0.1, 0.05]
    )

    # Generate order timestamps (distributed throughout the day)
    hours = np.random.randint(8, 22, size=num_orders)
    minutes = np.random.randint(0, 60, size=num_orders)
    seconds = np.random.randint(0, 60, size=num_orders)

    # Generate order IDs
    order_ids = np.array([str(uuid.uuid4()) for _ in range(num_orders)])

    # Calculate total number of transaction rows (sum of products_per_order)
    total_transactions = products_per_order.sum()

    # Expand arrays to match transaction count
    # Repeat each order_id and buyer_id by the number of products in that order
    expanded_order_ids = np.repeat(order_ids, products_per_order)
    expanded_buyer_ids = np.repeat(buyer_ids, products_per_order)

    # Expand timestamps
    expanded_hours = np.repeat(hours, products_per_order)
    expanded_minutes = np.repeat(minutes, products_per_order)
    expanded_seconds = np.repeat(seconds, products_per_order)

    # Generate timestamps
    base_datetime = datetime.combine(target_date, datetime.min.time())
    order_timestamps = np.array(
        [
            base_datetime.replace(hour=h, minute=m, second=s)
            for h, m, s in zip(expanded_hours, expanded_minutes, expanded_seconds)
        ]
    )

    # Generate product IDs (vectorized)
    product_indices = np.random.randint(0, len(PRODUCTS), size=total_transactions)
    product_ids = np.array([PRODUCTS[idx]["id"] for idx in product_indices])
    base_prices = np.array([PRODUCTS[idx]["base_price"] for idx in product_indices])

    # Generate quantities (1-4 units per product)
    quantities = np.random.randint(1, 5, size=total_transactions)

    # Generate unit prices with variation (0.8-1.2x base price)
    price_multipliers = np.random.uniform(0.8, 1.2, size=total_transactions)
    unit_prices = base_prices * price_multipliers
    unit_prices = np.round(unit_prices, 2)

    # Calculate revenue
    revenues = np.round(quantities * unit_prices, 2)

    # Generate payment methods (vectorized)
    payment_methods = np.random.choice(PAYMENT_METHODS, size=total_transactions)

    # Create DataFrame directly from arrays (much faster than appending dicts)
    df = pd.DataFrame(
        {
            "order_id": expanded_order_ids,
            "buyer_id": expanded_buyer_ids,
            "order_date": target_date,
            "order_timestamp": order_timestamps,
            "product_id": product_ids,
            "quantity": quantities,
            "unit_price": unit_prices,
            "revenue": revenues,
            "payment_method": payment_methods,
        }
    )

    return df


def process_date(target_date: date, base_orders: int):
    """
    Helper function to process a single date and return result with date info.
    Must be at module level for multiprocessing pickling.
    """
    daily_df = generate_orders_for_date(target_date, base_orders)
    return target_date, daily_df


def export_to_partitioned_parquet(df: pd.DataFrame, partitioned_dir: Path):
    """
    Export DataFrame to Hive-partitioned Parquet files using DuckDB.

    Args:
        df: DataFrame with store transaction data
        partitioned_dir: Base directory for partitioned output
    """
    if df.empty:
        print("  ⚠ No data to export")
        return False

    print(f"  Exporting {len(df):,} transactions...", end=" ", flush=True)

    try:
        # Connect to DuckDB (in-memory)
        conn = duckdb.connect()

        # Create partitioned base directory
        partitioned_base = partitioned_dir.resolve()
        partitioned_base.mkdir(parents=True, exist_ok=True)

        # Add partition columns
        df["year"] = pd.to_datetime(df["order_date"]).dt.year
        df["month"] = pd.to_datetime(df["order_date"]).dt.month
        df["date"] = pd.to_datetime(df["order_date"]).dt.day

        # Convert paths to absolute strings for DuckDB
        output_path_str = str(partitioned_base.resolve()).replace("\\", "/")

        # Use DuckDB's native PARTITION_BY for Hive-style partitioning
        # This creates: partitioned/year=YYYY/month=MM/date=DD/store_transactions_X.parquet
        partition_query = f"""
        COPY (
            SELECT
                order_id,
                buyer_id,
                order_date,
                order_timestamp,
                product_id,
                quantity,
                unit_price,
                revenue,
                payment_method,
                YEAR(DATE(order_date)) AS year,
                MONTH(DATE(order_date)) AS month,
                DAY(DATE(order_date)) AS date
            FROM df
            WHERE order_date IS NOT NULL
              AND revenue IS NOT NULL
              AND revenue > 0
        ) TO '{output_path_str}'
        (
            FORMAT PARQUET,
            PARTITION_BY (year, month, date),
            OVERWRITE_OR_IGNORE,
            FILENAME_PATTERN 'store_transactions_{{i}}'
        )
        """

        # Register DataFrame with DuckDB
        conn.register("df", df)
        conn.execute(partition_query)

        # Count files written
        files_written = len(list(partitioned_base.rglob("*.parquet")))

        conn.close()

        print(f"✓ ({files_written} partitioned files)")
        return True

    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Generate Store Transaction Data for DuckDB + dbt Demo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days to generate (default: 90)",
    )
    parser.add_argument(
        "--partitioned-dir",
        type=str,
        default="data/partitioned",
        help="Directory for partitioned Parquet files (default: data/partitioned)",
    )
    parser.add_argument(
        "--base-orders",
        type=int,
        default=20000,
        help="Base number of orders per weekday (default: 20000, weekends are ~40% of this)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel workers (default: min(CPU count, days))",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.days < 1:
        print("Error: --days must be >= 1", file=sys.stderr)
        sys.exit(1)

    # Create directories
    partitioned_dir = Path(args.partitioned_dir)
    partitioned_dir.mkdir(parents=True, exist_ok=True)

    # Calculate date range (last N days from today)
    end_date = date.today()
    start_date = end_date - timedelta(days=args.days - 1)

    print("=" * 70)
    print("Store Transaction Data Generation")
    print("=" * 70)
    print(f"Date range: {start_date} to {end_date} ({args.days} days)")
    print(f"Partitioned directory: {partitioned_dir}")
    print(f"Base orders per weekday: {args.base_orders:,}")
    print()

    # Generate data using multiprocessing for parallel day processing
    print("Generating transactions (using multiprocessing)...")
    print("-" * 70)

    # Generate list of dates to process
    dates_to_process = [start_date + timedelta(days=i) for i in range(args.days)]

    # Use multiprocessing to generate data for multiple days in parallel
    if args.workers is not None:
        num_workers = min(args.workers, args.days)
    else:
        num_workers = min(cpu_count(), args.days)  # Don't use more workers than days

    print(f"Using {num_workers} parallel worker(s)")
    print()
    all_transactions = []
    total_transactions = 0

    # Process dates in parallel
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Submit all tasks (pass base_orders as argument)
        future_to_date = {
            executor.submit(process_date, d, args.base_orders): d
            for d in dates_to_process
        }

        # Collect results as they complete
        results = {}
        completed = 0
        for future in as_completed(future_to_date):
            target_date, daily_df = future.result()
            results[target_date] = daily_df
            completed += 1
            print(
                f"  {target_date.strftime('%Y-%m-%d')} ({target_date.strftime('%A')})... "
                f"✓ {len(daily_df):,} transactions [{completed}/{args.days}]",
                flush=True,
            )
            total_transactions += len(daily_df)

    # Combine all transactions in date order
    print()
    print("Combining transactions...", end=" ", flush=True)
    sorted_dates = sorted(results.keys())
    all_transactions = [results[d] for d in sorted_dates]
    combined_df = pd.concat(all_transactions, ignore_index=True)
    print(f"✓ {len(combined_df):,} total transactions")

    # Export to partitioned Parquet
    print()
    print("Exporting to Hive-partitioned Parquet files...")
    print("-" * 70)

    success = export_to_partitioned_parquet(combined_df, partitioned_dir)

    # Summary
    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Date range: {start_date} to {end_date}")
    print(f"Total transactions: {total_transactions:,}")
    print(f"Total orders: {combined_df['order_id'].nunique():,}")
    print(f"Total buyers: {combined_df['buyer_id'].nunique():,}")
    print(f"Total products: {combined_df['product_id'].nunique():,}")
    print(f"Total revenue: ${combined_df['revenue'].sum():,.2f}")
    print(
        f"Average order value: ${combined_df.groupby('order_id')['revenue'].sum().mean():,.2f}"
    )

    if success:
        # Calculate total size of partitioned data
        total_size = sum(
            f.stat().st_size for f in partitioned_dir.rglob("*.parquet")
        ) / (1024**2)  # MB
        print(f"Total partitioned data size: {total_size:.2f} MB")
        print(f"Partitioned files: {len(list(partitioned_dir.rglob('*.parquet')))}")

    print("=" * 70)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
