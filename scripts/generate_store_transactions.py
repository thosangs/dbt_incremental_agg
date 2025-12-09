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
from datetime import date, datetime, timedelta
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
    Generate orders for a specific date with realistic patterns.

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

    # Generate buyer IDs
    # Returning buyers (reuse IDs from previous days)
    returning_buyer_ids = [
        f"buyer_{np.random.randint(1, 10000)}" for _ in range(num_returning_buyers)
    ]
    # New buyers (unique IDs)
    new_buyer_ids = [
        f"buyer_{np.random.randint(10000, 99999)}" for _ in range(num_new_buyers)
    ]
    buyer_ids = returning_buyer_ids + new_buyer_ids

    # Generate orders (each order can have 1-5 products)
    orders = []
    order_counter = 0

    for buyer_id in buyer_ids:
        # Number of products per order (1-5, weighted towards 1-2)
        num_products = np.random.choice([1, 2, 3, 4, 5], p=[0.4, 0.3, 0.15, 0.1, 0.05])

        # Generate order timestamp (distributed throughout the day)
        hour = np.random.randint(8, 22)  # 8 AM to 10 PM
        minute = np.random.randint(0, 60)
        second = np.random.randint(0, 60)
        order_timestamp = datetime.combine(target_date, datetime.min.time()).replace(
            hour=hour, minute=minute, second=second
        )

        order_id = str(uuid.uuid4())

        # Generate products for this order
        for _ in range(num_products):
            product = PRODUCTS[np.random.randint(0, len(PRODUCTS))]
            product_id = product["id"]
            quantity = np.random.randint(1, 5)  # 1-4 units per product
            unit_price = product["base_price"] * np.random.uniform(
                0.8, 1.2
            )  # Some price variation
            revenue = quantity * unit_price
            payment_method = np.random.choice(PAYMENT_METHODS)

            orders.append(
                {
                    "order_id": order_id,
                    "buyer_id": buyer_id,
                    "order_date": target_date,
                    "order_timestamp": order_timestamp,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": round(unit_price, 2),
                    "revenue": round(revenue, 2),
                    "payment_method": payment_method,
                }
            )

        order_counter += 1
        if order_counter >= num_orders:
            break

    return pd.DataFrame(orders)


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

    # Generate data day by day (to manage memory)
    print("Generating transactions...")
    print("-" * 70)

    all_transactions = []
    current_date = start_date
    total_transactions = 0

    while current_date <= end_date:
        print(
            f"  {current_date.strftime('%Y-%m-%d')} ({current_date.strftime('%A')})...",
            end=" ",
            flush=True,
        )

        # Generate orders for this date
        daily_df = generate_orders_for_date(current_date, args.base_orders)
        all_transactions.append(daily_df)
        total_transactions += len(daily_df)

        print(f"✓ {len(daily_df):,} transactions")

        current_date += timedelta(days=1)

    # Combine all transactions
    print()
    print("Combining transactions...", end=" ", flush=True)
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
