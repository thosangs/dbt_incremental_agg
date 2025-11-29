#!/usr/bin/env python3
"""
Helper script to get Parquet file sizes.
Called from dbt macro to get actual file sizes.
"""

import sys
from pathlib import Path


def get_parquet_size(directory):
    """Get total size of Parquet files in directory."""
    total_size = 0
    file_count = 0

    dir_path = Path(directory)
    if dir_path.exists():
        for file_path in dir_path.glob("*.parquet"):
            if file_path.is_file():
                total_size += file_path.stat().st_size
                file_count += 1

    size_mb = total_size / (1024 * 1024)
    return size_mb, file_count


if __name__ == "__main__":
    directory = sys.argv[1] if len(sys.argv) > 1 else "/data/raw"
    size_mb, file_count = get_parquet_size(directory)
    print(f"{size_mb:.2f},{file_count}")
