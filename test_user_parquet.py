import polars as pl
from pathlib import Path
import os
import sys

# Assume the user generated a panel file in binance-perps-pro/data
data_dir = Path("binance-perps-pro/data")
files = sorted(data_dir.glob("binance_perps_panel_*.parquet"))

if files:
    latest = files[-1]
    df = pl.read_parquet(latest)
    print("Columns:", df.columns)

    # Try the steps from metadata_builder
    stats = (
        df.lazy()
        .filter(pl.col("is_active"))
        .group_by("symbol")
        .agg([
            pl.col("timestamp").min().alias("first_trade_time"),
            pl.col("timestamp").max().alias("last_trade_time"),
        ])
        .collect()
    )
    print("Stats shape:", stats.shape)
    if "symbol" in stats.columns:
        print("Symbols in stats:", len(stats["symbol"]))
        print("Nulls in symbol:", stats["symbol"].null_count())
        unique = stats["symbol"].drop_nulls().to_list()
        print("Unique length:", len(unique))
    else:
        print("No symbol column")
else:
    print("No panel found to test")
