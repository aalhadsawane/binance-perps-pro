import polars as pl
from pathlib import Path
import os
import sys

sys.path.append(os.path.abspath('binance-perps-pro'))
from metadata_builder import build_symbol_metadata

# Let's inspect the user's issue with `is_active`
# When `process_data.py` builds the panel:
#             .with_columns([
#                pl.col("timestamp").is_between(pl.col("_first"), pl.col("_last")).fill_null(False).alias("is_active"),
#            ])

# It's possible `stats["symbol"].to_list()` is returning elements that don't match exactly with `stats.filter(pl.col("symbol") == sym)`!
# Wait! In `metadata_builder.py`:
#        emp_data = stats.filter(pl.col("symbol") == sym).to_dicts()[0]
# What if `stats.filter` returns NO rows because `sym` is a STRING and `stats["symbol"]` is CATEGORICAL?
# Let's test that!

stats = pl.DataFrame({
    "symbol": ["BTC", "ETH"],
    "val": [1, 2]
}).with_columns(pl.col("symbol").cast(pl.Categorical))

unique_symbols = stats["symbol"].drop_nulls().to_list()
print("Unique symbols (type):", [type(x) for x in unique_symbols])
print("Unique symbols:", unique_symbols)

sym = unique_symbols[0]
filtered = stats.filter(pl.col("symbol") == sym)
print("Filtered length:", len(filtered))

# Wait, if `stats.filter` works, then `to_dicts()[0]` works.
