import polars as pl
from pathlib import Path
import os
import sys

sys.path.append(os.path.abspath('binance-perps-pro'))
from metadata_builder import build_symbol_metadata

# Let's create a parquet file matching the user's setup
df = pl.DataFrame({
    'timestamp': pl.datetime_range(pl.datetime(2022,1,1), pl.datetime(2022,1,5), '1d', eager=True),
    'symbol': ['BTCUSDT', 'BTCUSDT', 'ETHUSDT', 'ETHUSDT', 'SOLUSDT'],
    'is_active': [True, True, True, False, False],
    'funding_rate': [0.001, None, 0.002, 0.001, None],
}).with_columns(pl.col('symbol').cast(pl.Categorical))

df.write_parquet("test_panel.parquet")

df_loaded = pl.read_parquet("test_panel.parquet")
print("Schema:", df_loaded.schema)

res = build_symbol_metadata(df_loaded, None)
print("Res:")
print(res)
