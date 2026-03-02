import polars as pl
from metadata_builder import build_symbol_metadata

df = pl.DataFrame({
    'timestamp': pl.datetime_range(pl.datetime(2022,1,1), pl.datetime(2022,1,5), '1d', eager=True),
    'symbol': ['BTCUSDT', 'BTCUSDT', 'ETHUSDT', 'ETHUSDT', 'SOLUSDT'],
    'is_active': [True, True, True, False, False],
    'funding_rate': [0.001, None, 0.002, 0.001, None],
}).with_columns(pl.col('symbol').cast(pl.Categorical))

# Drop BTCUSDT to test what happens if some categories are not active
df_filtered = df.filter(pl.col("symbol") != "BTCUSDT")

res = build_symbol_metadata(df_filtered, None)
print(res)
