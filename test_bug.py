import polars as pl
from metadata_builder import build_symbol_metadata

df = pl.DataFrame({
    'timestamp': pl.datetime_range(pl.datetime(2022,1,1), pl.datetime(2022,1,2), '1d', eager=True),
    'symbol': ['BTCUSDT', 'ETHUSDT'],
    'is_active': [True, False],
    'funding_rate': [0.001, 0.002],
}).with_columns(pl.col('symbol').cast(pl.Categorical))

res = build_symbol_metadata(df, None)
print(res)
