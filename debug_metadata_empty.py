import polars as pl
from metadata_builder import build_symbol_metadata

df = pl.DataFrame({
    "timestamp": [pl.datetime(2022,1,1)],
    "symbol": ["BTCUSDT"],
    "is_active": [False],
    "funding_rate": [None]
})

res = build_symbol_metadata(df, None)
print(res)
