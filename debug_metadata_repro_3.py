import polars as pl

df = pl.DataFrame({
    "timestamp": [pl.datetime(2022,1,1)],
    "symbol": ["BTCUSDT"],
    "is_active": [True],
    "funding_rate": [0.001]
}).with_columns(pl.col("symbol").cast(pl.Categorical))

print(df.lazy().filter(pl.col("is_active")).group_by("symbol").agg(pl.col("timestamp").min()).collect())
