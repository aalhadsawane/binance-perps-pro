import polars as pl
df2 = pl.DataFrame({
    'timestamp': pl.datetime_range(pl.datetime(2022,1,1), pl.datetime(2022,1,2), '1d', eager=True),
    'symbol': ['BTCUSDT', 'ETHUSDT'],
    'is_active': [True, False],
    'funding_rate': [0.001, 0.002],
}).with_columns(pl.col('symbol').cast(pl.Categorical))

stats2 = (
    df2.lazy()
    .filter(pl.col("is_active"))
    .group_by("symbol", maintain_order=True)
    .agg([
        pl.col("timestamp").min().alias("first_trade_time"),
        pl.col("timestamp").max().alias("last_trade_time"),
    ])
    .collect()
)
print("Stats2:\n", stats2)
print("Stats2 to_list:", stats2["symbol"].to_list())
