import polars as pl

df = pl.DataFrame({
    'timestamp': pl.datetime_range(pl.datetime(2022,1,1), pl.datetime(2022,1,2), '1d', eager=True),
    'symbol': ['BTCUSDT', 'ETHUSDT'],
    'is_active': [True, True],
    'funding_rate': [0.001, 0.002],
}).with_columns(pl.col('symbol').cast(pl.Categorical))

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

funding_stats = (
    df.lazy()
    .filter(pl.col("funding_rate").is_not_null())
    .group_by("symbol")
    .agg(pl.col("timestamp").min().alias("first_funding_time"))
    .collect()
)

print("stats:\n", stats)
print("funding_stats:\n", funding_stats)

stats = stats.join(funding_stats, on="symbol", how="left")
print("after join:\n", stats)

if "symbol" in stats.columns:
    unique_symbols = stats["symbol"].drop_nulls().to_list()
else:
    unique_symbols = []

print("unique_symbols:\n", unique_symbols)
