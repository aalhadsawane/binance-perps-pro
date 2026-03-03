import polars as pl
import os
import sys

sys.path.append(os.path.abspath('binance-perps-pro'))

# Wait, if stats.filter(pl.col("symbol") == sym).to_dicts()[0] gives an error if empty, and no error is raised...
# THEN unique_symbols MUST BE EMPTY!
# Why would unique_symbols be empty if there IS data?
# Let's write a script that does EXACTLY what metadata_builder does:

df = pl.DataFrame({
    'timestamp': pl.datetime_range(pl.datetime(2022,1,1), pl.datetime(2022,1,5), '1d', eager=True),
    'symbol': ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'LINKUSDT', 'ADAUSDT'],
    'is_active': [True, True, True, False, False],
    'funding_rate': [0.001, None, 0.002, 0.001, None],
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

print("Stats:")
print(stats)

funding_stats = (
    df.lazy()
    .filter(pl.col("funding_rate").is_not_null())
    .group_by("symbol")
    .agg(pl.col("timestamp").min().alias("first_funding_time"))
    .collect()
)
print("Funding Stats:")
print(funding_stats)

if "symbol" in funding_stats.columns:
    stats = stats.join(funding_stats, on="symbol", how="left")
else:
    stats = stats.with_columns(pl.lit(None).cast(pl.Datetime("ms")).alias("first_funding_time"))

print("Stats after join:")
print(stats)

if "symbol" in stats.columns:
    unique_symbols = stats["symbol"].drop_nulls().to_list()
else:
    unique_symbols = []

print("Unique symbols:", unique_symbols)
