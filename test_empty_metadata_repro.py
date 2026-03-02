import polars as pl

# Imagine user's data has a lot of rows but funding_rate is completely null
# wait, the user's data isn't missing funding_rate. Funding rate downloaded for AAVE and 1INCH.
# Let's read what the user output was:
# 2026-03-02 20:33:09,394 | INFO | Calculating empirical stats from Panel data...
# shape: (0, 16)
# This was their output when they added `print(metadata_df)`.
# Since `metadata_df` was completely empty, the issue must be that `unique_symbols` is empty.
# BUT we saw earlier that `unique_symbols` shouldn't be empty for actual panel data.

# Let's test the Polars `group_by` -> `drop_nulls` behavior!
# If `stats` is computed using `group_by("symbol")`
df = pl.DataFrame({
    "symbol": ["BTCUSDT", "ETHUSDT"],
    "is_active": [True, True],
    "timestamp": [1, 2],
    "funding_rate": [0.1, 0.2]
}).with_columns(pl.col("symbol").cast(pl.Categorical))

stats = (
    df.lazy()
    .filter(pl.col("is_active"))
    .group_by("symbol")
    .agg(pl.col("timestamp").min())
    .collect()
)

print(stats)
print("Unique:", stats["symbol"].drop_nulls().to_list())

# What if `is_active` has a null value instead of False?
df_null = pl.DataFrame({
    "symbol": ["BTCUSDT", "ETHUSDT"],
    "is_active": [None, None],
    "timestamp": [1, 2],
    "funding_rate": [0.1, 0.2]
}).with_columns([
    pl.col("symbol").cast(pl.Categorical),
    pl.col("is_active").cast(pl.Boolean)
])
stats_null = (
    df_null.lazy()
    .filter(pl.col("is_active"))
    .group_by("symbol")
    .agg(pl.col("timestamp").min())
    .collect()
)
print("Stats null:")
print(stats_null)
