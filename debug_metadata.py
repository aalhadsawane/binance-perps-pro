import polars as pl
from metadata_builder import build_symbol_metadata

df = pl.DataFrame(schema={
    "timestamp": pl.Datetime("ms"),
    "symbol": pl.Categorical,
    "is_active": pl.Boolean,
    "funding_rate": pl.Float64
})

print("Stats shape:")
try:
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
    print(stats)
except Exception as e:
    print(e)
