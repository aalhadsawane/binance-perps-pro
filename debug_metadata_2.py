import polars as pl

df = pl.DataFrame(schema={
    "timestamp": pl.Datetime("ms"),
    "symbol": pl.Categorical,
    "is_active": pl.Boolean,
    "funding_rate": pl.Float64
})

try:
    funding_stats = (
        df.lazy()
        .filter(pl.col("funding_rate").is_not_null())
        .group_by("symbol")
        .agg(pl.col("timestamp").min().alias("first_funding_time"))
        .collect()
    )
    print("Funding stats:")
    print(funding_stats)
except Exception as e:
    print(e)
