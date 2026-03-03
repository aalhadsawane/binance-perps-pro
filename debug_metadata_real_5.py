import polars as pl

df = pl.DataFrame({
    "timestamp": [1, 2, 3, 4],
    "volume": [None, 10, 20, None]
})

df = df.with_columns([
    pl.when(pl.col("volume").is_not_null() & (pl.col("volume") > 0))
      .then(pl.col("timestamp"))
      .otherwise(None)
      .drop_nulls()
      .first()
      .alias("_first"),
    pl.when(pl.col("volume").is_not_null() & (pl.col("volume") > 0))
      .then(pl.col("timestamp"))
      .otherwise(None)
      .drop_nulls()
      .last()
      .alias("_last"),
])

print(df)

df = df.with_columns([
    pl.col("timestamp").is_between(pl.col("_first"), pl.col("_last")).fill_null(False).alias("is_active"),
])

print(df)
