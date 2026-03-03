import polars as pl
from metadata_builder import build_symbol_metadata
# Let's test the `_first` and `_last` logic in Polars
df = pl.DataFrame({
    "timestamp": [1, 2, 3, 4],
    "volume": [0, 10, 20, 0]
})

df = df.with_columns([
    pl.when(pl.col("volume").is_not_null() & (pl.col("volume") > 0))
      .then(pl.col("timestamp"))
      .otherwise(None)
      .first()
      .alias("_first"),
    pl.when(pl.col("volume").is_not_null() & (pl.col("volume") > 0))
      .then(pl.col("timestamp"))
      .otherwise(None)
      .last()
      .alias("_last"),
])

print(df)
