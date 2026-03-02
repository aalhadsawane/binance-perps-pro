import polars as pl

# Maybe Polars 1.x has an issue when casting back from grouped categorical?
# But if unique_symbols is empty, why would stats be empty?
# Let's inspect what happens to the user's data locally if we run process_data.py
# The user ran `python metadata_builder.py` directly, meaning they loaded the previously generated parquet.
# What if the generated parquet doesn't have ANY `is_active=True` rows?
# The user said: "the symbol metadata had some issues: ... ❌ Failed to build symbol metadata: unable to find column \"symbol\"; valid columns: []" in their previous log snippet when running `process_data.py`.
# I fixed the exception by adding `if "symbol" not in stats.columns`.
# So now it safely returns an empty dataframe if `stats.columns` doesn't have `"symbol"`.
# THIS MEANS `stats` does NOT have `"symbol"`!
# WHY DOESN'T `stats` HAVE `"symbol"`?!
# Because `stats` is empty BEFORE `group_by`!
# Why would `panel_df.lazy().filter(pl.col("is_active"))` be EMPTY for the user?!
# If `is_active` is completely False for ALL rows.

# Why would `is_active` be False for ALL rows in a 117.6MB file?
# Let's look at `is_active` logic in `process_data.py`:
#                 pl.when(pl.col("volume").is_not_null() & (pl.col("volume") > 0))
#                   .then(pl.col("timestamp"))
#                   .otherwise(None)
#                   .first()
#                   .alias("_first"),
# Wait! `.first()` over the WHOLE dataframe? NO, it's grouped by symbol?
# Let's check `process_data.py`!
