import polars as pl
from metadata_builder import build_symbol_metadata
import sys

# Assume the problem is that 'symbol' after join with 'funding_stats' gets completely nulled out?
# Wait! In build_symbol_metadata:
#         if "symbol" in funding_stats.columns:
#             stats = stats.join(funding_stats, on="symbol", how="left")
# What if funding_stats has a 'symbol' column but with ZERO rows?
# Then it's a left join of stats (100 rows) with funding_stats (0 rows).
# In polars, left join keeps the 100 rows from stats, and fills first_funding_time with nulls.
# Does it drop the symbol column or null it out? Let's see:

df_stats = pl.DataFrame({
    "symbol": ["BTC", "ETH"],
    "first": [1, 2]
}).with_columns(pl.col("symbol").cast(pl.Categorical))

df_funding = pl.DataFrame(schema={"symbol": pl.Categorical, "funding": pl.Int64})

print("Before join:")
print(df_stats)
df_joined = df_stats.join(df_funding, on="symbol", how="left")
print("After join:")
print(df_joined)

# What if df_funding 'symbol' is String instead of Categorical?
df_funding_str = pl.DataFrame(schema={"symbol": pl.Utf8, "funding": pl.Int64})
try:
    df_joined_str = df_stats.join(df_funding_str, on="symbol", how="left")
    print("After string join:")
    print(df_joined_str)
except Exception as e:
    print("Exception on string join:", e)
