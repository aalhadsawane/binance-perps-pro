import polars as pl
from metadata_builder import build_symbol_metadata

df = pl.read_parquet("binance-perps-pro/data/binance_perps_panel_2022_2026_20260227.parquet")

print("DF Schema:", df.schema)
print("DF length:", len(df))

def build_debug(panel_df, exchange_info):
    stats = (
        panel_df.lazy()
        .filter(pl.col("is_active"))
        .group_by("symbol")
        .agg([
            pl.col("timestamp").min().alias("first_trade_time"),
            pl.col("timestamp").max().alias("last_trade_time"),
        ])
        .collect()
    )
    print("Stats columns:", stats.columns)
    print("Stats length:", len(stats))

    if "symbol" not in stats.columns:
        print("Symbol not in columns!")
        unique_symbols = []
    else:
        print("Symbol IS in columns.")
        funding_stats = (
            panel_df.lazy()
            .filter(pl.col("funding_rate").is_not_null())
            .group_by("symbol")
            .agg(pl.col("timestamp").min().alias("first_funding_time"))
            .collect()
        )
        if "symbol" in funding_stats.columns:
            print("Joining funding stats")
            stats = stats.join(funding_stats, on="symbol", how="left")
        else:
            print("Funding stats has no symbol")
            stats = stats.with_columns(pl.lit(None).cast(pl.Datetime("ms")).alias("first_funding_time"))

    if "symbol" in stats.columns:
        unique_symbols = stats["symbol"].to_list()
    else:
        unique_symbols = []

    print("Unique symbols length:", len(unique_symbols))

    rows = []
    for sym in unique_symbols:
        emp_data = stats.filter(pl.col("symbol") == sym).to_dicts()[0]
        rows.append({"symbol": sym, **emp_data})

    print("Rows length:", len(rows))
    return rows

build_debug(df, None)
