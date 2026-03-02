import polars as pl
from metadata_builder import build_symbol_metadata

df = pl.DataFrame({
    "timestamp": [pl.datetime(2022,1,1)],
    "symbol": ["BTCUSDT"],
    "is_active": [True],
    "funding_rate": [0.001]
}).with_columns(pl.col("symbol").cast(pl.Categorical))

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
    print("Stats:")
    print(stats)

    if "symbol" not in stats.columns:
        unique_symbols = []
    else:
        funding_stats = (
            panel_df.lazy()
            .filter(pl.col("funding_rate").is_not_null())
            .group_by("symbol")
            .agg(pl.col("timestamp").min().alias("first_funding_time"))
            .collect()
        )
        if "symbol" in funding_stats.columns:
            stats = stats.join(funding_stats, on="symbol", how="left")
        else:
            stats = stats.with_columns(pl.lit(None).cast(pl.Datetime("ms")).alias("first_funding_time"))

    print("Stats after join:")
    print(stats)

    if "symbol" in stats.columns:
        unique_symbols = stats["symbol"].to_list()
    else:
        unique_symbols = []

    print("Unique symbols:", unique_symbols)

    rows = []
    for sym in unique_symbols:
        emp_data = stats.filter(pl.col("symbol") == sym).to_dicts()[0]
        print("Emp data:", emp_data)
        rows.append({"symbol": sym, **emp_data})

    print("Rows:", rows)
    return rows

build_debug(df, None)
