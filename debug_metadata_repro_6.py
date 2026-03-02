import polars as pl
from metadata_builder import build_symbol_metadata

df = pl.DataFrame({
    'timestamp': pl.datetime_range(pl.datetime(2022,1,1), pl.datetime(2022,1,2), '1d', eager=True),
    'symbol': ['BTCUSDT', 'ETHUSDT'],
    'is_active': [True, True],
    'funding_rate': [0.001, 0.002],
}).with_columns(pl.col('symbol').cast(pl.Categorical))

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

    # Check if "symbol" not in stats.columns => unique_symbols = []
    # BUT WHAT IF it IS in columns, but there are ZERO rows?
    # then stats["symbol"].to_list() is []
    # Oh wait!
    # Let's say stats has 2 rows (BTCUSDT, ETHUSDT)

    unique_symbols = stats["symbol"].drop_nulls().to_list()
    print("Unique symbols via drop_nulls:", unique_symbols)

    # What did I write in metadata_builder.py?
    # unique_symbols = stats["symbol"].to_list()
    # Is it possible that stats["symbol"] contains nulls?
    # group_by on categorical in polars 1.x MIGHT return ALL categories, with null values for the aggregated columns, OR it might drop categories with no rows.
    # WAIT! If group_by returns all categories, the user has 80 symbols in categorical mapping, but many have 0 rows in `.filter(is_active)`.
    # Let's see what happens if we filter 1 category:
    df2 = pl.DataFrame({
        'timestamp': pl.datetime_range(pl.datetime(2022,1,1), pl.datetime(2022,1,2), '1d', eager=True),
        'symbol': ['BTCUSDT', 'ETHUSDT'],
        'is_active': [True, False],
        'funding_rate': [0.001, 0.002],
    }).with_columns(pl.col('symbol').cast(pl.Categorical))

    stats2 = (
        df2.lazy()
        .filter(pl.col("is_active"))
        .group_by("symbol")
        .agg([
            pl.col("timestamp").min().alias("first_trade_time"),
            pl.col("timestamp").max().alias("last_trade_time"),
        ])
        .collect()
    )
    print("Stats2:\n", stats2)
    print("Stats2 to_list:", stats2["symbol"].to_list())
build_debug(None, None)
