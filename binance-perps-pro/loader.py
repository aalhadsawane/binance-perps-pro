import polars as pl
from pathlib import Path

def load_panel(start_date: str = None, end_date: str = None, symbols: list[str] = None) -> pl.LazyFrame:
    """
    Efficiently loads the Binance Perps Clean Panel (Parquet) with predicate pushdown.
    """
    path = Path(__file__).parent / "data/binance_perps_panel_2022_2026.parquet"
    lf = pl.scan_parquet(path)

    if symbols:
        lf = lf.filter(pl.col("symbol").is_in(symbols))

    if start_date:
        lf = lf.filter(pl.col("timestamp") >= pl.lit(start_date).cast(pl.Datetime))

    if end_date:
        lf = lf.filter(pl.col("timestamp") <= pl.lit(end_date).cast(pl.Datetime))

    return lf
