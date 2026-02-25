import polars as pl
from pathlib import Path

def load_panel(start_date: str = None, end_date: str = None, symbols: list[str] = None) -> pl.LazyFrame:
    """
    Efficiently loads the latest Binance Perps Clean Panel (Parquet) with predicate pushdown.
    """
    data_dir = Path(__file__).parent / "data"
    # Find the latest parquet file matching the pattern
    files = sorted(data_dir.glob("binance_perps_panel_*.parquet"))

    if not files:
        raise FileNotFoundError(f"No panel parquet file found in {data_dir}. Run process_data.py first.")

    path = files[-1]  # Pick the latest one
    lf = pl.scan_parquet(path)

    if symbols:
        lf = lf.filter(pl.col("symbol").is_in(symbols))

    if start_date:
        lf = lf.filter(pl.col("timestamp") >= pl.lit(start_date).cast(pl.Datetime))

    if end_date:
        lf = lf.filter(pl.col("timestamp") <= pl.lit(end_date).cast(pl.Datetime))

    return lf

def load_metadata() -> pl.DataFrame:
    """
    Loads the Symbol Information (Metadata) Parquet file.
    """
    path = Path(__file__).parent / "data/symbol_information.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Metadata file not found at {path}. Run process_data.py first.")

    return pl.read_parquet(path)
