# Binance USDT-M Perpetual Clean Panel (Pro Edition)

A production-grade, backtest-ready hourly dataset for the top 80 liquid USDT-M perpetual futures on Binance.

## Features

- **Period:** 2022-01-01 to Present (4+ years)
- **Universe:** Top 80 liquid symbols (BTC, ETH, SOL, etc.)
- **Resolution:** Hourly (1h)
- **Format:** Parquet (ZSTD compressed, optimized schema)
- **Precision:** Float64 for all financial columns, correct timestamps (UTC ms)
- **Alignment:** Zero lookahead; Funding Rates & Premium Index aligned to hourly grid; `is_active` status handled automatically.
- **UTC Enforcement:** All timestamps are strictly UTC (timezone-aware) to avoid ambiguity.

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Generate Data
Run the builder script to download raw data and build the panel.
```bash
# Build full history (takes ~45-90 mins depending on network)
python process_data.py

# Or run a fast test (first 2 symbols, 1 month)
python process_data.py --max-symbols 2 --end-date 2022-02-01
```

### 3. Load Data
Use the included lazy loader for efficient access. **Ensure `loader.py` is in your Python path or working directory.**

```python
from loader import load_panel

# Load entire history lazy
lf = load_panel()

# Load specific slice with optional arguments
# start_date: ISO string (inclusive)
# end_date: ISO string (inclusive)
# symbols: List of ticker strings
df = load_panel(start_date="2023-01-01", end_date="2023-06-01", symbols=["BTCUSDT", "ETHUSDT"]).collect()
print(df)
```

## Data Handling & Methodology

### 1. Missing Data & Gaps
- **Candles:** We strictly adhere to the available data. If a candle is missing from Binance source files (e.g., maintenance or outage), the row is present in the timestamp grid but OHLCV columns will be `null` (NaN). **We DO NOT forward-fill prices silently.** It is up to the user to decide how to handle these gaps (e.g., `ffill` in Polars).
- **Start Date:** Data starts from **2022-01-01**. If a symbol was listed after this date, its rows will be `null` and `is_active` will be `False` until the first valid trade volume appears.

### 2. Universe Selection & Dead Coins
- **Universe:** The dataset tracks a fixed list of ~80 top liquid symbols (found in `symbols.txt`).
- **Dead/Delisted Coins:** If a coin in the universe was delisted, its data remains available up to the delisting point.
- **Survivorship Bias:** The `is_active` boolean flag allows you to filter for currently trading assets.
  - `is_active = True`: The symbol had valid trading volume > 0 within the known history window (from first trade to last trade).
  - Use `df.filter(pl.col("is_active"))` to exclude pre-listing or post-delisting periods.

### 3. Alignment & Normalization
- **Timestamps:** All timestamps are normalized to **UTC Milliseconds** (`datetime[ms, UTC]`).
- **Funding Rates:** Joined exactly at their payment times (every 4h or 8h). Intermediate hourly rows contain `null` for `funding_rate`, preserving the discrete nature of payments.
- **Deduplication:** Timestamps are guaranteed unique per symbol. Monotonicity is enforced.

### 4. Updates & Versioning
- **Reproducibility:** The `process_data.py` script is deterministic.
- **Versioning:** A `version.json` file tracks the build version. If code logic changes (e.g., schema update), the cache is automatically invalidated to ensure data correctness.

## Data Schema

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | Datetime (ms, UTC) | Candle open time / Observation time |
| `symbol` | Categorical | Ticker (e.g., BTCUSDT) |
| `open` | Float64 | Open Price |
| `high` | Float64 | High Price |
| `low` | Float64 | Low Price |
| `close` | Float64 | Close Price |
| `volume` | Float64 | Base Asset Volume |
| `dollar_volume` | Float64 | Volume * Close |
| `funding_rate` | Float64 | Periodic funding rate (only at funding times, else NaN) |
| `mark_price` | Float64 | Mark Price (Forward filled hourly) |
| `open_interest` | Float64 | Open Interest (Forward filled hourly) |
| `is_active` | Boolean | True if symbol is trading (has volume) |
| `next_funding_rate` | Float64 | Forward-looking funding rate (shifted) |
| `ret_1h` | Float64 | Log return over past hour |
| `fwd_ret_1h` | Float64 | Forward return (target) |

## Notebooks

Check `notebooks/` for examples:
1. `01_momentum_funding_factor.ipynb`: Feature engineering demo.
2. `02_backtest.ipynb`: Vectorized backtest with transaction costs & funding payments.
3. `03_custom_factor.ipynb`: Writing custom Polars factors.

## Technical Details

- **Parsing:** Handles Binance's headerless CSVs and timestamp scaling correctly.
- **Missing Data:** Implements a monthly download fallback strategy to recover missing daily files (common in 2022).
- **Validation:** Enforces OHLC integrity (High >= Low, etc.) and timestamp monotonicity.
- **Cache:** Smart caching prevents re-processing valid symbols; versioning ensures cache invalidation on code updates.
