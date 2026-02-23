# Binance USDT-M Perpetual Clean Panel (Pro Edition)

A production-grade, backtest-ready hourly dataset for the top 80 liquid USDT-M perpetual futures on Binance.

## Features

- **Period:** 2022-01-01 to Present (4+ years)
- **Universe:** Top 80 liquid symbols (BTC, ETH, SOL, etc.)
- **Resolution:** Hourly (1h)
- **Format:** Parquet (ZSTD compressed, optimized schema)
- **Precision:** Float64 for all financial columns, correct timestamps (UTC ms)
- **Alignment:** Zero lookahead; Funding Rates & Premium Index aligned to hourly grid; `is_active` status handled automatically.

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
Use the included lazy loader for efficient access.
```python
from loader import load_panel

# Load entire history lazy
lf = load_panel()

# Load specific slice
df = load_panel(start_date="2023-01-01", end_date="2023-06-01", symbols=["BTCUSDT", "ETHUSDT"]).collect()
print(df)
```

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
