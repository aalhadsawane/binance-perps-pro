# Binance USDT-M Perpetual Clean Panel (Pro Edition)

A production-grade, backtest-ready hourly dataset for the top 80 liquid USDT-M perpetual futures on Binance.

## Features

- **Period:** 2022-01-01 to Present (4+ years)
- **Universe:** Top 80 liquid symbols (BTC, ETH, SOL, etc.)
- **Resolution:** Hourly (1h)
- **Format:** Parquet (ZSTD compressed, optimized schema)
- **Precision:** Float64 for all financial columns, correct timestamps (UTC ms)
- **Alignment:** Zero lookahead; Funding Rates & Mark Price aligned to hourly grid; `is_active` status handled automatically.
- **Sort Order:** Primary Sort: `symbol` (ASC), Secondary Sort: `timestamp` (ASC).
- **UTC Enforcement:** All timestamps are strictly UTC (timezone-aware) to avoid ambiguity.

## Data Sources & Methodology

### 1. Data Sources
All data is sourced directly from the official **Binance Public Data Repository** (`data.binance.vision`). We strive for the highest fidelity by using the following specific paths:

- **OHLCV:** `data/futures/um/monthly/klines/{symbol}/1h/`
  - *Fallback:* `data/futures/um/daily/klines/{symbol}/1h/` (used if monthly missing, e.g., current month)
- **Funding Rates:** `data/futures/um/monthly/fundingRate/{symbol}/`
- **Mark Price:** `data/futures/um/monthly/markPriceKlines/{symbol}/1h/`
  - *Note:* We use `markPriceKlines` instead of `premiumIndex` to ensure consistent monthly file availability and kline-structured data.
- **Open Interest:** `data/futures/um/daily/metrics/{symbol}/`
  - *Note:* Binance **does not** provide monthly archives for metrics/Open Interest. The script automatically detects this and downloads daily files for the entire history. This is expected behavior.

### 2. Methodology & Cleaning
We apply a rigorous cleaning pipeline to transform raw dumps into a "Quant-Ready" panel:

1.  **Headerless CSV Handling:** Binance kline files (OHLC, Mark Price) are headerless. We manually enforce the schema (`open_time`, `open`, `high`, `low`, `close`, `volume`, etc.) to prevent data corruption.
2.  **UTC & Timestamp Normalization:**
    -   Raw timestamps (milliseconds) are parsed strictly as UTC.
    -   Funding Rate and Mark Price timestamps are **truncated to the hour** (`.dt.truncate("1h")`) to align perfectly with the hourly candle grid. This solves the issue of sparse/misaligned funding rows.
3.  **Strict Typing:** All price/volume columns are cast to `Float64` (double precision) to avoid rounding errors common with `Float32`.
4.  **Hourly Grid Enforcement:** We generate a complete hourly timestamp grid from `2022-01-01` to `Now`.
    -   **Left Join:** Data is joined onto this grid.
    -   **Missing Data:** If a symbol has no data for a timestamp (e.g., prior to listing, or exchange downtime), the row remains with `null` values. We **do not** silently forward-fill prices, as this introduces synthetic artifacts.
5.  **Metrics Fallback:** For Open Interest, since monthly files are unavailable, the system robustly iterates through daily files, with retry logic for intermittent server errors (5xx).

### 3. Disclaimers
-   **Lagged Features:** The dataset includes engineered features like `ret_1h` (1-hour return) and `fwd_ret_1h`. Naturally, the **first timestamp** for every symbol will have `null` for backward-looking features (returns) and the **last timestamp** will have `null` for forward-looking features (targets).
-   **Open Interest:** Sourced from `sum_open_interest_value` (USD Notional) in the Binance `metrics` files.
-   **Factors:** We focus on the core "Quant Panel" columns (OHLCV, Funding, Mark, OI). High-frequency data (Tickers, Trades, Book Depth) is intentionally excluded to maintain a lightweight, hourly resolution suitable for backtesting.

## Symbol Metadata
A separate file `data/symbol_information.parquet` contains static and statistical metadata for the universe.

| Column | Description | Source |
|--------|-------------|--------|
| `symbol` | Ticker | Binance API |
| `base_asset` | Base Currency | Binance API |
| `quote_asset` | Quote Currency | Binance API |
| `listing_time` | Listing Date | Binance API (`onboardDate`) |
| `delivery_time` | Delivery/Delisting Date | Binance API (`deliveryDate`) |
| `status` | Trading Status | Binance API |
| `tick_size` | Min Price Increment | Binance API |
| `min_qty` | Min Quantity | Binance API |
| `min_notional` | Min Trade Value | Binance API |
| `first_trade_time` | First candle timestamp | Derived from Data |
| `last_trade_time` | Last candle timestamp | Derived from Data |
| `first_funding_time`| First funding timestamp| Derived from Data |

Use `loader.load_metadata()` to access this file.

### Troubleshooting: Missing Metadata (Nulls)
The `symbol_information.parquet` file relies on the Binance Futures API to populate contract specifications (tick size, leverage, etc.).
- **Issue:** If you see `null` in fields like `tick_size` or `status`, it is likely because the script is running in a **Restricted Jurisdiction (e.g., USA)** where `fapi.binance.com` is blocked (HTTP 451).
- **Fallback:** The script attempts to use the **Testnet** API (`testnet.binancefuture.com`) as a fallback. However, testnet symbols may not match mainnet symbols (e.g., BTCUSDT might not be listed or have different specs).
- **Solution:** To get complete metadata, run the `process_data.py` script from a non-restricted IP address (VPN or VPS) or provide a custom proxy if you are technically inclined. The core OHLCV data (from `data.binance.vision`) is **NOT** geo-blocked and will work regardless.

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

#### Customizing the Universe (Two Ways)
1. **At Build Time (Master Universe):** Edit the `symbols.txt` file to define the complete list of symbols you want to download and include in the Parquet file. The default file contains ~100 top liquid symbols.
   - The script parses `symbols.txt` (supporting commas, spaces, or newlines) and builds the dataset based on this list.
   - **Note:** `symbols.txt` defines the *content* of the dataset. If you modify it, you must re-run `process_data.py` to rebuild the panel.

2. **At Load Time (Runtime Subset):** Use the `load_panel` function (see below) to load a *subset* of the built data into memory. This is faster and avoids loading the entire 1GB+ file if you only need specific assets.

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
