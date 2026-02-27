# Usage Guide

## 1. Setup
Ensure your environment is ready:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Define Universe (Optional)
The default universe is ~100 top liquid symbols defined in `symbols.txt`. You can edit this file to add or remove tickers (supports comma/newline/space separation).

## 3. Generate Data
Run the builder script. This one command downloads raw data, processes it into a clean panel, AND generates the symbol metadata file.

```bash
# Recommended: Fast test (2 symbols, 1 month) to verify setup
python process_data.py --max-symbols 2 --end-date 2022-02-01

# Full Production Run (takes ~45-90 mins depending on network)
python process_data.py
```

### Output Files
After a successful run, you will find in `data/`:
1.  `binance_perps_panel_2022_YYYY_YYYYMMDD.parquet`: The main hourly price/funding/OI panel.
2.  `binance_perps_panel_...meta.json`: Metadata about the build (version, hash, etc).
3.  `symbol_information.parquet`: Metadata about the symbols (listing dates, contract specs, status).

## 4. Load & Analyze
Use the helper functions in `loader.py`:

```python
from loader import load_panel, load_metadata

# 1. Load Symbol Info
meta = load_metadata()
print(meta)

# 2. Load Price Data (Lazy)
lf = load_panel()
# Filter for specific time/symbols if needed
df = lf.filter(pl.col("symbol") == "BTCUSDT").collect()
```

## 5. Metadata & Delistings
- **Active Symbols:** Contract specs (tick size, lot size) and dates are fetched from the live Binance API.
- **Delisted Symbols:** If a symbol is in your historical data but not the live API (delisted), the script automatically detects this. It marks the status as `DELISTED` and sets the `delivery_time` to the last observed trade timestamp in the data.
- **Geo-Blocking:** If running from a restricted region (e.g., USA), API metadata might be null. The script handles this gracefully, ensuring empirical stats (trade times) are still populated.
