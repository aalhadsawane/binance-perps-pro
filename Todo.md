# TODO & Technical Notes

## Data Sources
- **OHLCV:** `data.binance.vision` (Monthly Klines)
- **Funding Rates:** `data.binance.vision` (Monthly Funding Rate)
- **Mark Price:** `data.binance.vision` (Monthly Mark Price Klines)
- **Open Interest:** `data.binance.vision` (Daily Metrics - Monthly not available)
- **Symbol Metadata:** `fapi.binance.com` (Live API) + Derived Stats

## ⚠️ Notes

### 1. Cache Invalidation
The script uses `CODE_VERSION` tracking. If the logic or schema changes, the `cache/` directory is automatically cleared to ensure data integrity.

### 2. Funding Rates
Funding occurs every 8h (or 4h for some pairs). The dataset maintains a full hourly grid, so `funding_rate` will be `null` for non-funding hours. This is expected behavior to preserve point-in-time accuracy.

### 3. Memory Profile
- With 80 symbols: ~2-3 GB peak RAM.
- Safe for 8GB+ machines.

## 🟢 Performance
- **Fast Test (Recommended):** `python process_data.py --max-symbols 2 --end-date 2022-02-01` (~30-60s)
- **Full Run:** 35-70 mins (first run), seconds (cached).
