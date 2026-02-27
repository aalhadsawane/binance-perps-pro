# Human TODO: API Restrictions & Data Sources

This project uses two primary data sources:

1.  **Binance Public Data (`data.binance.vision`)**:
    -   This is the source for **ALL historical market data** (OHLCV, Funding Rates, Mark Price, Open Interest metrics).
    -   These files are downloaded via HTTP from `https://data.binance.vision/`.
    -   **Status:** ✅ **Not Geo-Blocked.** This data is accessible globally, including from the US.
    -   You can verify this by visiting: `https://data.binance.vision/?prefix=data/futures/um/monthly/klines/BTCUSDT/1h/`

2.  **Binance Futures API (`fapi.binance.com`)**:
    -   This is used **ONLY** for fetching the latest symbol metadata (contract specifications like tick size, lot size, listing status) via the `/fapi/v1/exchangeInfo` endpoint.
    -   **Status:** ❌ **Geo-Blocked in Restricted Jurisdictions (e.g., USA).**
    -   If you run `process_data.py` from a US IP address, this specific step will fail with an HTTP 451 error.
    -   **Result:** The script will still generate the main price panel, but the `symbol_information.parquet` file will contain `null` values for API-specific fields (e.g., `tick_size`, `status`).

**Action Required:**
If you are located in a restricted region and require the full metadata (tick sizes, etc.):
1.  Run the `process_data.py` script once from a non-restricted IP address (e.g., via a VPN or cloud server).
2.  Alternatively, you can manually populate the `data/symbol_information.parquet` file using data from another source.
