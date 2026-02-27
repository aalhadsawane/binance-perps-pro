# Human TODO

This project relies on the Binance Futures API (`fapi.binance.com`) to fetch symbol metadata (e.g., tick size, lot size, listing date).

**Important:** The Binance Futures API is geo-blocked in certain jurisdictions, most notably the United States. If you run `process_data.py` from a US IP address, the metadata step will fail (HTTP 451), resulting in `null` values for API-specific fields in `symbol_information.parquet`.

**Action Required:**
If you are located in a restricted region:
1.  Run the `process_data.py` script from a non-restricted IP address (e.g., via a VPN or a cloud server in a supported region) to generate the full dataset including metadata.
2.  Alternatively, you can manually construct or update the `data/symbol_information.parquet` file if you have access to the metadata from another source.

The core OHLCV data download (from `data.binance.vision`) is **not** geo-blocked and will work regardless of your location.
