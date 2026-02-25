# Data Requirements
This project relies on the Binance Futures API for symbol metadata (contract specs, listing dates) and manual input for delisting dates.

## Missing or Restricted Data
The following data points are not reliably available from the public API or require manual maintenance:

### 1. Delisting Dates (`delisting_time`)
- **Source:** Manual input via `delistings.csv`.
- **Reason:** The API's `deliveryDate` often defaults to a far-future placeholder (e.g., 2100) for perpetual contracts, even for delisted symbols.
- **Action:** Users should maintain `delistings.csv` in the root directory with columns `symbol,delisting_time` (Format: `YYYY-MM-DDTHH:MM:SS`).

### 2. Max Leverage (`max_leverage`)
- **Source:** Unavailable.
- **Reason:** The `leverageBracket` endpoint requires authentication (API Key) and is not accessible publicly.
- **Action:** This field is currently set to `null` in `symbol_information.parquet`.

### 3. API Restrictions (HTTP 451)
- **Issue:** `fapi.binance.com` is geo-blocked in the US.
- **Impact:** `symbol_information.parquet` will contain `null` for API-derived fields (tick size, lot size, etc.) if run from a restricted IP.
- **Action:** Run the script from a supported jurisdiction.
