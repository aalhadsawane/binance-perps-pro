# Usage: Binance Perps Pro

This pipeline generates two critical artifacts for quantitative research:
1.  **Main Panel (`binance_perps_panel_*.parquet`):** A clean, hourly, backtest-ready dataset of OHLCV, Funding Rates, Mark Price, and Open Interest.
2.  **Symbol Metadata (`symbol_information.parquet`):** A comprehensive table of contract specifications (e.g., tick size, listing date, status).

## Quick Start

### 1. Prerequisites
-   Python 3.10+
-   Dependencies installed: `pip install -r requirements.txt`
-   **Note:** If you are in the US, see `HumanTODO.md` for information about API restrictions on metadata.

### 2. Run the Pipeline
The `process_data.py` script handles everything: downloading raw data, cleaning, aligning timestamps, and generating both Parquet files.

```bash
# Build the full dataset (all symbols in symbols.txt, from 2022-01-01 to yesterday)
python process_data.py
```

**Options:**
-   `--max-symbols N`: Process only the first N symbols (useful for testing).
-   `--end-date YYYY-MM-DD`: Specify a custom end date (default is yesterday).

```bash
# Example: Fast test run
python process_data.py --max-symbols 2 --end-date 2023-01-01
```

### 3. Load the Data
Use the provided `loader.py` to efficiently load the generated files.

```python
from loader import load_panel, load_metadata

# 1. Load the Main Panel (LazyFrame for efficiency)
# Automatically finds the latest generated file.
lf = load_panel(start_date="2023-01-01")
df = lf.collect()
print(df)

# 2. Load Symbol Metadata (DataFrame)
meta_df = load_metadata()
print(meta_df)
```

## Output Files
After a successful run, you will find the following in the `data/` directory:

-   `binance_perps_panel_2022_YYYY_YYYYMMDD.parquet`: The main dataset.
-   `symbol_information.parquet`: The metadata file.
-   `binance_perps_panel_*.meta.json`: Summary statistics about the build.
