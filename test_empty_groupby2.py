import polars as pl
import zipfile
import io

# Let's write the current code in metadata_builder.py to see exactly what is returned
with open("binance-perps-pro/metadata_builder.py", "r") as f:
    code = f.read()

# Let's look for anything that drops rows in build_symbol_metadata
