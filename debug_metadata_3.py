import polars as pl
from metadata_builder import build_symbol_metadata

df = pl.DataFrame(schema={
    "timestamp": pl.Datetime("ms"),
    "symbol": pl.Categorical,
    "is_active": pl.Boolean,
    "funding_rate": pl.Float64
})

try:
    res = build_symbol_metadata(df, None)
    print("res:")
    print(res)
except Exception as e:
    import traceback
    traceback.print_exc()
