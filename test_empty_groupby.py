import polars as pl
df = pl.DataFrame({
    "a": ["a", "b", "c"],
    "b": [1, 2, 3]
}).with_columns(pl.col("a").cast(pl.Categorical))

res = df.filter(pl.col("b") > 10).group_by("a").agg(pl.col("b").min())
print("filtered and grouped empty:")
print(res)
print("columns:", res.columns)
print("length:", len(res))
print("a to_list:", res["a"].to_list())
