import pandas as pd

df = pd.read_csv("nyc_311_raw.csv")

print("=== SHAPE ===")
print(df.shape)

print("\n=== COLUMNS ===")
print(df.columns.tolist())

print("\n=== DATA TYPES ===")
print(df.dtypes)

print("\n=== NULL COUNTS ===")
null_counts = df.isnull().sum()
null_pct = (df.isnull().sum() / len(df) * 100).round(2)
null_report = pd.DataFrame({"null_count": null_counts, "null_%": null_pct})
print(null_report[null_report["null_count"] > 0].sort_values("null_%", ascending=False))

print("\n=== DUPLICATE ROWS ===")
print(f"Duplicate rows: {df.duplicated().sum()}")

print("\n=== SAMPLE VALUES (first 3 cols with nulls) ===")
top_null_cols = null_report[null_report["null_count"] > 0].head(3).index.tolist()
for col in top_null_cols:
    print(f"\n-- {col} --")
    print(df[col].value_counts(dropna=False).head(5))