import pandas as pd

url = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv?$limit=50000&$order=created_date+DESC"

print("Downloading 50k rows...")
df = pd.read_csv(url)

df.to_csv("nyc_311_raw.csv", index=False)
print(f"Done! Shape: {df.shape}")
print(df.head())