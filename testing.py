import pandas as pd

df = pd.read_parquet("temp_file_genres")

print(df["genres"].unique())
