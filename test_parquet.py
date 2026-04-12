import pandas as pd
import glob

# Step 1: Get all parquet files from data folder
files = glob.glob("data/*.parquet")

print("Files found:", files)

# Step 2: Read and combine all files
df_list = []

for file in files:
    print(f"Reading {file}...")
    temp_df = pd.read_parquet(file)
    df_list.append(temp_df)

df = pd.concat(df_list, ignore_index=True)

# Step 3: Show total rows
print("\nTotal rows:", len(df))

# Step 4: Show sample data
print("\nSample Data:")
print(df.head())

# Step 5: Show important columns
print("\nImportant Columns Preview:")
print(df[['txid', 'output_value', 'block_timestamp']].head())

# Step 6: Show all column names
print("\nColumns:")
print(df.columns)