import pandas as pd
import glob
import os

files = glob.glob("data/*.parquet")
good = []
bad = []

for f in files:
    try:
        pd.read_parquet(f, engine='pyarrow')
        good.append(f)
    except:
        bad.append(f)
        os.remove(f)  # delete corrupted file

print(f"✅ Good files: {len(good)}")
print(f"❌ Bad files deleted: {len(bad)}")
print("\nBad files were:")
for b in bad:
    print(b)