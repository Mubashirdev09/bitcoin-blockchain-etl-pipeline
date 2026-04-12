import urllib.request
import os
from datetime import datetime
import xml.etree.ElementTree as ET

os.makedirs("data", exist_ok=True)

base_url = "https://aws-public-blockchain.s3.us-east-2.amazonaws.com"
prefix = "v1.0/btc/transactions"

# Generate 1st of every month 2013-2017
dates = []
for year in range(2013, 2018):
    for month in range(1, 13):
        dates.append(f"{year}-{month:02d}-01")

print(f"Total dates to try: {len(dates)}")

downloaded = 0

for date in dates:
    # First list the files in this date folder
    list_url = f"{base_url}?prefix={prefix}/date={date}/&max-keys=5"
    try:
        with urllib.request.urlopen(list_url) as response:
            xml_data = response.read()
        
        # Parse XML to find parquet filename
        root = ET.fromstring(xml_data)
        ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        keys = root.findall('.//s3:Key', ns)
        
        parquet_key = None
        for key in keys:
            if key.text.endswith('.parquet'):
                parquet_key = key.text
                break
        
        if parquet_key:
            file_url = f"{base_url}/{parquet_key}"
            save_path = f"data/btc_{date}.parquet"
            print(f"Downloading {date}...", end=" ")
            urllib.request.urlretrieve(file_url, save_path)
            print(f"✅")
            downloaded += 1
        else:
            print(f"❌ {date} no parquet file found")
            
    except Exception as e:
        print(f"❌ {date} failed: {e}")

print(f"\nDone! Downloaded {downloaded} files 🔥")