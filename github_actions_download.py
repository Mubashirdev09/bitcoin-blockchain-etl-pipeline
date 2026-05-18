import urllib.request
import xml.etree.ElementTree as ET
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# Get today's date
today = datetime.now().strftime("%Y-%m-%d")
print(f"Starting Bitcoin data download for {today}...")

# AWS settings
base_url = "https://aws-public-blockchain.s3.us-east-2.amazonaws.com"
prefix = "v1.0/btc/transactions"

# Step 1: Find today's parquet file on AWS
print(f"Looking for Bitcoin data on AWS for {today}...")
list_url = f"{base_url}?prefix={prefix}/date={today}/&max-keys=5"

try:
    with urllib.request.urlopen(list_url, timeout=60) as response:
        xml_data = response.read()

    root = ET.fromstring(xml_data)
    ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    keys = root.findall('.//s3:Key', ns)

    parquet_key = None
    for key in keys:
        if key.text.endswith('.parquet'):
            parquet_key = key.text
            break

    if not parquet_key:
        print(f"No data found for {today} yet - AWS may not have published it yet")
        print("This is normal - try again tomorrow")
        exit(0)

    # Step 2: Download the file
    file_url = f"{base_url}/{parquet_key}"
    save_path = f"/tmp/btc_{today}.parquet"
    print(f"Downloading {today} data...")
    urllib.request.urlretrieve(file_url, save_path)
    print(f"Downloaded successfully!")

    # Step 3: Upload to Azure Data Lake
    print(f"Uploading to Azure Data Lake...")
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

    if not conn_str:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING not found!")

    blob_service = BlobServiceClient.from_connection_string(conn_str)

    # Upload to bronze layer
    blob_client = blob_service.get_blob_client(
        container="btc-transactions",
        blob=f"bronze/btc_{today}.parquet"
    )
    with open(save_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)

    print(f"✅ Successfully uploaded btc_{today}.parquet to Azure bronze layer!")
    print(f"Pipeline run complete for {today}")

except Exception as e:
    print(f"❌ Error: {e}")
    raise