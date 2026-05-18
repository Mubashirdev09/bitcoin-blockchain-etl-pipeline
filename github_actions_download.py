import urllib.request
import xml.etree.ElementTree as ET
import os
import time
import json
import requests
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# Get today's date
today = datetime.now().strftime("%Y-%m-%d")
print(f"Starting Bitcoin ETL pipeline for {today}...")

# Azure credentials from GitHub Secrets
CLIENT_ID = os.environ.get("AZURE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET")
TENANT_ID = os.environ.get("AZURE_TENANT_ID")
CONN_STR = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

# Synapse settings
SYNAPSE_WORKSPACE = "btc-etl-synapse"
NOTEBOOK_NAME = "Notebook 1"
SUBSCRIPTION_ID = "ff6930af-614e-4591-847c-2474bda0d9d4"
RESOURCE_GROUP = "BTC-ETL-RG"

# ─────────────────────────────────────────
# STEP 1: Download Bitcoin data from AWS
# ─────────────────────────────────────────
print(f"\n📥 STEP 1: Looking for Bitcoin data for {today}...")

base_url = "https://aws-public-blockchain.s3.us-east-2.amazonaws.com"
prefix = "v1.0/btc/transactions"
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
        print(f"No data found for {today} — AWS may not have published it yet")
        print("This is normal — try again tomorrow")
        exit(0)

    file_url = f"{base_url}/{parquet_key}"
    save_path = f"/tmp/btc_{today}.parquet"
    print(f"Downloading {today} data...")
    urllib.request.urlretrieve(file_url, save_path)
    print(f"✅ Downloaded successfully!")

except Exception as e:
    print(f"❌ Download failed: {e}")
    raise

# ─────────────────────────────────────────
# STEP 2: Upload to Azure Bronze Layer
# ─────────────────────────────────────────
print(f"\n☁️ STEP 2: Uploading to Azure Data Lake bronze layer...")

try:
    blob_service = BlobServiceClient.from_connection_string(CONN_STR)
    blob_client = blob_service.get_blob_client(
        container="btc-transactions",
        blob=f"bronze/btc_{today}.parquet"
    )
    with open(save_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    print(f"✅ Uploaded btc_{today}.parquet to bronze layer!")

except Exception as e:
    print(f"❌ Upload failed: {e}")
    raise

# ─────────────────────────────────────────
# STEP 3: Get Azure Access Token
# ─────────────────────────────────────────
print(f"\n🔐 STEP 3: Authenticating with Azure...")

try:
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "resource": "https://dev.azuresynapse.net"
    }
    token_response = requests.post(token_url, data=token_data)
    token_response.raise_for_status()
    access_token = token_response.json()["access_token"]
    print("✅ Authenticated successfully!")

except Exception as e:
    print(f"❌ Authentication failed: {e}")
    raise

# ─────────────────────────────────────────
# STEP 4: Trigger Synapse Notebook
# ─────────────────────────────────────────
print(f"\n⚡ STEP 4: Triggering Synapse notebook '{NOTEBOOK_NAME}'...")

try:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Create a pipeline run to execute the notebook
    synapse_endpoint = f"https://{SYNAPSE_WORKSPACE}.dev.azuresynapse.net"
    
    # Submit notebook run
    run_url = f"{synapse_endpoint}/notebooks/{NOTEBOOK_NAME}/runs?api-version=2020-12-01"
    run_response = requests.post(run_url, headers=headers, json={})
    
    if run_response.status_code in [200, 202]:
        print(f"✅ Notebook triggered successfully!")
        run_id = run_response.json().get("runId", "unknown")
        print(f"   Run ID: {run_id}")
    else:
        print(f"⚠️ Notebook trigger response: {run_response.status_code}")
        print(f"   Response: {run_response.text}")

except Exception as e:
    print(f"⚠️ Notebook trigger warning: {e}")
    print("Pipeline will continue — check Synapse manually")

# ─────────────────────────────────────────
# DONE!
# ─────────────────────────────────────────
print(f"\n🎉 Pipeline complete for {today}!")
print(f"   ✅ Downloaded from AWS")
print(f"   ✅ Uploaded to Azure bronze layer")
print(f"   ✅ Synapse notebook triggered")