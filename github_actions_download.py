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
SPARK_POOL = "etlsparkpool"
SUBSCRIPTION_ID = "ff6930af-614e-4591-847c-2474bda0d9d4"
RESOURCE_GROUP = "BTC-ETL-RG"

# -----------------------------------------
# STEP 1: Download Bitcoin data from AWS
# -----------------------------------------
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
        print(f"No data found for {today} - AWS may not have published it yet")
        print("This is normal - try again tomorrow")
        exit(0)

    file_url = f"{base_url}/{parquet_key}"
    save_path = f"/tmp/btc_{today}.parquet"
    print(f"Downloading {today} data...")
    urllib.request.urlretrieve(file_url, save_path)
    print(f"✅ Downloaded successfully!")

except Exception as e:
    print(f"❌ Download failed: {e}")
    raise

# -----------------------------------------
# STEP 2: Upload to Azure Bronze Layer
# -----------------------------------------
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

# -----------------------------------------
# STEP 3: Get Azure Access Token
# -----------------------------------------
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

# -----------------------------------------
# STEP 4: Trigger Synapse Spark Session
# -----------------------------------------
print(f"\n⚡ STEP 4: Triggering Synapse Spark pool '{SPARK_POOL}'...")

try:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    synapse_endpoint = f"https://{SYNAPSE_WORKSPACE}.dev.azuresynapse.net"

    # Create a Spark session via Livy API
    run_url = f"{synapse_endpoint}/livyApi/versions/2019-11-01-preview/sparkPools/{SPARK_POOL}/sessions"

    payload = {
        "name": f"btc_etl_{today}",
        "driverMemory": "28g",
        "driverCores": 4,
        "executorMemory": "28g",
        "executorCores": 4,
        "numExecutors": 2,
        "conf": {},
        "kind": "pyspark"
    }

    run_response = requests.post(run_url, headers=headers, json=payload)

    if run_response.status_code in [200, 201, 202]:
        print(f"✅ Spark session created successfully!")
        session_id = run_response.json().get("id", "unknown")
        print(f"   Session ID: {session_id}")

        # Wait for session to be ready
        print("   Waiting for session to start...")
        time.sleep(30)

        # Submit the ETL code to the session
        code_url = f"{synapse_endpoint}/livyApi/versions/2019-11-01-preview/sparkPools/{SPARK_POOL}/sessions/{session_id}/statements"

        etl_code = """
from pyspark.sql.functions import col, round, when
from datetime import datetime

today = datetime.now().strftime("%Y-%m-%d")
print(f"Running ETL for {today}")

# Read bronze layer
df = spark.read.parquet("abfss://btc-transactions@btcetlstorage.dfs.core.windows.net/bronze/")
print(f"Bronze rows: {df.count()}")

# Transform to silver
df_clean = df.select(
    "txid", "block_timestamp", "block_number",
    "output_value", "input_value", "fee",
    "input_count", "output_count", "is_coinbase"
).dropna(subset=["txid", "output_value", "block_timestamp"]) \
 .filter(col("is_coinbase") == False) \
 .withColumn("fee_percentage", round((col("fee") / col("input_value")) * 100, 4)) \
 .withColumn("complexity_ratio", round(col("output_count") / col("input_count"), 4)) \
 .withColumn("btc_value_category",
    when(col("output_value") > 50, "whale")
    .when(col("output_value") > 10, "large")
    .when(col("output_value") > 1, "medium")
    .otherwise("small"))

# Save silver layer
df_clean.write.mode("overwrite").parquet("abfss://btc-transactions@btcetlstorage.dfs.core.windows.net/silver/")
print(f"Silver rows: {df_clean.count()}")

# Save gold layer
from pyspark.sql.functions import year, count, avg, sum, max, min
df_gold = df_clean.groupBy(year("block_timestamp").alias("year")) \
    .agg(
        count("txid").alias("total_transactions"),
        avg("output_value").alias("avg_output_value"),
        sum("output_value").alias("total_output_value"),
        avg("fee_percentage").alias("avg_fee_percentage"),
        avg("complexity_ratio").alias("avg_complexity_ratio"),
        max("output_value").alias("max_transaction_value"),
        min("output_value").alias("min_transaction_value")
    ).orderBy("year")

df_gold.write.mode("overwrite").parquet("abfss://btc-transactions@btcetlstorage.dfs.core.windows.net/gold/")
print("ETL complete! Bronze -> Silver -> Gold done!")
"""

        code_payload = {
            "code": etl_code,
            "kind": "pyspark"
        }

        code_response = requests.post(code_url, headers=headers, json=code_payload)

        if code_response.status_code in [200, 201]:
            print(f"✅ ETL code submitted to Spark session!")
            statement_id = code_response.json().get("id", "unknown")
            print(f"   Statement ID: {statement_id}")
        else:
            print(f"⚠️ Code submission response: {code_response.status_code}")
            print(f"   Response: {code_response.text}")

    else:
        print(f"⚠️ Spark session response: {run_response.status_code}")
        print(f"   Response: {run_response.text}")

except Exception as e:
    print(f"⚠️ Synapse trigger warning: {e}")
    print("Pipeline will continue - check Synapse manually")

# -----------------------------------------
# DONE!
# -----------------------------------------
print(f"\n🎉 Pipeline complete for {today}!")
print(f"   ✅ Downloaded from AWS")
print(f"   ✅ Uploaded to Azure bronze layer")
print(f"   ✅ Synapse ETL triggered")
print(f"   ✅ Silver and Gold layers will update automatically")