from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import urllib.request
import xml.etree.ElementTree as ET
import json

AZURE_CONNECTION_STRING = "YOUR_AZURE_CONNECTION_STRING_HERE"
CONTAINER_NAME = "btc-transactions"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def download_latest_btc_data(**context):
    base_url = "https://aws-public-blockchain.s3.us-east-2.amazonaws.com"
    prefix = "v1.0/btc/transactions"
    today = datetime.now().strftime("%Y-%m-%d")
    
    print(f"🔍 Looking for Bitcoin data for {today}...")
    
    list_url = f"{base_url}?prefix={prefix}/date={today}/&max-keys=5"
    
    try:
        with urllib.request.urlopen(list_url, timeout=60) as response:
            xml_data = response.read()
        
        root = ET.fromstring(xml_data)
        ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        keys = root.findall('.//s3:Key', ns)
        
        for key in keys:
            if key.text.endswith('.parquet'):
                file_url = f"{base_url}/{key.text}"
                save_path = f"/tmp/btc_{today}.parquet"
                urllib.request.urlretrieve(file_url, save_path)
                print(f"✅ Downloaded {today} data to {save_path}")
                context['ti'].xcom_push(key='file_path', value=save_path)
                context['ti'].xcom_push(key='date', value=today)
                return
        
        print(f"❌ No data found for {today}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

def upload_to_azure(**context):
    from azure.storage.blob import BlobServiceClient
    
    ti = context['ti']
    file_path = ti.xcom_pull(key='file_path', task_ids='download_btc_data')
    today = ti.xcom_pull(key='date', task_ids='download_btc_data')
    
    if not file_path:
        print("❌ No file to upload")
        return
    
    print(f"☁️ Uploading {today} data to Azure...")
    
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(
        container=CONTAINER_NAME,
        blob=f"btc_{today}.parquet"
    )
    
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
    print(f"✅ Uploaded btc_{today}.parquet to Azure Data Lake!")

def send_to_kafka(**context):
    from kafka import KafkaProducer
    import pandas as pd
    
    ti = context['ti']
    file_path = ti.xcom_pull(key='file_path', task_ids='download_btc_data')
    today = ti.xcom_pull(key='date', task_ids='download_btc_data')
    
    if not file_path:
        print("❌ No file to stream")
        return
    
    print(f"📤 Streaming {today} transactions to Kafka...")
    
    producer = KafkaProducer(
        bootstrap_servers='host.docker.internal:9092',
        value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
    )
    
    df = pd.read_parquet(file_path)
    sent = 0
    
    for _, row in df.iterrows():
        message = {
            "txid": str(row.get("txid", "")),
            "block_timestamp": str(row.get("block_timestamp", "")),
            "output_value": float(row["output_value"]) if pd.notna(row.get("output_value")) else 0.0,
            "fee": float(row["fee"]) if pd.notna(row.get("fee")) else 0.0,
            "is_coinbase": bool(row.get("is_coinbase", False))
        }
        producer.send('btc-transactions', value=message)
        sent += 1
    
    producer.flush()
    print(f"✅ Streamed {sent} transactions to Kafka!")

with DAG(
    'btc_etl_pipeline',
    default_args=default_args,
    description='Bitcoin ETL Pipeline - Fully Connected',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='download_btc_data',
        python_callable=download_latest_btc_data,
        provide_context=True
    )

    task2 = PythonOperator(
        task_id='upload_to_azure',
        python_callable=upload_to_azure,
        provide_context=True
    )

    task3 = PythonOperator(
        task_id='send_to_kafka',
        python_callable=send_to_kafka,
        provide_context=True
    )

    task1 >> task2 >> task3