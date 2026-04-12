import pandas as pd
import glob
import json
import time
from kafka import KafkaProducer

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
)

print("✅ Connected to Kafka!")

# Read all parquet files
files = glob.glob("data/*.parquet")
print(f"📂 Found {len(files)} parquet files")

# Read and stream each file
total_sent = 0
for file in files:
    df = pd.read_parquet(file)
    
    for _, row in df.iterrows():
        # Convert row to dictionary
        message = {
            "txid": str(row["txid"]),
            "block_timestamp": str(row["block_timestamp"]),
            "output_value": float(row["output_value"]) if pd.notna(row["output_value"]) else 0.0,
            "input_value": float(row["input_value"]) if pd.notna(row["input_value"]) else 0.0,
            "fee": float(row["fee"]) if pd.notna(row["fee"]) else 0.0,
            "input_count": int(row["input_count"]) if pd.notna(row["input_count"]) else 0,
            "output_count": int(row["output_count"]) if pd.notna(row["output_count"]) else 0,
            "is_coinbase": bool(row["is_coinbase"])
        }
        
        # Send to Kafka topic
        producer.send('btc-transactions', value=message)
        total_sent += 1
        
        # Print progress every 1000 messages
        if total_sent % 1000 == 0:
            print(f"📤 Sent {total_sent} transactions to Kafka...")
            time.sleep(0.1)  # small delay to not overwhelm Kafka

producer.flush()
print(f"\n✅ Done! Total transactions sent to Kafka: {total_sent}")