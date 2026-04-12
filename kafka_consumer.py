from kafka import KafkaConsumer
import json

# Connect to Kafka and subscribe to topic
consumer = KafkaConsumer(
    'btc-transactions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("✅ Consumer connected! Listening for Bitcoin transactions...")
print("-" * 60)

count = 0
for message in consumer:
    transaction = message.value
    count += 1
    
    # Print every 1000th transaction
    if count % 1000 == 0:
        print(f"📥 Received #{count}:")
        print(f"   txid:          {transaction['txid'][:20]}...")
        print(f"   timestamp:     {transaction['block_timestamp']}")
        print(f"   output_value:  {transaction['output_value']} BTC")
        print(f"   fee:           {transaction['fee']} BTC")
        print(f"   is_coinbase:   {transaction['is_coinbase']}")
        print("-" * 60)