# inventory_consumer.py
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Inventory service listening...")

for message in consumer:
    order = message.value
    print("[Inventory] Update stock for:", order)
