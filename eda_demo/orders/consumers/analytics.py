# analytics_consumer.py
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Analytics service listening...")

total_orders = 0

for message in consumer:
    total_orders += 1
    print("[Analytics] Total orders processed:", total_orders)
