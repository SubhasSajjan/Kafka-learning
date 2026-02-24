from kafka import KafkaProducer
import json
import os


producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def publish_order_event(order_data):
    print("Publishing order event to Kafka:", order_data)
    producer.send("order_created", order_data)
    producer.flush()