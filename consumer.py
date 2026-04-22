from kafka import KafkaConsumer
import json
import logging
logging.basicConfig(level=logging.INFO)

consumer = KafkaConsumer(
    'user_events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    enable_auto_commit=True,
)

logging.info("Starting to consume events...")
for message in consumer:
    event = message.value
    logging.info(f"Consumed event: {event}")