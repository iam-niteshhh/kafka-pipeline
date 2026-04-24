from kafka import KafkaProducer
import json
import logging
logging.basicConfig(level=logging.INFO)
import random
import time
import uuid

producer  = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

events = ["click", "view", "purchase", "sell"]
devices = ["mobile", "desktop", "tablet"]

logging.info("Starting to produce events...")

while True:
    data = {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1000),
        "event": random.choice(events),
        "device": random.choice(devices),
        "timestamp": time.time()
    }
    producer.send('user_events', value=data)
    logging.info(f"Produced event: {data}")
    time.sleep(random.randint(1, 5))  # Simulate delay between events
