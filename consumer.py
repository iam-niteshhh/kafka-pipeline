from kafka import KafkaConsumer
import json
import psycopg2
import logging
logging.basicConfig(level=logging.INFO)

consumer = KafkaConsumer(
    'user_events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    enable_auto_commit=True,
)

# Postgres Conncetion Setup
conn = psycopg2.connect(
    host="localhost",
    database="events_db",
    user="kafka",
    password="kafka",
    port=5432
)

cursor = conn.cursor()

logging.info("Consumer + DB Started...")

for message in consumer:
    event = message.value
    logging.info(f"Consumed event: {event}")

    cursor.execute(
        "INSERT INTO user_events (user_id, event, device, timestamp) VALUES (%s, %s, %s, %s)",
        (event['user_id'], event['event'], event['device'], event['timestamp'])
    )
    conn.commit()