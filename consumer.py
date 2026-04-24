from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import psycopg2
import logging
import random
import time

logging.basicConfig(level=logging.INFO)
BATCH_SIZE  = 20
MAX_RETRIES = 4

consumer = KafkaConsumer(
    'user_events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    enable_auto_commit=False, # for manual commit after processing -> ensusre we read it and stored in DB before commiting
    group_id='event_consumers'
)

dlq_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
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

batch = []
for message in consumer:
    # time.sleep(5) # Simulate processing time
    retries = 0
    event = message.value
    
    # logging.info(f"Consumed event: {event}")
    batch.append((
        event['event_id'],
        event['user_id'],
        event['event'],
        event['device'],
        event['timestamp']
    ))
    logging.info(f"Batch size: {len(batch)}")

    if len(batch) >= BATCH_SIZE:
        while retries < MAX_RETRIES:

            logging.info(f"Processing batch of {len(batch)} events...")
            try:
                if random.random() < 0.2:  # Simulate random failure
                    logging.warning("Simulated DB failure. Retrying...")
                    raise Exception("Simulated DB failure")
                cursor.executemany(
                    """
                    INSERT INTO user_events (event_id, user_id, event, device, timestamp)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    batch
                )
                conn.commit()
                consumer.commit()  # Commit Kafka offsets after successful DB insert
                batch.clear()
                break
            except Exception as e:
                logging.error(f"Error inserting batch: {e}", exc_info=True)
                conn.rollback()
                retries += 1
                logging.info(f"Retrying batch... Attempt {retries}/{MAX_RETRIES}")

        if retries == MAX_RETRIES:
            logging.error("Max retries reached. Skipping batch.")
            dlq_producer.send('user_events_dlq', value=batch)
            batch.clear()  # Clear batch after sending to DLQ
            break
            
