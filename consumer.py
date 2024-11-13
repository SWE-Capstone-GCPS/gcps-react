import os
from confluent_kafka import Consumer, KafkaError
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'Asset_Tracking_Event')
GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'gcps_team2')

class KafkaConsumer:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'group.id': GROUP_ID,
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([TOPIC])

    def consume_events(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info('Reached end of partition')
                    continue
                else:
                    logging.error(f'Consumer error: {msg.error()}')
                    break
            try:
                event = json.loads(msg.value().decode('utf-8'))
                yield event
            except json.JSONDecodeError:
                logging.error(f'Failed to decode message: {msg.value()}')

    def close(self):
        self.consumer.close()

if __name__ == "__main__":
    consumer = KafkaConsumer(BOOTSTRAP_SERVERS, GROUP_ID, [TOPIC])
    try:
        for event in consumer.consume_events():
            logging.info(f"Received event: {event}")
    except KeyboardInterrupt:
        logging.info("Stopping consumer...")
    finally:
        consumer.close()