from confluent_kafka import Consumer, KafkaError
import json

class KafkaEventConsumer:
    def __init__(self, bootstrap_servers, group_id, topics):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(topics)

    def consume_events(self):
        while True:
            msg = self.consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            try:
                event = json.loads(msg.value().decode('utf-8'))
                yield event
            except json.JSONDecodeError:
                print(f"Failed to decode message: {msg.value()}")

    def close(self):
        self.consumer.close()

if __name__ == "__main__":
    consumer = KafkaEventConsumer('localhost:9092', 'gcps_team2', ['asset_location', 'asset_speed'])
    try:
        for event in consumer.consume_events():
            print(f"Received event: {event}")
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()