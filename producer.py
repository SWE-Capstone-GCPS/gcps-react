from confluent_kafka import Producer
import json

class KafkaEventProducer:
    def __init__(self, bootstrap_servers):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers
        })

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def produce_event(self, topic, event):
        try:
            event_data = json.dumps(event).encode('utf-8')
            self.producer.produce(topic, event_data, callback=self.delivery_report)
            self.producer.poll(0)
        except Exception as e:
            print(f"Failed to produce message: {e}")
    
    def flush(self):
        self.producer.flush()

producer = KafkaEventProducer('localhost:9092')
event_data = {'event_type': 'example_event', 'event_payload': {'key': 'value'}}
producer.produce_event('gcps-events', event_data)
producer.flush()