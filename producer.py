from confluent_kafka import Producer
import json
import random
import time

class KafkaEventProducerSimulator:
    def __init__(self, bootstrap_servers):
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        print(f"Producer-Simulator initialized with bootstrap servers: {bootstrap_servers}")

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def produce_event(self, topic, event):
        print(f"Producing event to topic {topic}: {event}")
        self.producer.produce(topic, json.dumps(event).encode('utf-8'), callback=self.delivery_report)
        self.producer.poll(0)

    def generate_location_event(self):
        event = {
            'type': 'asset_location',
            'assetId': f'BUS-{random.randint(1000, 9999)}',
            'latitude': random.uniform(33.5, 34.5),  # Approximate latitude range for Gwinnett County
            'longitude': random.uniform(-84.5, -83.5),  # Approximate longitude range for Gwinnett County
            'timestamp': int(time.time() * 1000)
        }
        print(f"Generated location event: {event}")
        return event

    def generate_speed_event(self):
        event = {
            'type': 'asset_speed',
            'assetId': f'BUS-{random.randint(1000, 9999)}',
            'speed': random.uniform(0, 65),  # assuming max speed of 65 mph
            'timestamp': int(time.time() * 1000)
        }
        print(f"Generated speed event: {event}")
        return event

    def run_simulation(self, num_events, interval):
        print(f"Starting simulation: {num_events} events with {interval} second interval")
        for i in range(num_events):
            if random.choice([True, False]):
                event = self.generate_location_event()
                self.produce_event('asset_location', event)
            else:
                event = self.generate_speed_event()
                self.produce_event('asset_speed', event)
            time.sleep(interval)
            print(f"Produced event {i+1}/{num_events}")
        self.producer.flush()
        print("Simulation complete")

if __name__ == "__main__":
    producer_simulator = KafkaEventProducerSimulator('localhost:9092')
    producer_simulator.run_simulation(50, 1)  