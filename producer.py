from confluent_kafka import Producer
import json
import random
import time

class KafkaEventProducerSimulator:
    def __init__(self, bootstrap_servers, num_assets=10):
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.asset_ids = [f'BUS-{random.randint(1000, 9999)}' for _ in range(num_assets)]
        self.routes = {asset_id: self.generate_route() for asset_id in self.asset_ids}
        print(f"Producer-Simulator initialized with bootstrap servers: {bootstrap_servers}")
        print(f"Simulating {num_assets} assets: {self.asset_ids}")

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def produce_event(self, topic, event):
        print(f"Producing event to topic {topic}: {event}")
        self.producer.produce(topic, json.dumps(event).encode('utf-8'), callback=self.delivery_report)
        self.producer.poll(0)

    def generate_route(self):
        # generates list of latitude, longitude pairs for bus route
        return [(random.uniform(33.5, 34.5), random.uniform(-84.5, -83.5)) for _ in range(10)]

    def produce_bus_event(self, asset_id, event_count):
        timestamp = int(time.time() * 1000)
        route = self.routes[asset_id]
        current_position = route[event_count % len(route)]  # cycle through route points

        location_event = {
            'type': 'asset_location',
            'assetId': asset_id,
            'timestamp': timestamp,
            'latitude': current_position[0],
            'longitude': current_position[1]
        }

        speed_event = {
            'type': 'asset_speed',
            'assetId': asset_id,
            'timestamp': timestamp,
            'speed': random.uniform(0, 65) 
        }

        self.produce_event('asset_location', location_event)
        self.produce_event('asset_speed', speed_event)

    def run_simulation(self, num_events, interval):
        print(f"Starting simulation: {num_events} events with {interval} second interval")
        for i in range(num_events):
            asset_id = random.choice(self.asset_ids)
            self.produce_bus_event(asset_id, i)
            time.sleep(interval)
            print(f"Produced events {i*2+1} and {i*2+2}/{num_events*2} for asset {asset_id}")
        
        self.producer.flush()
        print("Simulation complete")

if __name__ == "__main__":
    producer_simulator = KafkaEventProducerSimulator('localhost:9092', num_assets=5)
    producer_simulator.run_simulation(25, 1)