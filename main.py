import os
import asyncio
import json
import random
import time
import logging
from confluent_kafka import Producer, Consumer, KafkaError
from datetime import datetime

# Import the DataProcessor and DatabaseManager
from processor import DataProcessor
from database-manager import DatabaseManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'Asset_Tracking_Event'
GROUP_ID = 'gcps_team2'

# MySQL configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'gcpsteam2',
    'password': 'GcpsTeam2@2024',
    'database': 'bus_monitoring'
}

class BusTrackingSystem:
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})
        self.consumer = Consumer({
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'group.id': GROUP_ID,
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([TOPIC])
        self.data_processor = DataProcessor()
        self.db_manager = DatabaseManager(DB_CONFIG)
        self.routes = {
            'BUS-001': [
                {'lat': 33.9562, 'lng': -83.9879},  # Lawrenceville Square
                {'lat': 33.9584, 'lng': -83.9925},  # W Crogan St
                {'lat': 33.9619, 'lng': -84.0024},  # GA-20 W
                {'lat': 33.9704, 'lng': -84.0270},  # Buford Dr NW
                {'lat': 33.9736, 'lng': -84.0718},  # Pleasant Hill Rd
                {'lat': 33.9696, 'lng': -84.0947},  # Duluth Hwy
                {'lat': 33.9592, 'lng': -84.1118},  # Duluth Town Green
            ],
            'BUS-003': [
                {'lat': 34.0515, 'lng': -84.0712},  # Suwanee Town Center
                {'lat': 34.0598, 'lng': -84.0741},  # Lawrenceville-Suwanee Rd
                {'lat': 34.0805, 'lng': -84.0778},  # I-85 N
                {'lat': 34.1205, 'lng': -84.0044},  # I-985 N
                {'lat': 34.1207, 'lng': -83.9911},  # Buford Dr NE
                {'lat': 34.1207, 'lng': -83.9911},  # Buford Town Center
            ],
            'BUS-004': [
                {'lat': 33.9412, 'lng': -84.2135},  # Norcross City Hall
                {'lat': 33.9470, 'lng': -84.2179},  # Buford Hwy NE
                {'lat': 33.9613, 'lng': -84.2245},  # Jimmy Carter Blvd
                {'lat': 33.9695, 'lng': -84.2336},  # Peachtree Industrial Blvd
                {'lat': 33.9695, 'lng': -84.2336},  # Peachtree Corners Circle
                {'lat': 33.9695, 'lng': -84.2336},  # Peachtree Corners Town Center
            ],
            'BUS-005': [
                {'lat': 33.9887, 'lng': -83.8977},  # Dacula City Hall
                {'lat': 33.9933, 'lng': -83.8915},  # Dacula Rd
                {'lat': 34.0070, 'lng': -83.8698},  # GA-8 E
                {'lat': 34.0161, 'lng': -83.8327},  # GA-324 E
                {'lat': 34.0190, 'lng': -83.8285},  # Auburn Rd
                {'lat': 34.0190, 'lng': -83.8285},  # Auburn City Hall
            ],
        }

    def delivery_report(self, err, msg):
        if err is not None:
            logging.error(f'Message delivery failed: {err}')
        else:
            logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def interpolate_position(self, start, end, progress):
        return {
            'lat': start['lat'] + (end['lat'] - start['lat']) * progress,
            'lng': start['lng'] + (end['lng'] - start['lng']) * progress,
        }

    def generate_bus_event(self, bus_id, route_progress):
        route = self.routes[bus_id]
        segment_index = int(route_progress * (len(route) - 1))
        segment_progress = (route_progress * (len(route) - 1)) % 1

        start = route[segment_index]
        end = route[segment_index + 1] if segment_index < len(route) - 1 else route[0]
        current_position = self.interpolate_position(start, end, segment_progress)

        return {
            'Event_ID': f"{bus_id}_{int(time.time() * 1000)}",
            'Vehicle_ID': bus_id,
            'Event_Type': 'Asset_Tracking_Event',
            'Happened_At_Time': int(time.time() * 1000),
            'Latitude': current_position['lat'],
            'Longitude': current_position['lng'],
            'ECU_Speed_MPH': random.uniform(10, 40),
            'GPS_Time': int(time.time() * 1000)
        }

    def produce_event(self, event):
        self.producer.produce(TOPIC, json.dumps(event).encode('utf-8'), callback=self.delivery_report)
        self.producer.poll(0)

    async def produce_events(self):
        route_duration = 300  # 5 minutes for a full route cycle
        while True:
            for bus_id in self.routes.keys():
                route_progress = (time.time() % route_duration) / route_duration
                event = self.generate_bus_event(bus_id, route_progress)
                self.produce_event(event)
            await asyncio.sleep(1)  # Produce events every second

    async def consume_events(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info('Reached end of partition')
                else:
                    logging.error(f'Consumer error: {msg.error()}')
            else:
                try:
                    event = json.loads(msg.value().decode('utf-8'))
                    processed_event, error = self.data_processor.process_event(event)
                    if error:
                        logging.error(f"Error processing event: {error}")
                    else:
                        self.db_manager.insert_event(processed_event)
                except json.JSONDecodeError:
                    logging.error(f'Failed to decode message: {msg.value()}')
            await asyncio.sleep(0.1)  # Small delay to prevent busy-waiting

    async def run(self):
        producer_task = asyncio.create_task(self.produce_events())
        consumer_task = asyncio.create_task(self.consume_events())
        await asyncio.gather(producer_task, consumer_task)

if __name__ == "__main__":
    bus_tracking_system = BusTrackingSystem()
    asyncio.run(bus_tracking_system.run())