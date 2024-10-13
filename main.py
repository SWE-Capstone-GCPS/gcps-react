from flask import Flask, jsonify
from kafka import KafkaConsumer
import json
import threading
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = KafkaConsumer(
    'asset_location',
    'asset_speed',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='gcps_team2',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

bus_locations = {}

def process_kafka_events():
    for message in consumer:
        event = message.value
        if 'assetId' in event:
            bus_id = event['assetId']
            if 'latitude' in event and 'longitude' in event:
                bus_locations[bus_id] = {
                    'latitude': event['latitude'],
                    'longitude': event['longitude'],
                    'speed': bus_locations.get(bus_id, {}).get('speed', 0)
                }
            elif 'speed' in event:
                if bus_id in bus_locations:
                    bus_locations[bus_id]['speed'] = event['speed']
                else:
                    bus_locations[bus_id] = {'speed': event['speed']}
        logger.info(f"Processed event: {event}")

@app.route('/api/buses')
def get_buses():
    return jsonify([
        {
            'id': bus_id,
            'latitude': bus['latitude'],
            'longitude': bus['longitude'],
            'speed': bus['speed']
        }
        for bus_id, bus in bus_locations.items()
        if 'latitude' in bus and 'longitude' in bus
    ])

if __name__ == "__main__":
    kafka_thread = threading.Thread(target=process_kafka_events)
    kafka_thread.start()
    app.run(debug=True)