import json
from datetime import datetime

class DataProcessor:
    def process_event(self, event):
        if 'type' not in event:
            return None, "Missing event type"

        if event['type'] == 'asset_location':
            return self.process_location_event(event)
        elif event['type'] == 'asset_speed':
            return self.process_speed_event(event)
        else:
            return None, f"Unknown event type: {event['type']}"

    def process_location_event(self, event):
        required_fields = ['assetId', 'latitude', 'longitude', 'timestamp']
        if not all(field in event for field in required_fields):
            return None, "Missing required fields in location event"

        processed_event = {
            'asset_id': event['assetId'],
            'latitude': event['latitude'],
            'longitude': event['longitude'],
            'timestamp': datetime.fromtimestamp(event['timestamp'] / 1000.0)
        }
        return processed_event, None

    def process_speed_event(self, event):
        required_fields = ['assetId', 'speed', 'timestamp']
        if not all(field in event for field in required_fields):
            return None, "Missing required fields in speed event"

        processed_event = {
            'asset_id': event['assetId'],
            'speed': event['speed'],
            'timestamp': datetime.fromtimestamp(event['timestamp'] / 1000.0)
        }
        return processed_event, None

# Usage
processor = DataProcessor()
event = {
    'type': 'asset_location',
    'assetId': '12345',
    'latitude': 33.9519,
    'longitude': -83.9921,
    'timestamp': 1623456789000
}
processed_event, error = processor.process_event(event)
if error:
    print(f"Error processing event: {error}")
else:
    print(f"Processed event: {processed_event}")