import json
from datetime import datetime

class DataProcessor:
    def process_event(self, event):
        print(f"Processing event: {event}")
        if 'type' not in event:
            return None, "Missing event type"

        if event['type'] == 'asset_location':
            return self.process_location_event(event)
        elif event['type'] == 'asset_speed':
            return self.process_speed_event(event)
        else:
            return None, f"Unknown event type: {event['type']}"

    def process_location_event(self, event):
        print("Processing location event")
        required_fields = ['assetId', 'latitude', 'longitude', 'timestamp']
        if not all(field in event for field in required_fields):
            return None, "Missing required fields in location event"

        processed_event = {
            'asset_id': event['assetId'],
            'latitude': event['latitude'],
            'longitude': event['longitude'],
            'timestamp': datetime.fromtimestamp(event['timestamp'] / 1000.0)
        }
        print(f"Processed location event: {processed_event}")
        return processed_event, None

    def process_speed_event(self, event):
        print("Processing speed event")
        required_fields = ['assetId', 'speed', 'timestamp']
        if not all(field in event for field in required_fields):
            return None, "Missing required fields in speed event"

        processed_event = {
            'asset_id': event['assetId'],
            'speed': event['speed'],
            'timestamp': datetime.fromtimestamp(event['timestamp'] / 1000.0)
        }
        print(f"Processed speed event: {processed_event}")
        return processed_event, None

if __name__ == "__main__":
    processor = DataProcessor()
    test_events = [
        {
            'type': 'asset_location',
            'assetId': '12345',
            'latitude': 33.9519,
            'longitude': -83.9921,
            'timestamp': 1623456789000
        },
        {
            'type': 'asset_speed',
            'assetId': '67890',
            'speed': 55.5,
            'timestamp': 1623456790000
        },
        {
            'type': 'unknown_type',
            'assetId': '13579'
        }
    ]
    for event in test_events:
        processed_event, error = processor.process_event(event)
        if error:
            print(f"Error processing event: {error}")
        else:
            print(f"Successfully processed event: {processed_event}")