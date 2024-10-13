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

        try:
            processed_event = {
                'asset_id': event['assetId'],
                'latitude': float(event['latitude']),
                'longitude': float(event['longitude']),
                'timestamp': datetime.fromtimestamp(int(event['timestamp']) / 1000.0)
            }
            return processed_event, None
        except ValueError as e:
            return None, f"Invalid data format in location event: {str(e)}"

    def process_speed_event(self, event):
        required_fields = ['assetId', 'speed', 'timestamp']
        if not all(field in event for field in required_fields):
            return None, "Missing required fields in speed event"

        try:
            processed_event = {
                'asset_id': event['assetId'],
                'speed': float(event['speed']),
                'timestamp': datetime.fromtimestamp(int(event['timestamp']) / 1000.0)
            }
            return processed_event, None
        except ValueError as e:
            return None, f"Invalid data format in speed event: {str(e)}"

if __name__ == "__main__":
    processor = DataProcessor()
    test_events = [
        {
            'type': 'asset_location',
            'assetId': 'BUS-1234',
            'latitude': 33.9519,
            'longitude': -83.9921,
            'timestamp': 1623456789000
        },
        {
            'type': 'asset_speed',
            'assetId': 'BUS-5678',
            'speed': 45.5,
            'timestamp': 1623456790000
        },
        {
            'type': 'unknown_event',
            'assetId': 'BUS-9012'
        }
    ]

    for event in test_events:
        processed_event, error = processor.process_event(event)
        if error:
            print(f"Error processing event: {error}")
        else:
            print(f"Processed event: {processed_event}")