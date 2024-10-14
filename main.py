from consumer import KafkaEventConsumer
from processor import DataProcessor
import pyodbc

class DatabaseManager:
    def __init__(self, connection_string):
        self.connection_string = connection_string

    def insert_events(self, location_event, speed_event):
        query = """
            INSERT INTO asset_events (asset_id, event_type, latitude, longitude, speed, timestamp)
            VALUES (-------)
        """ # insert values and make sure the columns match the SQL Server database
        with pyodbc.connect(self.connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, 
                    location_event['asset_id'], 'location', location_event['latitude'], location_event['longitude'], None, location_event['timestamp'],
                    speed_event['asset_id'], 'speed', None, None, speed_event['speed'], speed_event['timestamp'])
            conn.commit()

def main():
    consumer = KafkaEventConsumer('localhost:9092', 'gcps_team2', ['asset_location', 'asset_speed'])
    processor = DataProcessor()
    db_manager = DatabaseManager('DRIVER={ODBC Driver 17 for SQL Server};---------')# need to insert server info here

    print("Starting main application...")
    event_pairs = {}
    try:
        for event in consumer.consume_events():
            processed_event, error = processor.process_event(event)
            if error:
                print(f"Error processing event: {error}")
                continue

            asset_id = processed_event['asset_id']
            event_type = processed_event['type']
            
            if asset_id not in event_pairs:
                event_pairs[asset_id] = {}
            
            event_pairs[asset_id][event_type] = processed_event
            
            if len(event_pairs[asset_id]) == 2:
                location_event = event_pairs[asset_id].get('asset_location')
                speed_event = event_pairs[asset_id].get('asset_speed')
                
                if location_event and speed_event and location_event['timestamp'] == speed_event['timestamp']:
                    print(f"Inserting paired events for asset {asset_id}")
                    db_manager.insert_events(location_event, speed_event)
                    del event_pairs[asset_id]
                else:
                    print(f"Incomplete or mismatched events for asset {asset_id}. Waiting for matching event.")

    except KeyboardInterrupt:
        print("Application interrupted. Shutting down...")
    finally:
        consumer.close()
        print("Application shut down complete.")

if __name__ == "__main__":
    main()