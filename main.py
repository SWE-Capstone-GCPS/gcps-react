from consumer import KafkaEventConsumer
from processor import DataProcessor
import mysql.connector

class DatabaseManager:
    def __init__(self, host, user, password, database):
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()

    def insert_events(self, location_event, speed_event):
        query = """
            INSERT INTO asset_events (asset_id, event_type, latitude, longitude, speed, timestamp)
            VALUES (-------)
        """ # insert values and make sure the columns match the SQL Server database
       self.cursor.execute(query, (
            location_event['asset_id'], 'location', 
            location_event['latitude'], location_event['longitude'], 
            None, location_event['timestamp']
        ))
        self.cursor.execute(query, (
            speed_event['asset_id'], 'speed',
            None, None, 
            speed_event['speed'], speed_event['timestamp']
        ))
        self.connection.commit()

    def close(self):
        self.cursor.close()
        self.connection.close()

def main():
    consumer = KafkaEventConsumer('localhost:9092', 'gcps_team2', ['asset_location', 'asset_speed'])
    processor = DataProcessor()
    db_manager = DatabaseManager('localhost', 'your_username', 'your_password', 'bus_monitoring')# need to insert server info here

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
        db_manager.close()
        print("Application shut down complete.")

if __name__ == "__main__":
    main()