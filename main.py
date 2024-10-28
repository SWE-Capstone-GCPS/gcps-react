from consumer import KafkaEventConsumer
from processor import DataProcessor
import mysql.connector
from datetime import datetime

class DatabaseManager:
    def __init__(self, host, user, password, database):
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()

    def insert_events(self, tracking_event):
        query = """
            INSERT INTO Event_Instances (Event_ID, Asset_ID, Event_Type, 
            Happened_At_Time, Row_Modified_Time, Is_Valid, Script_Version)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        self.cursor.execute(query, (
            tracking_event['Event_ID'],
            tracking_event['Asset_ID'],
            tracking_event['Event_Type'],
            tracking_event['Happened_At_Time'],
            datetime.now(),  # Row_Modified_Time
            tracking_event['Is_Valid'],
            tracking_event['Script_Version']
        ))
        self.connection.commit()

    def close(self):
        self.cursor.close()
        self.connection.close()

def main():
    consumer = KafkaEventConsumer('localhost:9092', 'gcps_team2', ['asset_location', 'asset_speed'])
    processor = DataProcessor()
    db_manager = DatabaseManager('localhost', 'your_username', 'your_password', 'your_database_name')

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
                    
                    # Create a tracking event for location
                    location_tracking_event = {
                        'Event_ID': f"{asset_id}_location_{location_event['timestamp'].strftime('%Y%m%d%H%M%S')}",
                        'Asset_ID': asset_id,
                        'Event_Type': 'asset_location',
                        'Happened_At_Time': location_event['timestamp'],
                        'Is_Valid': 1,
                        'Script_Version': '1.0'
                    }
                    db_manager.insert_events(location_tracking_event)
                    
                    # Create a tracking event for speed
                    speed_tracking_event = {
                        'Event_ID': f"{asset_id}_speed_{speed_event['timestamp'].strftime('%Y%m%d%H%M%S')}",
                        'Asset_ID': asset_id,
                        'Event_Type': 'asset_speed',
                        'Happened_At_Time': speed_event['timestamp'],
                        'Is_Valid': 1,
                        'Script_Version': '1.0'
                    }
                    db_manager.insert_events(speed_tracking_event)
                    
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