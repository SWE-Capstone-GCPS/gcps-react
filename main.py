from consumer import KafkaEventConsumer
from processor import DataProcessor
import json

def insert_into_database(processed_event):
    # This is a placeholder function. Replace with actual database insertion code.
    print(f"Inserting into database: {json.dumps(processed_event, default=str)}")

def main():
    consumer = KafkaEventConsumer('localhost:9092', 'gcps_team2', ['asset_location', 'asset_speed'])
    processor = DataProcessor()

    print("Starting main application...")
    try:
        for event in consumer.consume_events():
            processed_event, error = processor.process_event(event)
            if error:
                print(f"Error processing event: {error}")
            else:
                print(f"Successfully processed event: {processed_event}")
                insert_into_database(processed_event)
    except KeyboardInterrupt:
        print("Application interrupted. Shutting down...")
    finally:
        consumer.close()
        print("Application shut down complete.")

if __name__ == "__main__":
    main()