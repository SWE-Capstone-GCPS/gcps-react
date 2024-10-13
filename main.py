from consumer import KafkaEventConsumer
from processor import DataProcessor
import json

# inserts into database
def insert_into_database(processed_event):
    # need to replace with insertion code
    print(f"Inserting into database: {json.dumps(processed_event, default=str)}")

def main():
    consumer = KafkaEventConsumer('localhost:9092', 'gcps_team2', ['asset_location', 'asset_speed'])
    processor = DataProcessor()

    try:
        for event in consumer.consume_events():
            processed_event, error = processor.process_event(event)
            if error:
                print(f"Error processing event: {error}")
            else:
                print(f"Processed event: {processed_event}")
                insert_into_database(processed_event)
    except KeyboardInterrupt:
        print("Stopping application...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()