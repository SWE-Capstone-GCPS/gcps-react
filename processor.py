from datetime import datetime
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataProcessor:
    def __init__(self):
        self.rejected_events_logger = logging.getLogger('rejected_events')
        self.rejected_events_logger.setLevel(logging.INFO)
        rejected_handler = logging.FileHandler('rejected_events.log')
        rejected_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        self.rejected_events_logger.addHandler(rejected_handler)

    def log_rejected_event(self, event, reason):
        self.rejected_events_logger.info(f"Rejected Event: {json.dumps(event)} - Reason: {reason}")

    def process_event(self, event):
        logging.info(f"Processing event: {event}")
        if 'Event_Type' not in event:
            self.log_rejected_event(event, "Missing event type")
            return None, "Missing event type"

        if event['Event_Type'] == 'Asset_Tracking_Event':
            return self.process_tracking_event(event)
        else:
            self.log_rejected_event(event, f"Unknown event type: {event['Event_Type']}")
            return None, f"Unknown event type: {event['Event_Type']}"

    def process_tracking_event(self, event):
        logging.info("Processing tracking event")
        required_fields = ['Event_ID', 'Vehicle_ID', 'Latitude', 'Longitude', 'ECU_Speed_MPH', 'Happened_At_Time']
        missing_fields = [field for field in required_fields if field not in event]
        
        if missing_fields:
            self.log_rejected_event(event, f"Missing required fields: {', '.join(missing_fields)}")
            return None, f"Missing required fields: {', '.join(missing_fields)}"

        try:
            processed_event = {
                'Event_ID': event['Event_ID'],
                'Vehicle_ID': event['Vehicle_ID'],
                'Event_Type': event['Event_Type'],
                'Happened_At_Time': datetime.fromtimestamp(event['Happened_At_Time'] / 1000.0),
                'Row_Modified_Time': datetime.now(),
                'Is_Valid': True,
                'Script_Version': '1.0',
                'Vehicle_Name': f"Bus-{event['Vehicle_ID']}",
                'Longitude': float(event['Longitude']),
                'Latitude': float(event['Latitude']),
                'ECU_Speed_MPH': float(event['ECU_Speed_MPH']),
                'Geo_Speed_MPH': float(event.get('Geo_Speed_MPH', event['ECU_Speed_MPH'])),
                'Is_ECU_Speed': True,
                'Heading_Direction': float(event.get('Heading_Direction', 0.0)),
                'GPS_Time': datetime.fromtimestamp(event['GPS_Time'] / 1000.0)
            }
            logging.info(f"Processed tracking event: {processed_event}")
            return processed_event, None
        except ValueError as e:
            self.log_rejected_event(event, f"Invalid data format: {str(e)}")
            return None, f"Invalid data format: {str(e)}"

if __name__ == "__main__":
    processor = DataProcessor()
    
    # Test with a valid event
    test_event = {
        'Event_ID': 'BUS-001_1623456789000',
        'Vehicle_ID': 'BUS-001',
        'Event_Type': 'Asset_Tracking_Event',
        'Happened_At_Time': 1623456789000,
        'Latitude': 33.9562,
        'Longitude': -83.9879,
        'ECU_Speed_MPH': 35.5,
        'GPS_Time': 1623456789000
    }
    processed_event, error = processor.process_event(test_event)
    if error:
        logging.error(f"Error processing event: {error}")
    else:
        logging.info(f"Successfully processed event: {processed_event}")

    # Test with an invalid event (missing required field)
    invalid_event = {
        'Event_ID': 'BUS-002_1623456789000',
        'Vehicle_ID': 'BUS-002',
        'Event_Type': 'Asset_Tracking_Event',
        'Happened_At_Time': 1623456789000,
        'Longitude': -83.9879,
        'ECU_Speed_MPH': 35.5,
        'GPS_Time': 1623456789000
    }
    processed_event, error = processor.process_event(invalid_event)
    if error:
        logging.error(f"Error processing event: {error}")
    else:
        logging.info(f"Successfully processed event: {processed_event}")

    # Test with an unknown event type
    unknown_event = {
        'Event_ID': 'BUS-003_1623456789000',
        'Vehicle_ID': 'BUS-003',
        'Event_Type': 'Unknown_Event_Type',
        'Happened_At_Time': 1623456789000,
        'Latitude': 33.9562,
        'Longitude': -83.9879,
        'ECU_Speed_MPH': 35.5,
        'GPS_Time': 1623456789000
    }
    processed_event, error = processor.process_event(unknown_event)
    if error:
        logging.error(f"Error processing event: {error}")
    else:
        logging.info(f"Successfully processed event: {processed_event}")

print("Check the 'rejected_events.log' file for logged rejected events.")