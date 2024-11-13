import os
import mysql.connector
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DatabaseManager:
    def __init__(self):
        self.config = {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'user': os.getenv('MYSQL_USER', 'gcpsteam2'),
            'password': os.getenv('MYSQL_PASSWORD', 'GcpsTeam2@2024'),
            'database': os.getenv('MYSQL_DATABASE', 'bus_monitoring')
        }
        self.connection = mysql.connector.connect(**self.config)
        self.cursor = self.connection.cursor(dictionary=True)

    def insert_event(self, event):
        try:
            # Insert into Event table
            event_query = """
                INSERT INTO Event (
                    Event_ID, Vehicle_ID, Event_Type, Happened_At_Time,
                    Row_Modified_Time, Is_Valid, Script_Version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            event_data = (
                event['Event_ID'],
                event['Vehicle_ID'],
                event['Event_Type'],
                event['Happened_At_Time'] if isinstance(event['Happened_At_Time'], datetime) else datetime.fromtimestamp(event['Happened_At_Time'] / 1000.0),
                datetime.now(),
                True,
                '1.0'
            )
            self.cursor.execute(event_query, event_data)

            # Insert into Vehicle table
            vehicle_query = """
                INSERT INTO Vehicle (
                    Event_ID, Vehicle_ID, Vehicle_Name, Longitude, Latitude,
                    ECU_Speed_MPH, Geo_Speed_MPH, Is_ECU_Speed, Heading_Direction,
                    Address, Reverse_Geo, GPS_Time, AccuracyMeters
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            vehicle_data = (
                event_id,
                event['Vehicle_ID'],
                f"Bus-{event['Vehicle_ID'].split('-')[1]}" if '-' in event['Vehicle_ID'] else f"Bus-{event['Vehicle_ID']}",  # This will handle both formats
                event['Longitude'],
                event['Latitude'],
                event['ECU_Speed_MPH'],
                event.get('Geo_Speed_MPH', event['ECU_Speed_MPH']),
                True,
                event.get('Heading_Direction', 0.0),
                None,  # Address will be populated by geocoding service if needed
                None,  # Reverse_Geo will be populated by geocoding service if needed
                event['GPS_Time'] if isinstance(event['GPS_Time'], datetime) else datetime.fromtimestamp(event['GPS_Time'] / 1000.0),
                event.get('AccuracyMeters', 10.0)  # Default accuracy of 10 meters
            )
            self.cursor.execute(vehicle_query, vehicle_data)
            self.connection.commit()
            logging.info(f"Successfully inserted event: {event['Event_ID']}")
            
        except mysql.connector.Error as err:
            logging.error(f"Error inserting data: {err}")
            self.connection.rollback()

    def get_latest_vehicle_positions(self):
        try:
            query = """
                SELECT v.Vehicle_ID, v.Vehicle_Name, v.Latitude, v.Longitude, 
                       v.ECU_Speed_MPH, v.GPS_Time
                FROM Vehicle v
                INNER JOIN (
                    SELECT Vehicle_ID, MAX(GPS_Time) as max_gps_time
                    FROM Vehicle
                    GROUP BY Vehicle_ID
                ) latest ON v.Vehicle_ID = latest.Vehicle_ID 
                AND v.GPS_Time = latest.max_gps_time
            """
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except mysql.connector.Error as err:
            logging.error(f"Error fetching vehicle positions: {err}")
            return []

    def close(self):
        self.cursor.close()
        self.connection.close()