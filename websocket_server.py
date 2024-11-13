import os
import asyncio
import websockets
import json
import mysql.connector
from mysql.connector import pooling
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'user': os.getenv('MYSQL_USER', 'gcpsteam2'),
    'password': os.getenv('MYSQL_PASSWORD', 'GcpsTeam2@2024'),
    'database': os.getenv('MYSQL_DATABASE', 'bus_monitoring')
}

class DatabaseConnection:
    def __init__(self):
        self.pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=5,
            **DB_CONFIG
        )

    # In websocket-server.py

async def get_latest_vehicle_positions(self):
    connection = self.pool.get_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        query = """
            SELECT v.Vehicle_ID, v.Vehicle_Name, v.Latitude, v.Longitude, 
                   v.ECU_Speed_MPH, v.GPS_Time, v.AccuracyMeters,
                   e.Event_Type, e.Happened_At_Time
            FROM Vehicle v
            INNER JOIN Event e ON v.Event_ID = e.Event_ID
            WHERE (v.Vehicle_ID, v.GPS_Time) IN (
                SELECT Vehicle_ID, MAX(GPS_Time) as max_gps_time
                FROM Vehicle
                GROUP BY Vehicle_ID
            )
        """
        cursor.execute(query)
        result = cursor.fetchall()
        logging.info(f"Fetched {len(result)} vehicle positions")
        return result
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        return []
    finally:
        cursor.close()
        connection.close()

db = DatabaseConnection()

async def send_vehicle_updates(websocket, path):
    try:
        while True:
            vehicle_data = await db.get_latest_vehicle_positions()
            logging.info(f"Sending data for {len(vehicle_data)} vehicles")
            await websocket.send(json.dumps(vehicle_data))
            await asyncio.sleep(1)  # Send updates every second
    except websockets.exceptions.ConnectionClosed:
        logging.info("WebSocket connection closed")
    except Exception as e:
        logging.error(f"Error in send_vehicle_updates: {e}")

async def main():
    server = await websockets.serve(
        send_vehicle_updates, 
        os.getenv('WS_HOST', 'localhost'), 
        int(os.getenv('WS_PORT', '8765'))
    )
    logging.info(f"WebSocket server started on ws://{os.getenv('WS_HOST', 'localhost')}:{os.getenv('WS_PORT', '8765')}")
    await server.wait_closed()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Server stopped")