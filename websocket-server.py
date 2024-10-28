import asyncio
import websockets
import json
import mysql.connector
from mysql.connector import pooling

class DatabaseConnection:
    def __init__(self):
        self.pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=5,
            host='localhost',
            user='your_username',
            password='your_password',
            database='bus_monitoring'
        )

    async def get_latest_bus_data(self):
        connection = self.pool.get_connection()
        cursor = connection.cursor(dictionary=True)
        try:
            query = """
                SELECT asset_id, latitude, longitude, speed, MAX(timestamp) as timestamp
                FROM bus_events
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                GROUP BY asset_id
            """
            
            cursor.execute(query)
            return cursor.fetchall()
        finally:
            cursor.close()
            connection.close()

db = DatabaseConnection()

async def send_bus_updates(websocket, path):
    try:
        while True:
            bus_data = await db.get_latest_bus_data()
            await websocket.send(json.dumps(bus_data))
            await asyncio.sleep(1)  # Send updates every second
    except websockets.exceptions.ConnectionClosed:
        pass

start_server = websockets.serve(send_bus_updates, "localhost", 8765)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()