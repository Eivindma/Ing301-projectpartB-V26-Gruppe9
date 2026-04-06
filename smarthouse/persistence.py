from pathlib import Path
import sys
sys.path.append(str(Path().parent.absolute()))

import sqlite3
from typing import Optional
from smarthouse.domain import *

class SmartHouseRepository:
    """
    Provides the functionality to persist and load a _SmartHouse_ object 
    in a SQLite database.
    """

    def __init__(self, file: str) -> None:
        self.file = file 
        self.conn = sqlite3.connect(file, check_same_thread=False)

    def __del__(self):
        self.conn.close()

    def cursor(self) -> sqlite3.Cursor:
        """
        Provides a _raw_ SQLite cursor to interact with the database.
        When calling this method to obtain a cursors, you have to 
        rememeber calling `commit/rollback` and `close` yourself when
        you are done with issuing SQL commands.
        """
        return self.conn.cursor()

    def reconnect(self):
        self.conn.close()
        self.conn = sqlite3.connect(self.file)

    
    def load_smarthouse_deep(self):
        """
        This method retrives the complete single instance of the _SmartHouse_ 
        object stored in this database. The retrieval yields a _deep_ copy, i.e.
        all referenced objects within the object structure (e.g. floors, rooms, devices) 
        are retrieved as well. 
        """
        # TODO: START here! remove the following stub implementation and implement this function 
        #       by retrieving the data from the database via SQL `SELECT` statements.
        house = SmartHouse()
        repo = SmartHouseRepository("data/db.sql")
        cursor = repo.cursor()
        cursor.execute("SELECT DISTINCT floor FROM rooms ORDER BY floor")
        for (floor_level,) in cursor.fetchall():
            house.register_floor(floor_level)
    
        cursor.execute("""
            SELECT id, floor, area, name 
            FROM rooms 
            ORDER BY floor, id
            """)
        result = cursor.fetchall()
        for row in result:
            room_id, floor_level, area, name = row

            floor_list = house.get_floors
            if floor_level
        # Get the actual Floor object
            floor_obj = next((f for f in house.get_floors() if f.level == floor_level), None)
            
            
        
            if floor_obj is None:
            # Safety fallback (should not happen)
                floor_obj = house.register_floor(floor_level)
        
            house.register_room(floor_obj, area, name)
            
            cursor.execute("""
            SELECT d.id, d.room, d.kind, d.supplier, d.product 
            FROM devices d
            JOIN rooms r ON d.room_id = r.id
            ORDER BY r.floor, r.id, d.id
            """)

    
        cursor.close()
        return house


    def get_latest_reading(self, sensor) -> Optional[Measurement]:
        """
        Retrieves the most recent sensor reading for the given sensor if available.
        Returns None if the given object has no sensor readings.
        """
        # TODO: After loading the smarthouse, continue here
        repo = SmartHouseRepository("data/db.sql")
        cursor = repo.cursor()

        sql = """
            SELECT 
            DATE(ts) AS day,
            AVG(value) AS avg_temp
            FROM measurements m
            JOIN devices d ON m.device = d.id
            WHERE d.room = ?
            AND m.unit = '°C'
            """
        house = SmartHouse()
        
        params = [room.room_name]

        # Add date filters only when provided
        if from_date is not None:
            sql += " AND ts >= ?"
            params.append(from_date)

        if until_date is not None:
            sql += " AND ts <= ?"
            params.append(until_date)

            sql += """
            GROUP BY day
            ORDER BY day
            """

        cursor.execute(sql, params)
        rows = cursor.fetchall()
        cursor.close()
        result = {}
        for day, avg_temp in rows:
            if avg_temp is not None:
                result[day] = round(float(avg_temp), 2)   # round to 2 decimals

        return measurement


    def update_actuator_state(self, actuator: Actuator):
        """
        Saves the state of the given actuator in the database. 
        """
        # TODO: Implement this method. You will probably need to extend the existing database structure: e.g.
        #       by creating a new table (`CREATE`), adding some data to it (`INSERT`) first, and then issue
        #       and SQL `UPDATE` statement. Remember also that you will have to call `commit()` on the `Connection`
        #       stored in the `self.conn` instance variable.
        repo = SmartHouseRepository("data/db.sql")
        cursor = repo.cursor()

        sql = """
            UPDATE states
            state = ?
            WHERE id = ?
            """
        
        
        params = [actuator.state , actuator.id]

        cursor.execute(sql, params)
        repo.commit()
        cursor.close()

        return measurement
        
        
        


    # statistics

    
    def calc_avg_temperatures_in_room(self, room : Room, from_date: Optional[str] = None, until_date: Optional[str] = None) -> dict:
        """Calculates the average temperatures in the given room for the given time range by
        fetching all available temperature sensor data (either from a dedicated temperature sensor 
        or from an actuator, which includes a temperature sensor like a heat pump) from the devices 
        located in that room, filtering the measurement by given time range.
        The latter is provided by two strings, each containing a date in the ISO 8601 format.
        If one argument is empty, it means that the upper and/or lower bound of the time range are unbounded.
        The result should be a dictionary where the keys are strings representing dates (iso format) and 
        the values are floating point numbers containing the average temperature that day.
        """
        # TODO: This and the following statistic method are a bit more challenging. Try to design the respective 
        #       SQL statements first in a SQL editor like Dbeaver and then copy it over here.  
        
        repo = SmartHouseRepository("data/db.sql")
        cursor = repo.cursor()

        sql = """
            SELECT 
            DATE(ts) AS day,
            AVG(value) AS avg_temp
            FROM measurements m
            JOIN devices d ON m.device = d.id
            WHERE d.room = ?
            AND m.unit = '°C'
            """
        params = [room.room_name]

        # Add date filters only when provided
        if from_date is not None:
            sql += " AND ts >= ?"
            params.append(from_date)

        if until_date is not None:
            sql += " AND ts <= ?"
            params.append(until_date)

            sql += """
            GROUP BY day
            ORDER BY day
            """

        cursor.execute(sql, params)
        rows = cursor.fetchall()
        cursor.close()
        result = {}
        for day, avg_temp in rows:
            if avg_temp is not None:
                result[day] = round(float(avg_temp), 2)   # round to 2 decimals

        return result

    
    def calc_hours_with_humidity_above(self, room, date: str) -> list:
        """
        This function determines during which hours of the given day
        there were more than three measurements in that hour having a humidity measurement that is above
        the average recorded humidity in that room at that particular time.
        The result is a (possibly empty) list of number representing hours [0-23].
        """
        
        '''
        # TODO: implement
        room_id = getattr(room, 'id', room)
        repo = SmartHouseRepository("data/db.sql")
        cursor = repo.cursor()

        sql = """
            SELECT 
            DATE(ts) AS day,
            TIME(ts) AS time,
            AVG(value) AS avg,
            m.value,
            d.room,
            unit
            FROM measurements m
            JOIN devices d ON m.device = d.id
            WHERE d.room = ?
            AND day = 
            AND m.unit = "%"
            """
        params = [room_id, date]

        cursor.execute(sql, params)
        rows = cursor.fetchall()
        cursor.close()
        result = {}
        for time, avg_, value in rows:
            if avg is not None:
                result[day] = round(float(avg), 2)   # round to 2 decimals

        return result
        '''
        
        repo = SmartHouseRepository("data/db.sql")
        cursor = repo.cursor()

    # ←←← THIS IS THE IMPORTANT FIX
        room_id = getattr(room, 'id', room)   # works whether room is Room object or int

        sql = """
                WITH hourly_stats AS (
                SELECT 
                strftime('%H', ts) AS hour,
                AVG(value) AS hour_avg_humidity
                FROM measurements m
                JOIN devices d ON m.device = d.id
                WHERE d.room = ?
                AND DATE(ts) = ?
                AND m.unit = '%'
                GROUP BY hour
                HAVING COUNT(*) > 3
        )
            SELECT hour
            FROM measurements m
            JOIN devices d ON m.device = d.id
            JOIN hourly_stats hs ON strftime('%H', m.ts) = hs.hour
            WHERE d.room = ?
            AND DATE(m.ts) = ?
            AND m.unit = '%'
            AND m.value > hs.hour_avg_humidity
            GROUP BY hour
            HAVING COUNT(*) > 3
            ORDER BY hour
            """

        params = [room_id, date, room_id, date]

        cursor.execute(sql, params)
        rows = cursor.fetchall()
        cursor.close()

        # Convert hour strings to integers
        result = [int(row[0]) for row in rows]

        return result


        
house = SmartHouse()

repo = SmartHouseRepository("data/db.sql")
cursor = repo.cursor()
cursor.execute("SELECT DISTINCT floor FROM rooms ORDER BY floor")
for (floor_level,) in cursor.fetchall():
    house.register_floor(floor_level)
    
    # 2. Register all rooms (now we can look up the Floor object)
    cursor.execute("""
        SELECT id, floor, area, name 
        FROM rooms 
        ORDER BY floor, id
        """)
    result = cursor.fetchall()
for row in result:
    room_id, floor_level, area, name = row
        
        # Get the actual Floor object
    floor_obj = next((f for f in house.get_floors() if f.level == floor_level), None)
        
    if floor_obj is None:
        # Safety fallback (should not happen)
        floor_obj = house.register_floor(floor_level)
        
    house.register_room(floor_obj, area, name)
    

cursor.close()

print(house.get_rooms())

