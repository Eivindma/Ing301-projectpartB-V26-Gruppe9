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
        
    def commit(self):
        self.conn.commit()

    
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
            
        room_map = {}  
        sql = """
            SELECT id, floor, area, name 
            FROM rooms 
            ORDER BY floor, id
            """
    
        cursor.execute(sql)
        result = cursor.fetchall()
        for row in result:
            room_id, floor_level, area, name = row

            floor_list = house.get_floors()
            for floor in floor_list:
                if floor_level == floor.level:
                    room = house.register_room(floor, area, name)
                    room_map[room_id] = room
                    
        sql = """
            SELECT id, room, kind, category, supplier, product 
            FROM devices 
            ORDER BY room, id
            """
        cursor.execute(sql)
        result = cursor.fetchall()
        for row in result:
            id, room_id, kind, category, supplier, product = row
            r = room_map.get(room_id)  # use the map, not the nested loop
            if r:
                if category == "sensor":
                    house.register_device(r, Sensor(id, product, supplier, kind))
                elif category == "actuator":
                    house.register_device(r, Actuator(id, product, supplier, kind))
                        
        cursor.execute("SELECT id, state FROM states")
        for dev_id, state in cursor.fetchall():
            device = house.get_device_by_id(dev_id)
            if device and device.is_actuator():
                device.state = False if (state is None or state == 0) else state
                
        cursor.close()
        return house


    def get_latest_reading(self, sensor: Device) -> Optional[Measurement]:
        """
        Retrieves the most recent sensor reading for the given sensor if available.
        Returns None if the given object has no sensor readings.
        """
        # TODO: After loading the smarthouse, continue here
        repo = SmartHouseRepository("data/db.sql")
        cursor = repo.cursor()
        if not sensor.is_sensor():
            return None
        else:
            

            sql = """
            SELECT value, ts, unit
            FROM measurements
            WHERE device = ?
            ORDER BY ts DESC
            LIMIT 1
            """
            
            sensor_id = sensor.id
            params = [sensor_id]

            cursor.execute(sql, params)
            row = cursor.fetchone()
            cursor.close()
            
            if row is None:
                return None
            value, ts, unit = row
            return Measurement(ts, value, unit)



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
            SET state = ?
            WHERE id = ?
            """
        
        
        params = [actuator.state , actuator.id]
        

        cursor.execute(sql, params)
        
        repo.commit()
        cursor.close()

        return actuator.state
        
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
            SELECT DATE(m.ts) AS day, AVG(m.value) AS avg_temp
            FROM measurements m
            JOIN devices d ON m.device = d.id
            JOIN rooms r ON d.room = r.id
            WHERE r.name = ?
            AND m.unit = '°C'
            """
            
        params = [room.room_name]
        
        
        if from_date is not None:
            sql += " AND DATE(m.ts) >= ?"
            params.append(from_date)

        if until_date is not None:
            sql += " AND DATE(m.ts) <= ?"
            params.append(until_date)

        sql += " GROUP BY day ORDER BY day"
            
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        result = {}
        for day, avg_temp in rows:
            if avg_temp is not None:
                result[day] = round(float(avg_temp), 4)
        
                
        cursor.close()
        return result

    
    def calc_hours_with_humidity_above(self, room, date: str) -> list:
        """
        This function determines during which hours of the given day
        there were more than three measurements in that hour having a humidity measurement that is above
        the average recorded humidity in that room at that particular time.
        The result is a (possibly empty) list of number representing hours [0-23].
        """
        repo = SmartHouseRepository("data/db.sql")
        cursor = repo.cursor()
        
        
        sql = """
        WITH avg_humidity AS (
            SELECT AVG(m.value) AS avg_val
            FROM measurements m
            JOIN devices d ON m.device = d.id
            JOIN rooms r ON d.room = r.id
            WHERE r.name = ?
            AND DATE(m.ts) = ?
            AND m.unit = '%'
        )
        SELECT CAST(strftime('%H', m.ts) AS INTEGER) AS hour
        FROM measurements m
        JOIN devices d ON m.device = d.id
        JOIN rooms r ON d.room = r.id, avg_humidity
        WHERE r.name = ?
        AND DATE(m.ts) = ?
        AND m.unit = '%'
        AND m.value > avg_humidity.avg_val
        GROUP BY hour
        HAVING COUNT(*) > 3
        ORDER BY hour
        """

        cursor.execute(sql, [room.room_name, date, room.room_name, date])
        rows = cursor.fetchall()
        cursor.close()

        return [row[0] for row in rows]


        
