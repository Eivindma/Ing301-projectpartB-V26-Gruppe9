import sys
import sqlite3 as sq
import datetime
import random


class Measurement():
    """
    This class represents a measurement taken from a sensor.
    """

    def __init__(self, timestamp: str, 
                 value:float, unit:str) -> None:
        self.value = value
        self.timestamp = timestamp
        self.unit = unit
        self.valueHistory = []



class State():
    def __init__(self, isOn:bool, 
                 targetTemperature:float, dimLevel:int):
        self.isOn = isOn
        self.targetTemperature = targetTemperature
        self.dimLevel = dimLevel

# TODO: Add your own classes here!

class Building:
    def __init__(self):
        self.floors : list[Floor] = []
        
    def add(self, floor: Floor):
        self.floors.append(floor)
        self.floors.sort(key=lambda f: f.level) #Sorting list after adding
        
    
    def get_floors(self):
        return self.floors
        
    

class Floor:
    def __init__(self, level : int):
        self.rooms : list[Room] = []
        self.level = level

    def get_area(self):
        return sum(room.area for room in self.rooms)
    
    def add(self, room: Room):
        
        self.rooms.append(room)
        return room
    
    def get_rooms(self):
        '''
        all_rooms = []
        for floor in self.get_floors():
            all_rooms.extend(floor.get_rooms())'''
        
        return self.rooms
    
    def __repr__(self):
        area = self.get_area()
        return f"Floor(level={self.level}, rooms={len(self.rooms)}, area={area:.1f}m²)"
        
        

class Room:
    def __init__(self, floor, area, room_name):
        self.floor = floor
        self.room_name = room_name
        self.area = area
        self.devices: list[Device] = []
        
    def add(self, device:Device):
        self.devices.append(device)
        
    def __repr__(self):
        return (f"Room(name='{self.name}', "
            f"area={self.area:.1f}m², "
            f"floor_level={self.floor.level})")
        
        

class Device():
    def __init__(self, id: str, model_name: str, supplier: str, device_type: str):
        self.id = id
        self.model_name = model_name 
        self.supplier = supplier
        self.device_type = device_type
        self.room : Optional[Room] = None
        
    def is_sensor(self):
        pass
    
    def get_device_type(self):
        pass

    
class Sensor(Device):
    def __init__(self, id,model_name, supplier, device_type,unit:str = ""):
        super().__init__(id, model_name, supplier, device_type)
        self.unit = unit
        

    def last_measurement(self):
        return Measurement(datetime.datetime.now().isoformat(), random.random() * 10, self.unit)

    def historicalMeasurement(self):
        pass
        
    def is_actuator(self):
        return False
    
    def is_sensor(self):
        return True


class Actuator(Device):
    def __init__(self, id, model_name, supplier, device_type):
        super().__init__(id, model_name, supplier, device_type)
        

    def is_actuator(self):
        return True
    
    def is_sensor(self):
        return False
    

    def get_state(self,):
        return self.state
    
    def turn_on(self, target_value: Optional[float] = None):
        if target_value:
            self.state = target_value
        else:
            self.state = True

    def turn_off(self):
        self.state = False

    def is_active(self) -> bool:
        return self.state is not False

class Actuator_with_sensor(Actuator, Sensor):

    def __init__(self, id: str, model_name: str, supplier: str, device_type: str):
        super().__init__(id, model_name, supplier, device_type)

    def is_actuator(self):
        return True
    
    def is_sensor(self):
        return True
        


class SmartHouse:
    def __init__(self):
        self.building = Building()
        
        

    """
    This class serves as the main entity and entry point for the SmartHouse system app.
    Do not delete this class nor its predefined methods since other parts of the
    application may depend on it (you are free to add as many new methods as you like, though).

    The SmartHouse class provides functionality to register rooms and floors (i.e. changing the 
    house's physical layout) as well as register and modify smart devices and their state.
    """

    def register_floor(self, level):
        """
        This method registers a new floor at the given level in the house
        and returns the respective floor object.
        """
        
        floor = Floor(level)
        self.building.add(floor)
        return floor
        


    def register_room(self, floor, room_size, room_name = None):
        """
        This methods registers a new room with the given room areal size 
        at the given floor. Optionally the room may be assigned a mnemonic name.
        """
        if not isinstance(floor, Floor):
            raise TypeError(f"Expected Floor object, got {type(floor)}")
    
        room = Room(floor, room_size, room_name)   # Create the room correctly
        floor.add(room)                            # Add it to the actual floor
        return room


    def get_floors(self):
        """
        This method returns the list of registered floors in the house.
        The list is ordered by the floor levels, e.g. if the house has 
        registered a basement (level=0), a ground floor (level=1) and a first floor 
        (leve=1), then the resulting list contains these three flors in the above order.
        """
        return sorted(self.building.floors, key=lambda f: f.level)


    def get_rooms(self):
        """
        This methods returns the list of all registered rooms in the house.
        The resulting list has no particular order.
        """
        rooms = []
        for floor in self.building.floors:
            rooms.extend(floor.rooms)
        return rooms


    def get_area(self):
        """
        This methods return the total area size of the house, i.e. the sum of the area sizes of each room in the house.
        """
        return sum(room.area for room in self.get_rooms())


    def register_device(self, room: Room, device: Device):
        """
        This methods registers a given device in a given room.
        """
        if hasattr(device, 'room') and device.room is not None:
            if device in device.room.devices:
                device.room.devices.remove(device)

        room.add(device)
        device.room = room

    
    def get_devices(self) -> List[Device]:
        """
        This method retrieves a device object via its id.
        """
        result = []
        for r in self.get_rooms():
            result.extend(r.devices)
        return result

    def get_device_by_id(self, device_id: str) -> Optional[Device]:
        """
        This method retrieves a device object via its id.
        """
        for d in self.get_devices():
            if d.id == device_id:
                return d
        return None
