from typing import Any, Iterable, Generator

class WeatherProcessor:
    """
    A class to process and manage weather station data.
    
    This class maintains a collection of weather stations and their temperature data,
    including high and low temperatures for each station. It provides methods to
    process new temperature samples, generate snapshots of the current state,
    and reset the data.
    """
    
    def __init__(self):
        """
        Initialize a new WeatherProcessor instance.
        
        Creates an empty dictionary to store station data and initializes
        the last timestamp to 0.
        """
        self.stations = {} 
        self.last_timestamp = 0

    def process_sample(self, sample: dict[str, Any]) -> None:
        """
        Process a single weather sample.
        
        Args:
            sample (dict): A dictionary containing weather data with keys:
                - stationName (str): Name of the weather station
                - temperature (float): Temperature reading
                - timestamp (int): Time of the reading
        
        Raises:
            ValueError: If station name is empty or temperature is not a number
        """
        station_name = sample['stationName']
        temperature = sample['temperature']
        timestamp = sample['timestamp']
        
        # Check if station name is empty
        if not station_name:
            raise ValueError("Station name cannot be empty")
        
        # Check if temperature is a valid number
        if not isinstance(temperature, (int, float)):
            raise ValueError("Temperature must be a number")
        
        # Update last timestamp
        self.last_timestamp = max(self.last_timestamp, timestamp)
        
        # If station doesn't exist, create new record
        if station_name not in self.stations:
            self.stations[station_name] = {
                'high': temperature,
                'low': temperature,
                'last_timestamp': timestamp
            }
        else:
            # Update existing station data
            station = self.stations[station_name]
            station['high'] = max(station['high'], temperature)
            station['low'] = min(station['low'], temperature)
            station['last_timestamp'] = timestamp

    def get_snapshot(self) -> dict[str, Any]:
        """
        Generate a snapshot of the current weather data.
        
        Returns:
            dict: A dictionary containing:
                - type (str): Always "snapshot"
                - asOf (int): Timestamp of the latest data
                - stations (dict): Dictionary of station data with high/low temperatures
        """
        return {
            "type": "snapshot",
            "asOf": self.last_timestamp,
            "stations": {
                name: {"high": data['high'], "low": data['low']}
                for name, data in self.stations.items()
            }
        }

    def reset(self) -> dict[str, Any]:
        """
        Reset all weather data and return a reset confirmation.
        
        Returns:
            dict: A dictionary containing:
                - type (str): Always "reset"
                - asOf (int): Timestamp when the reset occurred
        """
        reset_timestamp = self.last_timestamp
        self.stations.clear()
        self.last_timestamp = 0
        return {
            "type": "reset",
            "asOf": reset_timestamp
        }

def process_events(events: Iterable[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
    """
    Process a stream of weather events and control commands.
    
    Args:
        events (Iterable[dict]): An iterable of event dictionaries. Each event can be:
            - A sample event with weather data
            - A control event with commands (snapshot/reset)
    
    Yields:
        dict: Response events for control commands (snapshots and reset confirmations)
    
    Raises:
        ValueError: If an event is invalid or processing fails
    """
    processor = WeatherProcessor()
    
    for event in events:
        try:
            event_type = event.get('type')
            if event_type is None:
                raise ValueError("Event must have a type")
            
            if event_type == 'sample':
                processor.process_sample(event)
            elif event_type == 'control':
                # Only process control messages if we have sample data
                if not processor.stations:
                    continue
                    
                command = event.get('command')
                if command is None:
                    raise ValueError("Control message must have a command")
                    
                if command == 'snapshot':
                    yield processor.get_snapshot()
                elif command == 'reset':
                    yield processor.reset()
                else:
                    raise ValueError("Unknown command")
            else:
                raise ValueError("Please verify input")
                
        except ValueError as e:
            raise ValueError(f"Error processing event: {str(e)}")
        except Exception as e:
            raise ValueError(f"Error processing event: {str(e)}")