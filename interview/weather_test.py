import pytest
import json
from . import weather
from typing import Any

def test_process_sample():
    """Test processing weather station sample data"""
    processor = weather.WeatherProcessor()
    
    # Test first sample
    sample1 = {
        "type": "sample",
        "stationName": "Station1",
        "timestamp": 1000,
        "temperature": 25.5
    }
    processor.process_sample(sample1)
    
    # Verify data
    assert processor.stations["Station1"]["high"] == 25.5
    assert processor.stations["Station1"]["low"] == 25.5
    assert processor.last_timestamp == 1000

    # Test multiple samples for the same station
    sample2 = {
        "type": "sample",
        "stationName": "Station1",
        "timestamp": 1001,
        "temperature": 26.0
    }
    processor.process_sample(sample2)
    
    assert processor.stations["Station1"]["high"] == 26.0
    assert processor.stations["Station1"]["low"] == 25.5
    assert processor.last_timestamp == 1001

    # Test multiple stations
    sample3 = {
        "type": "sample",
        "stationName": "Station2",
        "timestamp": 1002,
        "temperature": 20.0
    }
    processor.process_sample(sample3)
    
    assert processor.stations["Station2"]["high"] == 20.0
    assert processor.stations["Station2"]["low"] == 20.0
    assert processor.last_timestamp == 1002

    # Test temperature updates
    sample4 = {
        "type": "sample",
        "stationName": "Station2",
        "timestamp": 1003,
        "temperature": 19.0
    }
    processor.process_sample(sample4)
    
    assert processor.stations["Station2"]["high"] == 20.0
    assert processor.stations["Station2"]["low"] == 19.0
    assert processor.last_timestamp == 1003

def test_get_snapshot():
    """Test getting data snapshot"""
    processor = weather.WeatherProcessor()
    
    # Add multiple station data
    samples = [
        {
            "type": "sample",
            "stationName": "Station1",
            "timestamp": 1000,
            "temperature": 25.5
        },
        {
            "type": "sample",
            "stationName": "Station1",
            "timestamp": 1001,
            "temperature": 26.0
        },
        {
            "type": "sample",
            "stationName": "Station2",
            "timestamp": 1002,
            "temperature": 20.0
        },
        {
            "type": "sample",
            "stationName": "Station2",
            "timestamp": 1003,
            "temperature": 19.0
        }
    ]
    
    for sample in samples:
        processor.process_sample(sample)
    
    # Get snapshot and parse JSON
    snapshot_json = processor.get_snapshot()
    snapshot = json.loads(snapshot_json)
    
    # Verify snapshot format and data
    assert snapshot["type"] == "snapshot"
    assert snapshot["asOf"] == 1003
    assert "Station1" in snapshot["stations"]
    assert "Station2" in snapshot["stations"]
    
    # Verify Station1 data
    assert snapshot["stations"]["Station1"]["high"] == 26.0
    assert snapshot["stations"]["Station1"]["low"] == 25.5
    
    # Verify Station2 data
    assert snapshot["stations"]["Station2"]["high"] == 20.0
    assert snapshot["stations"]["Station2"]["low"] == 19.0

def test_reset():
    """Test reset functionality"""
    processor = weather.WeatherProcessor()
    
    # Add multiple station data
    samples = [
        {
            "type": "sample",
            "stationName": "Station1",
            "timestamp": 1000,
            "temperature": 25.5
        },
        {
            "type": "sample",
            "stationName": "Station2",
            "timestamp": 1001,
            "temperature": 20.0
        }
    ]
    
    for sample in samples:
        processor.process_sample(sample)
    
    # Reset data and parse JSON
    reset_json = processor.reset()
    reset = json.loads(reset_json)
    
    # Verify reset response
    assert reset["type"] == "reset"
    assert reset["asOf"] == 1001
    
    # Verify data is cleared
    assert len(processor.stations) == 0
    assert processor.last_timestamp == 0
    
    # Verify can add data after reset
    processor.process_sample(samples[0])
    assert len(processor.stations) == 1
    assert processor.stations["Station1"]["high"] == 25.5
    assert processor.stations["Station1"]["low"] == 25.5

def test_process_events():
    """Test event processing flow"""
    test_events = [
        # Add first station data
        {
            "type": "sample",
            "stationName": "Station1",
            "timestamp": 1000,
            "temperature": 25.5
        },
        # Get first snapshot
        {
            "type": "control",
            "command": "snapshot"
        },
        # Add second station data
        {
            "type": "sample",
            "stationName": "Station2",
            "timestamp": 1001,
            "temperature": 20.0
        },
        # Get second snapshot
        {
            "type": "control",
            "command": "snapshot"
        },
        # Reset data
        {
            "type": "control",
            "command": "reset"
        },
        # Add new data
        {
            "type": "sample",
            "stationName": "Station3",
            "timestamp": 1002,
            "temperature": 30.0
        },
        # Get final snapshot
        {
            "type": "control",
            "command": "snapshot"
        }
    ]
    
    outputs = list(weather.process_events(test_events))
    
    # Verify output count
    assert len(outputs) == 4  # Two snapshots, one reset, and one final snapshot
    
    # Parse and verify first snapshot
    snapshot1 = json.loads(outputs[0])
    assert snapshot1["type"] == "snapshot"
    assert snapshot1["asOf"] == 1000
    assert snapshot1["stations"]["Station1"]["high"] == 25.5
    assert snapshot1["stations"]["Station1"]["low"] == 25.5
    
    # Parse and verify second snapshot
    snapshot2 = json.loads(outputs[1])
    assert snapshot2["type"] == "snapshot"
    assert snapshot2["asOf"] == 1001
    assert snapshot2["stations"]["Station1"]["high"] == 25.5
    assert snapshot2["stations"]["Station1"]["low"] == 25.5
    assert snapshot2["stations"]["Station2"]["high"] == 20.0
    assert snapshot2["stations"]["Station2"]["low"] == 20.0
    
    # Parse and verify reset response
    reset = json.loads(outputs[2])
    assert reset["type"] == "reset"
    assert reset["asOf"] == 1001
    
    # Parse and verify final snapshot
    snapshot3 = json.loads(outputs[3])
    assert snapshot3["type"] == "snapshot"
    assert snapshot3["asOf"] == 1002
    assert snapshot3["stations"]["Station3"]["high"] == 30.0
    assert snapshot3["stations"]["Station3"]["low"] == 30.0

def test_invalid_input():
    """Test invalid input handling"""
    # Test unknown message type
    with pytest.raises(ValueError) as exc_info:
        list(weather.process_events([{"type": "unknown"}]))
    assert "Please verify input" in str(exc_info.value)
    
    # Test unknown command (only when we have sample data)
    with pytest.raises(ValueError) as exc_info:
        list(weather.process_events([
            {
                "type": "sample",
                "stationName": "Station1",
                "timestamp": 1000,
                "temperature": 25.5
            },
            {
                "type": "control",
                "command": "unknown"
            }
        ]))
    assert "Unknown command" in str(exc_info.value)
    
    # Test missing required fields
    with pytest.raises(ValueError) as exc_info:
        list(weather.process_events([{
            "type": "sample",
            "stationName": "Station1"
            # Missing timestamp and temperature
        }]))
    assert "Error processing event" in str(exc_info.value)
    
    # Test invalid temperature value
    with pytest.raises(ValueError) as exc_info:
        list(weather.process_events([{
            "type": "sample",
            "stationName": "Station1",
            "timestamp": 1000,
            "temperature": "invalid"  # Temperature should be a number
        }]))
    assert "Error processing event" in str(exc_info.value)
    
    # Test invalid timestamp
    with pytest.raises(ValueError) as exc_info:
        list(weather.process_events([{
            "type": "sample",
            "stationName": "Station1",
            "timestamp": "invalid",  # Timestamp should be a number
            "temperature": 25.5
        }]))
    assert "Error processing event" in str(exc_info.value)

def test_control_messages_without_data():
    """Test that control messages are ignored when no sample data is present"""
    # Test at program start
    events = [
        {
            "type": "control",
            "command": "snapshot"
        }
    ]
    outputs = list(weather.process_events(events))
    assert len(outputs) == 0  # No output should be generated
    
    # Test after reset
    events = [
        {
            "type": "sample",
            "stationName": "Station1",
            "timestamp": 1000,
            "temperature": 25.5
        },
        {
            "type": "control",
            "command": "reset"
        },
        {
            "type": "control",
            "command": "snapshot"
        }
    ]
    outputs = list(weather.process_events(events))
    assert len(outputs) == 1  # Only reset response should be generated
