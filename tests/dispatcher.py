import pytest
import os
from unittest.mock import MagicMock, patch
from blissdata.redis_engine.store import DataStore
from blissdata.schemas.scan_info import DeviceDict, ChannelDict
import datetime

from bluesky_blissdata import BlissdataDispatcher
from bluesky_blissdata.dispatcher import ExceptionHandler

@pytest.fixture
def dispatcher():
    redis_host = os.getenv('redis_host', 'localhost')
    redis_port = int(os.getenv('redis_port', '6379'))
    with patch('blissdata.redis_engine.store.DataStore') as MockDataStore:
        mock_data_store = MagicMock()
        MockDataStore.return_value = mock_data_store
        return BlissdataDispatcher(host=redis_host, port=redis_port)

def test_initialization(dispatcher):
    assert dispatcher

def test_prepare_scan(dispatcher):
    doc = {
        "plan_name": "test_plan",
        "scan_id": 1,
        "uid": "12345",
        "time": datetime.datetime.now().timestamp(),
        "detectors": ["det1"],
        "motors": ["motor1"],
        "num_points": 10,
        "plan_args": {"args": [0, 1, 2, 3, 4, 5]},
        "data_policy": "test_policy",
    }
    dispatcher.prepare_scan(doc)


def test_config_datastream(dispatcher):
    doc = {
        "data_keys": {
            "det1": {"object_name": "det1", "shape": [], "precision": 4},
            "motor1": {"object_name": "motor1", "shape": [], "precision": 4}
        }
    }
    dispatcher.devices = {
            "timer": DeviceDict(name="timer", channels=[], metadata={}),
            "counters": DeviceDict(name="counters", channels=[], metadata={}),
            "axis": DeviceDict(name="axis", channels=[], metadata={}),
        }
    dispatcher.motors = ["motor1"]
    dispatcher.dets = ["det1"]
    dispatcher.scan = MagicMock()
    dispatcher.config_datastream(doc)
    assert "det1" in dispatcher.channels
    assert "motor1" in dispatcher.channels

def test_push_datastream(dispatcher):
    doc = {
        "data": {"det1": 1.0, "motor1": 2.0},
        "time": datetime.datetime.now().timestamp()
    }
    dispatcher.stream_list = {
        "det1": MagicMock(),
        "motor1": MagicMock(),
        "time": MagicMock()
    }
    dispatcher.push_datastream(doc)
    dispatcher.stream_list["det1"].send.assert_called_with(1.0)
    dispatcher.stream_list["motor1"].send.assert_called_with(2.0)
    dispatcher.stream_list["time"].send.assert_called_with(doc["time"])

def test_stop_datastream(dispatcher):
    doc = {
        "time": datetime.datetime.now().timestamp(),
        "exit_status": "success",
        "num_events": 10,
        "reason": "test"
    }
    dispatcher.stream_list = {
        "det1": MagicMock(),
        "motor1": MagicMock(),
        "time": MagicMock()
    }
    dispatcher.scan = MagicMock()
    dispatcher.scan.info = {
        "end_time": datetime.datetime.now().timestamp(),
        "exit_status": "success",
        "num_events": 10,
        "reason": "test"
    }
    dispatcher.stop_datastream(doc)
    for stream in dispatcher.stream_list.values():
        stream.seal.assert_called_once()
    dispatcher.scan.stop.assert_called_once()
    assert dispatcher.scan.info["end_time"] is not None
    assert dispatcher.scan.info["exit_status"] == "success"
    assert dispatcher.scan.info["num_events"] == 10
    assert dispatcher.scan.info["reason"] == "test"

def test_exception_handler():
    handler = ExceptionHandler("Test error")
    with pytest.raises(RuntimeError, match="Test error"):
        handler(Exception("Test exception"))