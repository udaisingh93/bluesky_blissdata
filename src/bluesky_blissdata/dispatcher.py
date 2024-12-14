#!/usr/bin/env python
import numpy as np
import logging
import datetime
from typing import Dict, List, Optional, Any
from blissdata.redis_engine.store import DataStore
from blissdata.redis_engine.encoding.numeric import NumericStreamEncoder
from blissdata.redis_engine.encoding.json import JsonStreamEncoder
from blissdata.schemas.scan_info import DeviceDict, ChainDict, ChannelDict
from blissdata.scan import Scan
import event_model

_logger = logging.getLogger(__name__)


class ExceptionHandler:
    def __init__(self, msg: str) -> None:
        self.msg = msg

    def __call__(self, e: Exception) -> None:
        _logger.error(f"{self.msg}: {e}")
        raise RuntimeError(f"{self.msg}: {e}") from e


class BlissdataDispatcher:
    _data_store: DataStore
    scan: Scan
    scan_id: Dict[str, Any] = {}
    uid: Optional[str] = None
    devices: Dict[str, DeviceDict] = {}
    dets: Optional[List[str]] = None
    motors: Optional[List[str]] = None
    start_time: Optional[str] = None
    npoints: Optional[int] = None
    count_time: Optional[int] = None
    start: Optional[List[float]] = None
    stop: Optional[List[float]] = None
    stream_list: Dict[str, Any] = {}
    acq_chain: Dict[str, ChainDict] = {}
    channels: Dict[str, ChannelDict] = {}
    catalog_data: Dict[str, Any] = {}

    def __init__(self, host: str = "localhost", port: int = 6379) -> None:
        _logger.info("Connecting to redis server")
        exception_handler = ExceptionHandler("Error in connecting to redis server")
        try:
            self._data_store = DataStore(f"redis://{host}:{port}", init_db=True)
        except OSError as e:
            exception_handler(e)
        except RuntimeError:
            try:
                self._data_store = DataStore(f"redis://{host}:{port}")
            except RuntimeError as exc:
                exception_handler(exc)

    def __call__(self, name: str, doc: Dict[str, Any]) -> None:
        self.scan_id = {
            "name": "my_scan",
            "number": 1,
            "data_policy": "no_policy",
            "session": "sim_session",
            "proposal": "blc00001",
        }

        if name == "start":
            _logger.debug("Validating start document.")
            event_model.schema_validators[event_model.DocumentNames.start].validate(doc)
            _logger.debug("Start document validated. Preparing scan.")
            self.prepare_scan(doc)
        elif name == "descriptor":
            _logger.debug("Validating descriptor document.")
            event_model.schema_validators[event_model.DocumentNames.descriptor].validate(
                doc)
            _logger.debug("Descriptor document validated. Configuring datastream.")
            self.config_datastream(doc)
        elif name == "event":
            _logger.debug("Validating event document.")
            event_model.schema_validators[event_model.DocumentNames.event].validate(doc)
            _logger.debug("Event document validated. Pushing data to datastream.")
            self._push_datastream(doc)
        elif name == "stop":
            _logger.debug("Validating stop document.")
            event_model.schema_validators[event_model.DocumentNames.stop].validate(doc)
            _logger.debug("Stop document validated. Stopping datastream.")
            self._stop_datastream(doc)

    def prepare_scan(self, doc: Dict[str, Any]) -> None:
        self.scan_id["name"] = doc.get("plan_name", self.scan_id.get("name", ""))
        self.scan_id["number"] = doc.get("scan_id", self.scan_id.get("number", 0))
        self.scan_id["data_policy"] = doc.get(
            "data_policy", self.scan_id.get("data_policy", ""))
        self.uid = doc.get("uid")
        self.catalog_data = doc.get('meta_catalog', {})
        _logger.info(f"Sending new scan data with uid {self.uid}")
        _logger.debug(f"prepare scan doc data {doc}")
        self.scan = self._data_store.create_scan(
            self.scan_id,
            info={"name": doc["plan_name"], "uid": self.uid}
        )
        self.dets = doc.get("detectors")
        self.motors = doc.get("motors")
        dt = datetime.datetime.fromtimestamp(doc["time"])
        self.start_time = dt.isoformat()
        self.npoints = doc.get("num_points", 1000)
        self.count_time = 1
        self.start = []
        self.stop = []
        if self.motors is not None and "grid" in self.scan.info.get("name").lower():
            j = 0
            for i in range(len(self.motors)):
                self.start.append(doc.get("plan_args")["args"][j + 1])
                self.stop.append(doc.get("plan_args")["args"][j + 2])
                j += 3
        self.devices: Dict[str, DeviceDict] = {
            "timer": DeviceDict(name="timer", channels=[], metadata={}),
            "counters": DeviceDict(name="counters", channels=[], metadata={}),
            "axis": DeviceDict(name="axis", channels=[], metadata={}),
        }

    def config_datastream(self, doc: Dict[str, Any]) -> None:
        ddesc_dict = {}
        self.stream_list = {}
        _logger.debug(f"Preparing datastream for {self.uid}")
        _logger.debug(f"prepare scan doc data {doc}")
        self.acq_chain: Dict[str, ChainDict] = {}
        self.channels: Dict[str, ChannelDict] = {}
        elem = {
            "name": None,
            "label": None,
            "dtype": None,
            "shape": None,
            "unit": None,
            "precision": None,
            "plot_type": 0,
        }
        for dev in doc.get("data_keys").keys():
            elem["label"] = dev
            dev = doc["data_keys"][dev]
            elem["name"] = dev.get("object_name")
            dtype = dev.get('dtype')
            if dtype == "number":
                elem["dtype"] = np.float64
            elif dtype == "integer":
                elem["dtype"] = np.int64
            elif dtype == "array":
                numpy_dtype = dev.get('numpy_dtype')
                if numpy_dtype is None:
                    elem["dtype"] = dict
                else:
                    elem["dtype"] = numpy_dtype
            else:
                elem["dtype"] = dict

            elem["shape"] = dev.get("shape", [])
            elem["precision"] = dev.get("precision", 4)

            unit = ""
            plot_axes = self.motors
            device_type = ""
            if self.motors is not None:
                if elem["name"] in self.motors:
                    device_type = "axis"
                    if "grid" in self.scan.info.get("name").lower():
                        elem["plot_type"] = 2
                        elem["plot_axes"] = plot_axes
                    else:
                        elem["plot_type"] = 1
                        elem["plot_axes"] = plot_axes
            if self.dets is not None:
                if elem["name"] in self.dets:
                    device_type = "counters"
                    elem["group"] = "scatter"
            self.devices[device_type]["channels"].append(elem["label"])
            self.channels[elem["label"]] = ChannelDict(
                device=device_type,
                dim=len(elem["shape"]),
                display_name=elem["label"],
                group="scatter",
            )
            if elem['dtype'] == dict:
                encoder = JsonStreamEncoder()
            else:
                encoder = NumericStreamEncoder(dtype=elem['dtype'], shape=elem["shape"])
            scalar_stream = self.scan.create_stream(
                elem["label"],
                encoder,
                {"unit": unit, "shape": [], "dtype": elem['dtype'], "group": "scatter"},
            )
            ddesc_dict[elem["label"]] = dict(elem)
            self.stream_list[elem["label"]] = scalar_stream
        print(ddesc_dict)
        elem["name"] = "timer"
        elem["label"] = "time"
        elem["dtype"] = np.float64
        elem["shape"] = []
        elem["precision"] = 4
        unit = "s"
        device_type = "timer"
        self.devices[device_type]["channels"].append(elem["label"])
        self.channels[elem["label"]] = ChannelDict(
            device=device_type, dim=len(elem["shape"]), display_name=elem["name"]
        )

        encoder = NumericStreamEncoder(dtype=np.float64, shape=elem["shape"])
        scalar_stream = self.scan.create_stream(
            elem["label"], encoder, info={"unit": unit, "shape": [], "dtype": "float64"}
        )
        ddesc_dict[elem["label"]] = dict(elem)
        print(ddesc_dict)
        self.stream_list[elem["label"]] = scalar_stream

        self.acq_chain["axis"] = ChainDict(
            top_master="timer",
            devices=list(self.devices.keys()),
            scalars=[
                f"{channel}"
                for device, details in self.devices.items()
                if device != "timer"
                for channel in details["channels"]
            ],
            spectra=[],
            images=[],
            master={
                "scalars": [
                    f"{channel}"
                    for device, details in self.devices.items()
                    if device == "timer"
                    for channel in details["channels"]
                ],
                "spectra": [],
                "images": [],
            },
        )
        scan_info = self.scan_info(ddesc_dict)
        self.scan.info.update(scan_info)
        self.scan.prepare()
        self.scan.start()

    def _push_datastream(self, doc: Dict[str, Any]) -> None:
        """ Pushes data to Redis streams as specified in the given document.

            This method iterates over the 'data' in the document and sends the values 
            to their corresponding Redis streams. The dtype of each stream is validated 
            against the data type of the value being pushed. If a mismatch is found, 
            a TypeError is raised. If a stream is not found, a KeyError is logged.

        Args:
            doc (Dict[str, Any]): A dictionary containing the data to be pushed to Redis. 
                               It must contain a 'data' key with stream names as keys 
                               and corresponding values to be sent to those streams. 
                               It should also contain a 'time' key to be sent to the 
                               'time' stream.

        Raises:
            KeyError: If a specified stream is not found in the stream list.
            TypeError: If the dtype of the value does not match the expected dtype of the stream.
        """
        _logger.debug(f"Pushing data to Redis for {self.uid}")
        data = doc.get("data", {})
        exception_handler = ExceptionHandler("Error pushing data to Redis")

        for k, value in data.items():
            try:
                ch_stream = self.stream_list[k]
            # Check if the dtype matches
                if np.issubdtype(ch_stream.info['dtype'], type(value)):
                    ch_stream.send(value)
                else:
                    raise TypeError(
                        f"Data type mismatch: expected {ch_stream.info['dtype']} but got {type(value)}")
            except KeyError:
                exception_handler(
                    KeyError(f"Stream {k} not found, expected dtype: {ch_stream.info['dtype']}"))
            except TypeError as e:
                exception_handler(e)

        try:
            self.stream_list["time"].send(doc["time"])
        except KeyError:
            exception_handler(KeyError("Stream 'time' not found"))
        except TypeError as e:
            exception_handler(e)

    def _stop_datastream(self, doc: Dict[str, Any]) -> None:
        """
    Stops the datastream, seals all streams, and updates the scan information.

    This method seals all active streams in the `stream_list` by calling their
    `seal()` method and stops the associated scan. It updates the scan's 
    information based on the `doc` provided, including the end time, exit status,
    number of events, and the reason for stopping the scan. The scan is then closed.

    Args:
        doc (Dict[str, Any]): A dictionary containing information about the scan 
                               stop event. It must include:
                               - 'time' (timestamp for the end of the scan),
                               - 'exit_status' (status of the scan, typically 'success' or 'failure'),
                               - 'num_events' (number of events during the scan),
                               - 'reason' (reason for the scan's stop).

    Raises:
        Exception: If an error occurs while sealing the streams or stopping the scan.
    """
        _logger.debug("Stopping datastream.")
        exception_handler = ExceptionHandler("Error sealing stream")

        for stream in self.stream_list.values():
            try:
                stream.seal()
            except Exception as e:
                exception_handler(e)

        self.scan.stop()
        self.scan.info.update({
            "end_time": datetime.datetime.fromtimestamp(doc["time"]).isoformat(),
            "exit_status": doc["exit_status"],
            "end_reason": "SUCCESS" if doc["exit_status"] == "success" else "ERROR",
            "num_events": doc["num_events"],
            "reason": doc["reason"]
        })
        self.scan.close()

    def scan_info(self, ddesc_dict: Dict[str, Any]) -> Dict[str, Any]:
        scan_info = {
            "name": self.scan.info.get("name"),
            "scan_nb": self.scan.number,
            "session_name": self.scan.session,
            "catalog_data": self.catalog_data,
            "data_policy": self.scan.data_policy,
            "start_time": self.start_time,
            "type": self.scan.info.get('name'),
            "npoints": self.npoints,
            "count_time": self.count_time,
            "title": self.scan.info.get("name") + str(self.scan.number),
            "acquisition_chain": self.acq_chain,
            "devices": self.devices,
            "channels": self.channels,
            "display_extra": {"plotselect": []},
            "plots": [],
            "start": self.start,
            "stop": self.stop,
            "user_name": "bluesky",
        }

        axes = []
        scan_info["plots"].append({"kind": "curve-plot"})

        if self.motors is not None:
            elem = ddesc_dict[self.motors[0]]
        else:
            elem = ddesc_dict["time"]
        plot_type = elem.get("plot_type", 0)
        plot_axes = elem.get("plot_axes", [])

        if plot_type == 1:
            for axis in plot_axes:
                if elem["name"] != axis:
                    axes.append({"kind": "curve", "x": axis, "y": elem["name"]})
        elif plot_type == 2:
            for axis in plot_axes:
                if elem["name"] != axis:
                    axes.append({"kind": "scatter", "x": axis, "y": elem["name"]})
        elif plot_type == 3:
            _logger.info("Image plot not implemented yet")

        if "grid" in scan_info["name"].lower():
            scan_info["plots"].append(
                {"kind": "scatter-plot", "name": "Scatter", "items": axes}
            )
        else:
            scan_info["plots"].append(
                {"kind": "curve-plot", "name": "Curve", "items": axes}
            )
        return scan_info
