#!/usr/bin/env python
import numpy as np

from blissdata.redis_engine.store import DataStore
import os
from blissdata.redis_engine.scan import ScanState
from blissdata.redis_engine.encoding.numeric import NumericStreamEncoder
from blissdata.redis_engine.encoding.json import JsonStreamEncoder
from blissdata.schemas.scan_info import ScanInfoDict, DeviceDict, ChainDict, ChannelDict
import logging
import datetime
_logger = logging.getLogger(__name__)
# Configure blissdata for a Redis database


# The identity model we use is ESRFIdentityModel in blissdata.redis_engine.models
# It defines the json fields indexed by RediSearch


# ------------------------------------------------------------------ #
# create scan in the database

# ------------------------------------------------------------------ #

class blissdata_dispacher:
    def __init__(self,host="localhost",port=6380) :
        _logger.info("Connecting to redis sever")
        try:
            self._data_store = DataStore("redis://"+host+":"+str(port),init_db=True)
        except OSError as e:
            _logger.debug("Error in connecting to redis sever")
            raise ConnectionError(self._error_message(e))
        except RuntimeError as e:
            try: 
                self._data_store = DataStore("redis://"+host+":"+str(port))
            except RuntimeError as e:
                raise RuntimeError(self._error_message(e))
            
            
    
    def __call__(self,name,doc):
        self.scan_id = {
        "name": "my_scan",
        "number": 1,
        "data_policy": "no_policy",
        "session": "sim_session",
        "proposal": "blc00001",}
      #"collection": "sample1",
      #"dataset": "sample1_scan59"}
        
        if (name=="start"):
            self.prepare_scan(doc)
        if (name=="descriptor"):
            self.config_datastream(doc)
        if (name=="event"):
            self.push_datastream(doc)
        if (name=="stop"):
            self.stop_datastream(doc)
    def prepare_scan(self,doc):
        self.scan_id['name']=doc.get('plan_name',self.scan_id['name'])
        self.scan_id['number']=doc.get('scan_id',self.scan_id['number'])
        self.uid=doc.get('uid')
        _logger.info(f"Sending new scan data with uid {self.uid}")
        self.scan = self._data_store.create_scan(
            self.scan_id, info={"name": doc['plan_name'],"uid":self.uid})
        self.dets=doc.get('detectors')
        self.motors=doc.get('motors')
        dt = datetime.datetime.fromtimestamp(doc['time'])
        self.start_time=dt.isoformat()
        self.npoints=doc['num_points']
        self.count_time=1
        self.start=[]
        self.stop=[]
        if self.motors is not None:
            j=0
            for i in range(len(self.motors)):
                self.start.append(doc.get('plan_args')['args'][j+1])
                self.stop.append(doc.get('plan_args')['args'][j+2])
                j+=3
        self.devices: dict[str, DeviceDict] = {}
        self.devices["timer"] = DeviceDict(
            name="timer", channels=[], metadata={})
        self.devices["counters"] = DeviceDict(
            name="counters", channels=[], metadata={})
        self.devices["axis"] = DeviceDict(
            name="axis", channels=[], metadata={})
    # declare some streams in the scan
    def config_datastream(self,doc):
        ddesc_dict = {}
        self.stream_list={}
        _logger.debug(f"Preparing datastream for {self.uid}")
        self.acq_chain: dict[str, ChainDict] = {}
        self.channels: dict[str, ChannelDict] = {}
        elem={'name':None,"label":None,'dtype':None,"shape":None,"unit":None}
        for dev in doc.get("data_keys").keys():
            elem['name']=doc.get("data_keys")[dev]['object_name']
            elem['label']=dev
            elem['dtype']=np.float64
            elem['shape']=doc.get("data_keys")[dev]['shape']
            elem['precision']=doc.get("data_keys")[dev]['precision']
            unit=""
            if self.motors is not None:
                if elem['name'] in self.motors:
                    device_type="axis"
            if self.dets is not None:
                if elem['name'] in self.dets:
                    device_type = "counters"
            self.devices[device_type]['channels'].append(dev)
            self.channels[elem['label']] = ChannelDict(device=device_type,
                                               dim=len(elem['shape']),
                                               display_name=dev)
            
            encoder = NumericStreamEncoder(dtype=np.float64,shape=elem['shape'])
            scalar_stream = self.scan.create_stream(elem['label'], encoder, info={"unit": unit})
            ddesc_dict[elem['label']] =elem
            self.stream_list[elem['label']] = scalar_stream
        elem['name']="timer"
        elem['label']="time"
        elem['dtype']=np.float64
        elem['shape']=[]
        elem['precision']=4
        unit="s"
        device_type='timer'
        self.devices[device_type]['channels'].append(elem['label'])
        self.channels[elem['label']] = ChannelDict(device=device_type,
                                               dim=len(elem['shape']),
                                               display_name=elem['name'])
            
        encoder = NumericStreamEncoder(dtype=np.float64,shape=elem['shape'])
        scalar_stream = self.scan.create_stream( elem['label'], encoder, info={"unit": unit})
        ddesc_dict[elem['label']] =elem
        self.stream_list[elem['label']] = scalar_stream
        # self.scalar_stream = self.scan.create_stream("scalars", encoder, info={"unit": "mV", "embed the info": "you want"})

        # encoder = NumericStreamEncoder(dtype=np.int32, shape=(4096, ))
        # self.vector_stream = scan.create_stream("vectors", encoder, info={})

        # encoder = NumericStreamEncoder(dtype=np.uint16, shape=(1024, 100, ))
        # self.array_stream = scan.create_stream("arrays", encoder, info={})

        # encoder = JsonStreamEncoder()
        # self.json_stream = scan.create_stream("jsons", encoder, info={})
        self.acq_chain["axis"] = ChainDict(
            top_master="timer",
            devices=list(self.devices.keys()),
            scalars=[f"{device}:{channel}" for device, details in self.devices.items() if device != 'timer' for channel in details['channels']],
            spectra=[],
            images=[],
            master = { "scalars": [f"{device}:{channel}" for device, details in self.devices.items() if device == 'timer' for channel in details['channels']],
            "spectra": [],
            "images": [],})
    # gather some metadata before running the scan
        scan_info = self.scan_info(ddesc_dict)
        self.scan.info.update(scan_info)
    # ------------------------------------------------------------------ #
        self.scan.prepare() # upload initial metadata and stream declarations
    # ------------------------------------------------------------------ #
        self.scan.start()
    # Scan is ready to start, eventually wait for other processes.
    # Sharing stream keys to external publishers can be done there.
    def push_datastream(self, doc):
    # ------------------------------------------------------------------ #
    # ------------------------------------------------------------------ #
        _logger.debug(f"pushing data to redis sever for {self.uid}")
        _logger.debug(f"event doc data {doc}")
        data=doc.get('data')
        for k  in data.keys():
    
            try:
                ch_stream = self.stream_list[k]
            except KeyError:
                self.warning("Stream for {} not found".format(k))
                continue
            ch_stream.send(data[k])
            
        try:
            ch_stream = self.stream_list['time']
        except KeyError:
            self.warning("Stream for {} not found".format(k))
        # dt = datetime.datetime.fromtimestamp(doc['time'])
        ch_stream.send(doc['time'])

    # close streams
        # scalar_stream.seal()
        # vector_stream.seal()
        # array_stream.seal()
        # json_stream.seal()
    def stop_datastream(self,doc):
        _logger.debug(f"stopping doc data {doc}")
        for stream in self.stream_list.values():
            try:
                stream.seal()
            except Exception as e:
                print(e)
                self.warning(
                    "Error sealing stream {}".format(stream.name))
                continue

        self.scan.stop()
        dt = datetime.datetime.fromtimestamp(doc['time'])
        self.scan.info['end_time'] = dt.isoformat()
        self.scan.info['exit_status'] = doc['exit_status']
        self.scan.info['reason']=doc['reason']
        self.scan.info['num_events']=doc['num_events']
        self.scan.close()
    # ------------------------------------------------------------------ #
        
    # ---------------------------------------------------------------- #
    def scan_info(self, ddesc_dict):
        # filename, masterfiles, images_path = self.file_info(
        #     singleFile=self.nx_save_single_file)
        scan_info = {
            ##################################
            # Scan metadata
            ##################################
            "name": self.scan.name,
            "scan_nb": self.scan.number,
            "session_name": self.scan.session,
            "data_policy": self.scan.data_policy,
            "start_time": self.start_time,
            "type": self.scan.name,
            "npoints": self.npoints,
            "count_time": self.count_time,
            "title": self.scan.name + str(self.scan.number),
            
            ##################################
            # Device information
            ##################################
            "acquisition_chain": self.acq_chain,
            "devices": self.devices,
            "channels": self.channels,
            ##################################
            # Plot metadata
            ##################################
            "display_extra": {"plotselect": []},
            "plots": [],
            "start": self.start,
            "stop": self.stop,
            ##################################
            # NeXus writer metadata
            ##################################
            # "save": self.nexus_save,
            # "filename": filename,
            # "images_path": images_path,
            # "publisher": "test",
            # "publisher_version": "1.0",
            # "data_writer": "nexus",
            # "writer_options": {"chunk_options": {}, "separate_scan_files": False},
            # "scan_meta_categories": [
            #     "positioners",
            #     "nexuswriter",
            #     "instrument",
            #     "technique",
            #     "snapshot",
            #     "datadesc",
            # ],
            # "nexuswriter": {
            #     "devices": {},
            #     "instrument_info": {"name": "alba-"+self.scan.beamline, "name@short_name": self.scan.beamline},
            #     "masterfiles": masterfiles,
            #     "technique": {},
            # },
            # "positioners": {},  # TODO decide how to fill this (from snapshot?)
            # # TODO decide how to fill this (from instruments? env var?)
            # "instrument": {},
            # "datadesc": ddesc_dict,
            ##################################
            # Mandatory by the schema
            ##################################
            "user_name": "bluesky",  # tangosys?
        }

        scan_info["plots"].append({"kind": "curve-plot"})

        # Add curves selected in measurement group for plotting
        for elem in ddesc_dict.items():
            try:
                plot_type = elem[1].get("plot_type", 0)
                plot_axes = elem[1].get("plot_axes", [])
                name = elem[1].get("label", "")
                axes = []
                if plot_type == 1:
                    for axis in plot_axes:
                        if "<idx>" in axis:
                            axis = "#Pt No"
                        axes.append({"kind": "curve", "x": axis, "y": name})
                    scan_info["plots"].append(
                        {"kind": "curve-plot", "name": name, "items": axes})
                elif plot_type == 2:
                    self.info("Image plot not implemented yet")
            except IndexError:
                continue

        return scan_info

