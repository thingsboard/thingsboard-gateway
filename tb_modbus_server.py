import threading
from pymodbus.client.sync import ModbusTcpClient
from tb_modbus_transport_manager import TBModbusTransportManager
import logging
import os
import socket
import threading
from json import load
import logging
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.schedulers.background import BlockingScheduler

log = logging.getLogger(__name__)


class TBModbusServer():
    _POLL_PERIOD = 1000  # time in milliseconds
    _thread = None
    scheduler = None
    client = None
    _server = None

    def __init__(self, server, scheduler):
        self.scheduler = scheduler
        self._server = server
        self._dict_current_parameters = {}
        self._thread = threading.Thread(target=self._server_processor_thread)
        self._thread.daemon = True
        self._thread.start()

    def _server_processor_thread(self):
        self.client = TBModbusTransportManager(self._server["transport"])
        for device in self._server["devices"]:
            device_check_data_changed = self._get_parameter(device, "sendDataOnlyOnChange", False)
            device_attr_poll_period = self._get_parameter(device, "attributesPollPeriod", self._POLL_PERIOD)
            device_ts_poll_period = self._get_parameter(device, "timeseriesPollPeriod", self._POLL_PERIOD)

            for ts in device["timeseries"]:
                self._process_message(ts, device_ts_poll_period, "ts", device_check_data_changed, device)
            for atr in device["attributes"]:
                self._process_message(atr, device_attr_poll_period, "atr", device_check_data_changed, device)

    def _process_message(self, item, device_poll_period, type_of_data, device_check_data_changed, device):
        poll_period = self._get_parameter(item, "pollPeriod", device_poll_period) / 1000  # millis to seconds
        check_data_changed = self._get_parameter(item, "sendDataOnlyOnChange", device_check_data_changed)
        self.scheduler.add_job(self._get_values_check_send_to_tb,
                               'interval',
                               seconds=poll_period,
                               args=(check_data_changed, item, type_of_data, device))

    def _get_values_check_send_to_tb(self, check_data_changed, item, type_of_data, device):
        result = self.client.get_data_from_device(item)
        result = self.transform_ansver_to_readable_format(result, item)
        if not check_data_changed or self._check_ts_atr_changed(result, type_of_data, device, item):
            self._send_to_tb(result)

    def _send_to_tb(self, data):
        log.info(data.bits)

# todo stopped working after pymodbus, understand and fix
    def _check_ts_atr_changed(self, value, type_of_data, device, item):
        key = self._server["transport"]["host"] + "|" + str(self._server["transport"]["port"]) + "|" \
            + str(device["unitId"]) + "|" + type_of_data + "|" + str(item["address"])
        if self._dict_current_parameters.get(key) == value:
            log.debug("value didnt changed")
            return False
        else:
            log.debug("value did changed")
            self._dict_current_parameters.update({key: value})
            return True

    @staticmethod
    def _get_parameter(data, param, default_value):
        if data.get(param):
            return data.get(param)
        else:
            return default_value

    def write_to_server(self, config_file):
        with open(config_file, 'r') as f:
            log.debug("reading file")
        # connect to server if not connected
        # send data with lock?

    def transform_ansver_to_readable_format(self, ans, item):
        result = ans
        return result
