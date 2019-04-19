import threading

from pymodbus.client.sync import ModbusTcpClient
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
    _TIMEOUT = 3000
    _POLL_PERIOD = 1000
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
        if self._server["transport"]["type"] == "tcp":
            self.client = ModbusTcpClient(self._server["transport"]["host"],
                                          timeout=self._get_parameter(self._server["transport"],
                                                                      "timeout",
                                                                      self._TIMEOUT) / 1000)
            self.client.connect()
            self._dict_functions = {
                1: self.client.read_coils,
                2: self.client.read_discrete_inputs,
                3: self.client.read_holding_registers,
                4: self.client.read_input_registers
            }

            for device in self._server["devices"]:
                device_check_data_changed = self._get_parameter(device, "sendDataOnlyOnChange", False)
                device_attr_poll_period = self._get_parameter(device, "attributesPollPeriod", self._POLL_PERIOD)
                device_ts_poll_period = self._get_parameter(device, "timeseriesPollPeriod", self._POLL_PERIOD)

                for ts in device["timeseries"]:
                    self._process_message_tcp(ts, device_ts_poll_period, "ts", device_check_data_changed, device)
                for atr in device["attributes"]:
                    self._process_message_tcp(atr, device_attr_poll_period, "atr", device_check_data_changed, device)
        if self._server["transport"]["type"] == "SMTh else":
            # todo add other transport types
            pass

    def _process_message_tcp(self, item, device_poll_period, type_of_data, device_check_data_changed, device):
        poll_period = self._get_parameter(item, "pollPeriod", device_poll_period) / 1000
        check_data_changed = self._get_parameter(item, "sendDataOnlyOnChange", device_check_data_changed)
        self.scheduler.add_job(self._get_values_check_send_to_tb,
                               'interval',
                               seconds=poll_period,
                               args=(check_data_changed, item, type_of_data, device))

    def _get_values_check_send_to_tb(self, check_data_changed, item, type_of_data, device):
        bit = self._get_parameter(item, "bit", 0)

        # todo куда биты деваются и как их использвоать? из конфига
        #log.debug(self.item["functionCode"])
        result = self.client.read_coils(item["address"], 8, unit=device["unitId"])

        # result = self._dict_functions[self.item["functionCode"](self.item["address"],
        #                                                        self._get_parameter(self.item,
        #                                                                            "registerCount",
        #                                                                            1))]

        # todo make class or function for result transforming to one format
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
