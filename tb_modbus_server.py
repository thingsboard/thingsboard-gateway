import re
import logging
from threading import Thread, Lock
from tb_modbus_transport_manager import TBModbusTransportManager as Manager
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder, BinaryPayloadBuilder
from queue import Queue, Empty
import time
import datetime
log = logging.getLogger(__name__)

# todo first try of retrieving data returns None and raises error, may be critical
# todo should we log and wrap param getting?


class TBModbusServer(Thread):
    _POLL_PERIOD = 1000  # time in milliseconds
    _RUN_TIMEOUT = 0.1  # time in seconds
    _CHARS_IN_REGISTER = 2
    scheduler = None
    client = None
    _server = None

    def __init__(self, server, scheduler, gateway, ext_id):
        super(TBModbusServer, self).__init__()
        self.ext_id = ext_id
        self.gateway = gateway
        self.devices_names = set()
        self.queue_write_to_device = Queue()
        self.scheduler = scheduler
        self._server = server
        self.lock = Lock()
        self._dict_current_parameters = {}
        self.daemon = True
        self._read_from_devices_jobs_add()
        self.start()

    def run(self):
        while True:
            try:
                item = self.queue_write_to_device.get()
                self.client.write_data_to_device(item)
            except Empty:
                time.sleep(self._RUN_TIMEOUT)

    def _read_from_devices_pack_jobs_add(self):
        self.client = Manager(self._server["transport"], self.ext_id)
        if len(self._server["devices"]) == 0:
            log.warning("there are no devices to process, extension id {id}".format(id=self.ext_id))
        for device in self._server["devices"]:
            log.debug("adding polling job for device id {id}, extension id {extid}".format(id=device["unitId"],
                                                                                           extid=self.ext_id))
            device_check_data_changed = Manager.get_parameter(device, "sendDataOnlyOnChange", False)
            device_attr_poll_period = Manager.get_parameter(device, "attributesPollPeriod", self._POLL_PERIOD)
            device_ts_poll_period = Manager.get_parameter(device, "timeseriesPollPeriod", self._POLL_PERIOD)


            # todo find params with similar timings and send them in one message
            # we do not check attributes and tss simultaneously because their periods usually differentiate drastically
            dict_ts_poll_periods = {}
            dict_atr_poll_periods = {}

            def update_poll_period_dicts_add_jobs(device_poll_period, item_type, dict_poll_period):
                for item in device[item_type]:
                    poll_period = Manager.get_parameter(item, "pollPeriod", device_poll_period) / 1000  # millis to secs

                    if poll_period not in dict_poll_period:
                        dict_poll_period.update({poll_period: [item]})
                    else:
                        dict_poll_period[poll_period].append(item)
                add_jobs_from_dict(dict_poll_period, item_type)

            def add_jobs_from_dict(dict_read_values, data_type):
                for poll_period in dict_read_values:
                    list_check_values_update = []
                    # todo check_data_changed = Manager.get_parameter(item, "sendDataOnlyOnChange", device_check_data_changed)
                    payload = None
                    # todo create payload
                    for config in dict_read_values[poll_period]:
                        list_check_values_update.append(dict_read_values[poll_period][config])
                    self.scheduler.add_job(self._get_values_pack_check_send_to_tb,
                                           'interval',
                                           seconds=poll_period,
                                           next_run_time=datetime.datetime.now(),
                                           args=(payload, list_check_values_update, data_type, device["unitId"]))

            update_poll_period_dicts_add_jobs(device_ts_poll_period, device["timeseries"], dict_ts_poll_periods)
            update_poll_period_dicts_add_jobs(device_attr_poll_period, device["attributes"], dict_atr_poll_periods)
            self.devices_names.add(device["deviceName"])

    def _get_values_pack_check_send_to_tb(self, payload, data_type, unit_id):
        pass

    def _read_from_devices_jobs_add(self):
        self.client = Manager(self._server["transport"], self.ext_id)
        if len(self._server["devices"]) == 0:
            log.warning("there are no devices to process")
        for device in self._server["devices"]:
            log.debug("adding polling job for device id {id}, extension id {ext_id}".format(id=device["unitId"],
                                                                                            ext_id=self.ext_id))
            device_check_data_changed = Manager.get_parameter(device, "sendDataOnlyOnChange", False)
            device_attr_poll_period = Manager.get_parameter(device, "attributesPollPeriod", self._POLL_PERIOD)
            device_ts_poll_period = Manager.get_parameter(device, "timeseriesPollPeriod", self._POLL_PERIOD)
            for ts in device["timeseries"]:
                self._process_message(ts, device_ts_poll_period, "ts", device_check_data_changed, device)
            for atr in device["attributes"]:
                self._process_message(atr, device_attr_poll_period, "atr", device_check_data_changed, device)
            self.devices_names.add(device["deviceName"])

    def _process_message(self, item, device_poll_period, type_of_data, device_check_data_changed, device):
        poll_period = Manager.get_parameter(item, "pollPeriod", device_poll_period) / 1000  # millis to seconds
        check_data_changed = Manager.get_parameter(item, "sendDataOnlyOnChange", device_check_data_changed)
        self.scheduler.add_job(self._get_values_check_send_to_tb,
                               'interval',
                               seconds=poll_period,
                               next_run_time=datetime.datetime.now(),
                               args=(check_data_changed, item, type_of_data, device))

    def _get_values_check_send_to_tb(self, check_data_changed, config, type_of_data, device):
        result = self.client.get_data_from_device(config, device["unitId"])
        result = self._transform_answer_to_readable_format(result, config)

        # firstly we check if we need to check data change, if true then do it
        if result and (check_data_changed or self._check_ts_atr_changed(result, type_of_data, device, config)):
            # todo this is version with only one key in data (telem/atr) update
            result = [
                {
                    "ts": int(round(time.time() * 1000)),
                    "values": {
                        config["tag"]: result
                    }
                }
            ]
            # todo should we convert time to local? do we check timezone?

            # wrap result to ts and other shit
            self.gateway.send_modbus_data_to_storage(result, type_of_data)

    def _check_ts_atr_changed(self, value, type_of_data, device, item):
        key = self._server["transport"]["host"] + "|" + str(self._server["transport"]["port"]) + "|" \
              + str(device["unitId"]) + "|" + type_of_data + "|" + str(item["address"])
        if self._dict_current_parameters.get(key) == value:
            log.debug("{type} value {val} related to device id {id} didn't change".format(type=type_of_data,
                                                                                          val=value,
                                                                                          id=device["unitId"]))
            return False
        else:
            log.debug("{type} value {val} related to device id {id} changed".format(type=type_of_data,
                                                                                    val=value,
                                                                                    id=device["unitId"]))
            self._dict_current_parameters.update({key: value})
            return True

    def write_to_device(self, config):
        log.debug(config)
        payload = self._transform_request_to_device_format(config)
        if payload:
            config.update({"payload": payload})
            with self.lock:
                self.queue_write_to_device.put(config)
        # todo if we need to respond to tb (according to old config) make this a future and return(?)

    def _transform_request_to_device_format(self, request):
        # we choose hardware type dependently of tb type and hardware registers
        # firstly we choose byte order
        bo = {"BIG": Endian.Big,
              "LITTLE": Endian.Little,
              # TODO add support for custom byte orders
              1023: None,
              "CD AB EG FH": None}
        byte_order = bo[request["byteOrder"] if request.get("byteOrder") else Endian.Big]
        builder = BinaryPayloadBuilder(byteorder=byte_order)
        # we do not use register count for something else then checking needed registers for related data type
        reg_count = Manager.get_parameter(request, "registerCount", 1)
        value = request["value"]

        # todo is it enough to log.debug error in config?
        # all values are signed, # todo should we add logic for auto choosing unsigned?
        tags = (re.findall('[A-Z][a-z]*', request["tag"]))
        if "Coil" in tags:
            builder.add_bits(value)
        elif "String" in tags:
            builder.add_string(value)
        elif "Double" in tags:
            if reg_count == 4:
                builder.add_64bit_float(value)
            else:
                log.warning("unsupported amount of registers with double type,"
                            " ext id {id}".format(id=self.ext_id))
                return None
        elif "Float" in tags:
            if reg_count == 2:
                builder.add_32bit_float(value)
            else:
                log.warning("unsupported amount of registers with float type, "
                            "extension id {id}".format(id=self.ext_id))
                return None
        elif "Integer" in tags or "DWord" in tags or "DWord/Integer" in tags:
            if reg_count == 2:
                builder.add_32bit_int(value)
            else:
                log.warning("unsupported amount of registers with integer type,"
                            " extension id {id}".format(id=self.ext_id))
                return None
        elif "Word" in tags:
            if reg_count == 1:
                builder.add_16bit_int(value)
            else:
                log.warning("unsupported amount of registers with word type,"
                            " extension id {id}".format(id=self.ext_id))
                return None
        else:
            log.warning("unsupported hardware data type, extension id {id}".format(id=self.ext_id))
        # todo now if values of config file are wrong, we just log this fact and return empty payload, is it valid?
        log.debug(request)
        # todo shound we add examination of correlation of functionCode to type? some types does not fit to some fc's
        if request["functionCode"] in [5, 6]:
            payload = builder.to_coils()
        elif request["functionCode"] == 16:
            payload = builder.to_registers()
        else:
            log.warning("Unsupported function code, extension id {id}".format(id=self.ext_id))
            return None
        return payload

    def _transform_answer_to_readable_format(self, answer, config):
        # todo can we extract logic from loop to avoid repeats?
        result = None
        # working with coils
        if config.get("functionCode") == 1 or config.get("functionCode") == 2:
            result = answer.bits
            log.debug(result)
            if "registerCount" in config:
                return result[:config["registerCount"]]
            else:
                return result[0]
        # working with registers
        elif config.get("functionCode") == 3 or config.get("functionCode") == 4:
            result = answer.registers
            byte_order = Manager.get_parameter(config, "byteOrder", "BIG")
            reg_count = Manager.get_parameter(config, "registerCount", 1)
            type_of_data = config["type"]
            decoder = BinaryPayloadDecoder.fromRegisters(result,
                                                         byteorder=byte_order)
            if type_of_data == "string":
                decoder.decode_string(TBModbusServer._CHARS_IN_REGISTER * reg_count)
            elif type_of_data == "long":
                if reg_count == 1:
                    decoder.decode_16bit_int()
                elif reg_count == 2:
                    decoder.decode_32bit_int()
                else:
                    log.warning("unsupported register count for long data type,"
                                " extension id {id}".format(id=self.ext_id))
                    return None
            elif type_of_data == "double":
                if reg_count == 2:
                    decoder.decode_32bit_float()
                elif reg_count == 4:
                    decoder.decode_64bit_float()
                else:
                    log.warning("unsupported register count for double data type,"
                                " extension id {id}".format(id=self.ext_id))
                    return None
            else:
                log.warning("unknown data type, not string, long or double,"
                            " extension id {id}".format(id=self.ext_id))
                return None
            if "bit" in config:
                if len(result) > 1:
                    log.warning("with bit parameter only one register is expected, got more then one,"
                                " extension id {id}".format(id=self.ext_id))
                    return None
                result = result[0]
                position = 15 - config["bit"]  # reverse order
                # transform result to string representation of a bit sequence, add "0" to make it longer >16
                result = "0000000000000000" + str(bin(result)[2:])
                # get length of 16, then get bit, then cast it to int(0||1 from "0"||"1", then cast to boolean)
                return bool(int((result[len(result) - 16:])[15 - position]))
        return result
