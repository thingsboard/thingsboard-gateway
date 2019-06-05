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
    _server_config = None
    # dict_bo = {"A": 0, "B": 1, "C": 2, "D": 3, "E": 4, "F": 5, "G": 6, "H": 7}
    # WC = {"b": 1, "h": 2, "i": 4, "l": 4, "q": 8, "f": 4, "d": 8}

    def __init__(self, server, scheduler, gateway, ext_id):
        super(TBModbusServer, self).__init__()
        self.ext_id = ext_id
        self.gateway = gateway
        self.devices_names = set()
        self.queue_write_to_device = Queue()
        self.scheduler = scheduler
        self._server_config = server
        self.lock = Lock()
        self._dict_current_parameters = {}
        self.daemon = True
        self._read_from_devices_jobs_add()
        self.start()

    def run(self):
        while True:
            t = 0
            try:
                t = time.time()
                item = self.queue_write_to_device.get()
                resp = self.client.write_data_to_device(item)
                # todo here we can reply to rpc with id (add id to config)

            except Empty:
                t = time.time() - t
                if t < self._RUN_TIMEOUT:
                    time.sleep(self._RUN_TIMEOUT - t)

    def _read_from_devices_jobs_add(self):
        self.client = Manager(self._server_config["transport"], self.ext_id)
        if len(self._server_config["devices"]) == 0:
            log.warning("there are no devices to process")
        for device in self._server_config["devices"]:
            log.debug("adding polling job for device id {id}, extension id {ext_id}".format(id=device["unitId"],
                                                                                            ext_id=self.ext_id))
            device_check_data_changed = Manager.get_parameter(device, "sendDataOnlyOnChange", False)
            device_attr_poll_period = Manager.get_parameter(device, "attributesPollPeriod", self._POLL_PERIOD)
            device_ts_poll_period = Manager.get_parameter(device, "timeseriesPollPeriod", self._POLL_PERIOD)
            device_name = device["deviceName"]
            for ts in device["timeseries"]:
                self._process_message(ts, device_ts_poll_period, "tms", device_check_data_changed, device)
            for atr in device["attributes"]:
                self._process_message(atr, device_attr_poll_period, "atr", device_check_data_changed, device)
            self.devices_names.add(device_name)
            rpc_handlers = device.get("rpc")
            self.gateway.on_device_connected(device_name, self.write_to_device, rpc_handlers)

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
        if result is not None and (not check_data_changed or self._check_ts_atr_changed(result, type_of_data, device,
                                                                                        config)):
            # todo add attributes!

            result = {
                    "values": {
                        config["tag"]: result
                    }
                }

            if type_of_data == "tms":
                result.update({
                    "ts": int(round(time.time() * 1000))
                    })

            # todo should we convert time to local? do we check timezone?
            self.gateway.send_data_to_storage(result, type_of_data, device["deviceName"])

    def _check_ts_atr_changed(self, value, type_of_data, device, item):
        key = self._server_config["transport"]["host"] + "|" + str(self._server_config["transport"]["port"]) + "|" \
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

    def _transform_request_to_device_format(self, request):
        # def pack_words(fstring, value):
        #     value = pack("!{}".format(fstring), value)
        #     wc = self.WC.get(fstring.lower()) // 2
        #     up = "!{}H".format(wc)
        #     payload = unpack(up, value)
        #
        #     if byte_order == "LITTLE":
        #         payload = list(reversed(payload))
        #         fstring = Endian.Little + "H"
        #     elif byte_order == "BIG":
        #         fstring = Endian.Big + "H"
        #     else:
        #         # todo add custom wordorder logic
        #         fstring = Endian.Big + "H"
        #
        #         input_payload = payload[0]
        #         result_payload = bytearray(len(input_payload))
        #         for item in range(len(input_payload)):
        #             result_payload[byte_order[item]] = input_payload[item]
        #     payload = [pack(fstring, word) for word in payload]
        #     payload = b''.join(payload)
        #     return payload

        # we choose hardware type dependently of tb type and hardware registers
        # firstly we choose byte order
        byte_order = request["byteOrder"] if request.get("byteOrder") else "LITTLE"
        if byte_order == "LITTLE":
            builder = BinaryPayloadBuilder(byteorder=Endian.Little)
        elif byte_order == "BIG":
            builder = BinaryPayloadBuilder(byteorder=Endian.Big)
        else:
            log.warning("byte order is not BIG or LITTLE")
            return
            # todo implement custom byteorders
            # byte_order = byte_order.replace(" ", "")
            # if not byte_order[0].isdigit():
            #     byte_order = list(map(lambda letter: self.dict_bo[letter], byte_order))
            # builder = BinaryPayloadBuilder(byteorder=Endian.Little, wordorder=Endian.Big)
        # we do not use register count for something else then checking needed registers for related data type
        reg_count = Manager.get_parameter(request, "registerCount", 1)
        value = request["value"]
        # all values are signed
        tags = (re.findall('[A-Z][a-z]*', request["tag"]))
        if "Coil" in tags:
            builder.add_bits(value)
        elif "String" in tags:
            # todo add wordorder for string? problem is last char if len is even,
            #  so we need to add one space to make wordorder
            builder.add_string(value)
        elif "Double" in tags:
            if reg_count == 4:
                # p_string = pack_words("d", value)
                # builder._payload.append(p_string)
                builder.add_64bit_float(value)
            else:
                log.warning("unsupported amount of registers with double type,"
                            " ext id {id}".format(id=self.ext_id))
                return
        elif "Float" in tags:
            if reg_count == 2:
                # p_string = pack_words("f", value)
                # builder._payload.append(p_string)
                builder.add_32bit_float(value)
            else:
                log.warning("unsupported amount of registers with float type, "
                            "extension id {id}".format(id=self.ext_id))
                return
        elif "Integer" in tags or "DWord" in tags or "DWord/Integer" in tags or "Word" in tags:
            if reg_count == 1:
                # p_string = pack_words("h", value)
                # builder._payload.append(p_string)
                builder.add_16bit_int(value)
            elif reg_count == 2:
                # p_string = pack_words("i", value)
                # builder._payload.append(p_string)
                builder.add_32bit_int(value)
            elif reg_count == 4:
                # p_string = pack_words("q", value)
                # builder._payload.append(p_string)
                builder.add_64bit_int(value)
            else:
                log.warning("unsupported amount of registers with integer/word/dword type,"
                            " extension id {id}".format(id=self.ext_id))
                return
        else:
            log.warning("unsupported hardware data type, extension id {id}".format(id=self.ext_id))
        # todo now if values of config file are wrong, we just log this fact and return empty payload, is it valid?
        log.debug(request)
        # todo shound we add examination of correlation of functionCode to type? some types does not fit to some fc's

        if request["functionCode"] in [5, 6]:
            return builder.to_coils()
        elif request["functionCode"] == 16:
            return builder.to_registers()
        else:
            log.warning("Unsupported function code, extension id {id}".format(id=self.ext_id))
            return

    def _transform_answer_to_readable_format(self, answer, config):
        # def unpack_words(fstring, value):
        #     #todo add "default" fstring value handling
        #     value = value.encode()
        #     wc = self.WC.get(fstring.lower()) // 2
        #     up = "!{}H".format(wc)
        #     value = unpack(up, value)
        #     if decoder._wordorder == Endian.Little:
        #         value = list(reversed(value))
        #     elif decoder._wordorder == Endian.Big:
        #         pass
        #     else:
        #         # todo implement decoding
        #         pass
        #
        #     # Repack as unsigned Integer
        #     pk = decoder._byteorder + 'H'
        #     value = [pack(pk, p) for p in value]
        #     value = b''.join(value)
        #     return value
        # todo can we extract logic from loop to avoid repeats?
        result = None
        # working with coils
        if config.get("functionCode") == 1 or config.get("functionCode") == 2:
            result = answer.bits
            log.debug(result)
            if "registerCount" in config:
                result = result[:config["registerCount"]]
            else:
                result = result[0]
        # working with registers
        elif config.get("functionCode") == 3 or config.get("functionCode") == 4:
            result = answer.registers
            byte_order = Manager.get_parameter(config, "byteOrder", "LITTLE")
            reg_count = Manager.get_parameter(config, "registerCount", 1)
            type_of_data = config["type"]
            if byte_order == "LITTLE":
                decoder = BinaryPayloadDecoder.fromRegisters(result, byteorder=Endian.Little)
            elif byte_order == "BIG":
                decoder = BinaryPayloadDecoder.fromRegisters(result, byteorder=Endian.Big)
            else:
                log.warning("byte order is not BIG or LITTLE")
                return
            # if byte_order == "default":
            #     decoder = BinaryPayloadDecoder.fromRegisters(result,
            #                                                  byteorder=Endian.Little,
            #                                                  wordorder=Endian.Big)
            # elif byte_order == "LITTLE":
            #     decoder = BinaryPayloadDecoder.fromRegisters(result,
            #                                                  byteorder=Endian.Little,
            #                                                  wordorder=Endian.Little)
            # elif byte_order == "BIG":
            #     decoder = BinaryPayloadDecoder.fromRegisters(result,
            #                                                  byteorder=Endian.Big,
            #                                                  wordorder=Endian.Big)
            # else:
            #     decoder = BinaryPayloadDecoder.fromRegisters(result,
            #                                                  byteorder=Endian.Little,
            #                                                  wordorder=Endian.Big)
            if type_of_data == "string":
                result = decoder.decode_string(TBModbusServer._CHARS_IN_REGISTER * reg_count)
            elif type_of_data == "long":
                if reg_count == 1:
                    # result = unpack_words(byte_order + "h", result[0])
                    # result = unpack(decoder._byteorder, result)[0]
                    result = decoder.decode_16bit_int()
                elif reg_count == 2:
                    result = decoder.decode_32bit_int()
                elif reg_count == 4:
                    result = decoder.decode_64bit_int()
                else:
                    log.warning("unsupported register count for long data type,"
                                " extension id {id}".format(id=self.ext_id))
                    return
            elif type_of_data == "double":
                if reg_count == 2:
                    result = decoder.decode_32bit_float()
                elif reg_count == 4:
                    result = decoder.decode_64bit_float()
                else:
                    log.warning("unsupported register count for double data type,"
                                " extension id {id}".format(id=self.ext_id))
                    return
            else:
                log.warning("unknown data type, not string, long or double,"
                            " extension id {id}".format(id=self.ext_id))
                return
            if "bit" in config:
                if len(result) > 1:
                    log.warning("with bit parameter only one register is expected, got more then one,"
                                " extension id {id}".format(id=self.ext_id))
                    return
                result = result[0]
                position = 15 - config["bit"]  # reverse order
                # transform result to string representation of a bit sequence, add "0" to make it longer >16
                result = "0000000000000000" + str(bin(result)[2:])
                # get length of 16, then get bit, then cast it to int(0||1 from "0"||"1", then cast to boolean)
                return bool(int((result[len(result) - 16:])[15 - position]))
        return result
