#     Copyright 2022. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import re
import sched
import time
from copy import copy
from random import choice
from string import ascii_lowercase
from threading import Thread

from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    from can import Notifier, BufferedReader, Message, CanError, ThreadSafeBus
except ImportError:
    print("CAN library not found - installing...")
    TBUtility.install_package("python-can")
    from can import Notifier, BufferedReader, Message, CanError, ThreadSafeBus

from thingsboard_gateway.connectors.can.bytes_can_downlink_converter import BytesCanDownlinkConverter
from thingsboard_gateway.connectors.can.bytes_can_uplink_converter import BytesCanUplinkConverter
from thingsboard_gateway.connectors.connector import Connector, log


class CanConnector(Connector, Thread):
    CMD_REGEX = r"^(\d{1,2}):(\d{1,2}):?(big|little)?:(\d+)$"
    VALUE_REGEX = r"^(\d{1,2}):((?:-1)?|\d{1,2}):?(big|little)?:(bool|boolean|int|long|float|double|string|raw):?([0-9A-Za-z-_]+)?$"

    NO_CMD_ID = "no_cmd"
    UNKNOWN_ARBITRATION_ID = -1

    DEFAULT_RECONNECT_PERIOD = 30.0
    DEFAULT_POLL_PERIOD = 1.0

    DEFAULT_SEND_IF_CHANGED = False
    DEFAULT_RECONNECT_STATE = True

    DEFAULT_EXTENDED_ID_FLAG = False
    DEFAULT_FD_FLAG = False
    DEFAULT_BITRATE_SWITCH_FLAG = False

    DEFAULT_BYTEORDER = "big"
    DEFAULT_ENCODING = "ascii"

    DEFAULT_ENABLE_UNKNOWN_RPC = False
    DEFAULT_OVERRIDE_RPC_PARAMS = False
    DEFAULT_STRICT_EVAL_FLAG = True

    DEFAULT_SIGNED_FLAG = False

    DEFAULT_RPC_RESPONSE_SEND_FLAG = False

    def __init__(self, gateway, config, connector_type):
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        super().__init__()
        self.setName(config.get("name", 'CAN Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.__gateway = gateway
        self._connector_type = connector_type
        self.__bus_conf = {}
        self.__bus = None
        self.__reconnect_count = 0
        self.__reconnect_conf = {}
        self.__devices = {}
        self.__nodes = {}
        self.__commands = {}
        self.__polling_messages = []
        self.__rpc_calls = {}
        self.__shared_attributes = {}
        self.__converters = {}
        self.__bus_error = None
        self.__connected = False
        self.__stopped = True
        self.daemon = True
        self.__parse_config(config)

    def open(self):
        log.info("[%s] Starting...", self.get_name())
        self.__stopped = False
        self.start()

    def close(self):
        if not self.__stopped:
            self.__stopped = True
            log.debug("[%s] Stopping", self.get_name())

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connected

    def on_attributes_update(self, content):
        for attr_name, attr_value in content["data"].items():
            attr_config = self.__shared_attributes.get(content["device"], {}).get(attr_name)
            if attr_config is None:
                log.warning("[%s] No configuration for '%s' attribute, ignore its update", self.get_name(), attr_name)
                return

            log.debug("[%s] Processing attribute update for '%s' device: attribute=%s,value=%s",
                      self.get_name(), content["device"], attr_name, attr_value)

            # Converter expects dictionary as the second parameter so pack an attribute value to a dictionary
            data = self.__converters[content["device"]]["downlink"].convert(attr_config, {"value": attr_value})
            if data is None:
                log.error("[%s] Failed to update '%s' attribute for '%s' device: data conversion failure",
                          self.get_name(), attr_name, content["device"])
                return

            done = self.send_data_to_bus(data, attr_config, data_check=True)
            if done:
                log.debug("[%s] Updated '%s' attribute for '%s' device", self.get_name(), attr_name, content["device"])
            else:
                log.error("[%s] Failed to update '%s' attribute for '%s' device",
                          self.get_name(), attr_name, content["device"])

    def server_side_rpc_handler(self, content):
        rpc_config = self.__rpc_calls.get(content["device"], {}).get(content["data"]["method"])
        if rpc_config is None:
            if not self.__devices[content["device"]]["enableUnknownRpc"]:
                log.warning("[%s] No configuration for '%s' RPC request (id=%s), ignore it",
                            self.get_name(), content["data"]["method"], content["data"]["id"])
                return
            else:
                rpc_config = {}

        log.debug("[%s] Processing %s '%s' RPC request (id=%s) for '%s' device: params=%s",
                  self.get_name(), "pre-configured" if rpc_config else "UNKNOWN", content["data"]["method"],
                  content["data"]["id"], content["device"], content["data"].get("params"))

        if self.__devices[content["device"]]["overrideRpcConfig"]:
            if rpc_config:
                conversion_config = self.__merge_rpc_configs(content["data"].get("params", {}), rpc_config)
                log.debug("[%s] RPC request (id=%s) params and connector config merged to conversion config %s",
                          self.get_name(), content["data"]["id"], conversion_config)
            else:
                log.debug("[%s] RPC request (id=%s) will use its params as conversion config",
                          self.get_name(), content["data"]["id"])
                conversion_config = content["data"].get("params", {})
        else:
            conversion_config = rpc_config

        data = self.__converters[content["device"]]["downlink"].convert(conversion_config,
                                                                        content["data"].get("params", {}))
        if data is not None:
            done = self.send_data_to_bus(data, conversion_config, data_check=True)
            if done:
                log.debug("[%s] Processed '%s' RPC request (id=%s) for '%s' device",
                          self.get_name(), content["data"]["method"], content["data"]["id"], content["device"])
            else:
                log.error("[%s] Failed to process '%s' RPC request (id=%s) for '%s' device",
                          self.get_name(), content["data"]["method"], content["data"]["id"], content["device"])
        else:
            done = False
            log.error("[%s] Failed to process '%s' RPC request (id=%s) for '%s' device: data conversion failure",
                      self.get_name(), content["data"]["method"], content["data"]["id"], content["device"])

        if conversion_config.get("response", self.DEFAULT_RPC_RESPONSE_SEND_FLAG):
            self.__gateway.send_rpc_reply(content["device"], content["data"]["id"], {"success": done})

    def run(self):
        need_run = True
        while need_run:
            bus_notifier = None
            poller = None
            try:
                interface = self.__bus_conf["interface"]
                channel = self.__bus_conf["channel"]
                kwargs = self.__bus_conf["backend"]
                self.__bus = ThreadSafeBus(interface=interface, channel=channel, **kwargs)
                reader = BufferedReader()
                bus_notifier = Notifier(self.__bus, [reader])

                log.info("[%s] Connected to CAN bus (interface=%s,channel=%s)", self.get_name(), interface, channel)

                if self.__polling_messages:
                    poller = Poller(self)
                    # Poll once to check if network is not down.
                    # It would be better to have some kind of a ping message to check a bus state.
                    poller.poll()

                # Initialize the connected flag and reconnect count only after bus creation and sending poll messages.
                # It is expected that after these operations most likely the bus is up.
                self.__connected = True
                self.__reconnect_count = 0

                while not self.__stopped:
                    message = reader.get_message()
                    if message is not None:
                        # log.debug("[%s] New CAN message received %s", self.get_name(), message)
                        self.__process_message(message)
                    self.__check_if_error_happened()
            except Exception as e:
                log.error("[%s] Error on CAN bus: %s", self.get_name(), str(e))
            finally:
                try:
                    if poller is not None:
                        poller.stop()
                    if bus_notifier is not None:
                        bus_notifier.stop()
                    if self.__bus is not None:
                        log.debug("[%s] Shutting down connection to CAN bus (state=%s)",
                                  self.get_name(), self.__bus.state)
                        self.__bus.shutdown()
                except Exception as e:
                    log.error("[%s] Error on shutdown connection to CAN bus: %s", self.get_name(), str(e))

                self.__connected = False

                if not self.__stopped:
                    if self.__is_reconnect_enabled():
                        retry_period = self.__reconnect_conf["period"]
                        log.info("[%s] Next attempt to connect will be in %f seconds (%s attempt left)",
                                 self.get_name(), retry_period,
                                 "infinite" if self.__reconnect_conf["maxCount"] is None
                                 else self.__reconnect_conf["maxCount"] - self.__reconnect_count + 1)
                        time.sleep(retry_period)
                    else:
                        need_run = False
                        log.info("[%s] Last attempt to connect has failed. Exiting...", self.get_name())
                else:
                    need_run = False
        log.info("[%s] Stopped", self.get_name())

    def is_stopped(self):
        return self.__stopped

    def get_polling_messages(self):
        return self.__polling_messages

    def send_data_to_bus(self, data, config, data_check=True, raise_exception=False):
        try:
            self.__bus.send(Message(arbitration_id=config["nodeId"],
                                    is_extended_id=config.get("isExtendedId", self.DEFAULT_EXTENDED_ID_FLAG),
                                    is_fd=config.get("isFd", self.DEFAULT_FD_FLAG),
                                    bitrate_switch=config.get("bitrateSwitch", self.DEFAULT_BITRATE_SWITCH_FLAG),
                                    data=data,
                                    check=data_check))
            return True
        except (ValueError, TypeError) as e:
            log.error("[%s] Wrong CAN message data: %s", self.get_name(), str(e))
        except CanError as e:
            log.error("[%s] Failed to send CAN message: %s", self.get_name(), str(e))
            if raise_exception:
                raise e
            else:
                self.__on_bus_error(e)
        return False

    def __on_bus_error(self, e):
        log.warning("[%s] Notified about CAN bus error. Store it to later processing", self.get_name())
        self.__bus_error = e

    def __check_if_error_happened(self):
        if self.__bus_error is not None:
            # Maybe copying is redundant
            e = copy(self.__bus_error)
            self.__bus_error = None
            raise e

    def __merge_rpc_configs(self, rpc_params, rpc_config):
        config = {}
        options = {
            "nodeId": self.UNKNOWN_ARBITRATION_ID,
            "isExtendedId": self.DEFAULT_EXTENDED_ID_FLAG,
            "isFd": self.DEFAULT_FD_FLAG,
            "bitrateSwitch": self.DEFAULT_BITRATE_SWITCH_FLAG,
            "response": True,
            "dataLength": 1,
            "dataByteorder": self.DEFAULT_BYTEORDER,
            "dataSigned": self.DEFAULT_SIGNED_FLAG,
            "dataExpression": None,
            "dataEncoding": self.DEFAULT_ENCODING,
            "dataBefore": None,
            "dataAfter": None,
            "dataInHex": None
            }
        for option_name, option_value in options.items():
            if option_name in rpc_params:
                config[option_name] = rpc_params[option_name]
            elif option_value is not None:
                config[option_name] = rpc_config.get(option_name, option_value)
        return config

    def __process_message(self, message):
        if message.arbitration_id not in self.__nodes:
            # Too lot log messages in case of high message generation frequency
            log.debug("[%s] Ignoring CAN message. Unknown arbitration_id %d", self.get_name(), message.arbitration_id)
            return

        cmd_conf = self.__commands[message.arbitration_id]
        if cmd_conf is not None:
            cmd_id = int.from_bytes(message.data[cmd_conf["start"]:cmd_conf["start"] + cmd_conf["length"]],
                                    cmd_conf["byteorder"])
        else:
            cmd_id = self.NO_CMD_ID

        if cmd_id not in self.__nodes[message.arbitration_id]:
            log.debug("[%s] Ignoring CAN message. Unknown cmd_id %d", self.get_name(), cmd_id)
            return

        log.debug("[%s] Processing CAN message (id=%d,cmd_id=%s): %s",
                  self.get_name(), message.arbitration_id, cmd_id, message)

        parsing_conf = self.__nodes[message.arbitration_id][cmd_id]
        data = self.__converters[parsing_conf["deviceName"]]["uplink"].convert(parsing_conf["configs"], message.data)
        if data is None or not data.get("attributes", []) and not data.get("telemetry", []):
            log.warning("[%s] Failed to process CAN message (id=%d,cmd_id=%s): data conversion failure",
                        self.get_name(), message.arbitration_id, cmd_id)
            return

        self.__check_and_send(parsing_conf, data)

    def __check_and_send(self, conf, new_data):
        self.statistics['MessagesReceived'] += 1
        to_send = {"attributes": [], "telemetry": []}
        send_on_change = conf["sendOnChange"]

        for tb_key in to_send.keys():
            for key, new_value in new_data[tb_key].items():
                if not send_on_change or self.__devices[conf["deviceName"]][tb_key][key] != new_value:
                    self.__devices[conf["deviceName"]][tb_key][key] = new_value
                    to_send[tb_key].append({key: new_value})

        if to_send["attributes"] or to_send["telemetry"]:
            to_send["deviceName"] = conf["deviceName"]
            to_send["deviceType"] = conf["deviceType"]

            log.debug("[%s] Pushing to TB server '%s' device data: %s", self.get_name(), conf["deviceName"], to_send)

            self.__gateway.send_to_storage(self.get_name(), to_send)
            self.statistics['MessagesSent'] += 1
        else:
            log.debug("[%s] '%s' device data has not been changed", self.get_name(), conf["deviceName"])

    def __is_reconnect_enabled(self):
        if self.__reconnect_conf["enabled"]:
            if self.__reconnect_conf["maxCount"] is None:
                return True
            self.__reconnect_count += 1
            return self.__reconnect_conf["maxCount"] >= self.__reconnect_count
        else:
            return False

    def __parse_config(self, config):
        self.__reconnect_count = 0
        self.__reconnect_conf = {
            "enabled": config.get("reconnect", self.DEFAULT_RECONNECT_STATE),
            "period": config.get("reconnectPeriod", self.DEFAULT_RECONNECT_PERIOD),
            "maxCount": config.get("reconnectCount", None)
            }

        self.__bus_conf = {
            "interface": config.get("interface", "socketcan"),
            "channel": config.get("channel", "vcan0"),
            "backend": config.get("backend", {})
            }

        for device_config in config.get("devices"):
            is_device_config_valid = False
            device_name = device_config["name"]
            device_type = device_config.get("type", self._connector_type)
            strict_eval = device_config.get("strictEval", self.DEFAULT_STRICT_EVAL_FLAG)

            self.__devices[device_name] = {}
            self.__devices[device_name]["enableUnknownRpc"] = device_config.get("enableUnknownRpc",
                                                                                self.DEFAULT_ENABLE_UNKNOWN_RPC)
            self.__devices[device_name]["overrideRpcConfig"] = True if self.__devices[device_name]["enableUnknownRpc"] \
                else device_config.get("overrideRpcConfig", self.DEFAULT_OVERRIDE_RPC_PARAMS)

            self.__converters[device_name] = {}

            if not strict_eval:
                log.info("[%s] Data converters for '%s' device will use non-strict eval", self.get_name(), device_name)

            if "serverSideRpc" in device_config and device_config["serverSideRpc"]:
                is_device_config_valid = True
                self.__rpc_calls[device_name] = {}
                self.__converters[device_name]["downlink"] = self.__get_converter(device_config.get("converters"),
                                                                                  False)
                for rpc_config in device_config["serverSideRpc"]:
                    rpc_config["strictEval"] = strict_eval
                    self.__rpc_calls[device_name][rpc_config["method"]] = rpc_config

            if "attributeUpdates" in device_config and device_config["attributeUpdates"]:
                is_device_config_valid = True
                self.__shared_attributes[device_name] = {}

                if "downlink" not in self.__converters[device_name]:
                    self.__converters[device_name]["downlink"] = self.__get_converter(device_config.get("converters"),
                                                                                      False)
                for attribute_config in device_config["attributeUpdates"]:
                    attribute_config["strictEval"] = strict_eval
                    attribute_name = attribute_config.get("attributeOnThingsBoard") or attribute_config.get("attribute")
                    self.__shared_attributes[device_name][attribute_name] = attribute_config

            for config_key in ["timeseries", "attributes"]:
                if config_key not in device_config or not device_config[config_key]:
                    continue

                is_device_config_valid = True
                is_ts = (config_key[0] == "t")
                tb_item = "telemetry" if is_ts else "attributes"

                self.__devices[device_name][tb_item] = {}

                if "uplink" not in self.__converters[device_name]:
                    self.__converters[device_name]["uplink"] = self.__get_converter(device_config.get("converters"),
                                                                                    True)
                for msg_config in device_config[config_key]:
                    tb_key = msg_config["key"]
                    msg_config["strictEval"] = strict_eval
                    msg_config["is_ts"] = is_ts

                    node_id = msg_config.get("nodeId", self.UNKNOWN_ARBITRATION_ID)
                    if node_id == self.UNKNOWN_ARBITRATION_ID:
                        log.warning("[%s] Ignore '%s' %s configuration: no arbitration id",
                                    self.get_name(), tb_key, config_key)
                        continue

                    value_config = self.__parse_value_config(msg_config.get("value"))
                    if value_config is not None:
                        msg_config.update(value_config)
                    else:
                        log.warning("[%s] Ignore '%s' %s configuration: no value configuration",
                                    self.get_name(), tb_key, config_key, )
                        continue

                    if msg_config.get("command"):
                        cmd_config = self.__parse_command_config(msg_config["command"])
                        if cmd_config is None:
                            log.warning("[%s] Ignore '%s' %s configuration: wrong command configuration",
                                        self.get_name(), tb_key, config_key, )
                            continue

                        cmd_id = cmd_config["value"]

                        if node_id not in self.__commands:
                            self.__commands[node_id] = cmd_config
                        else:
                            prev_cmd_config = self.__commands[node_id]
                            if cmd_config["start"] != prev_cmd_config["start"] or \
                                    cmd_config["length"] != prev_cmd_config["length"] or \
                                    cmd_config["byteorder"] != prev_cmd_config["byteorder"]:
                                log.warning("[%s] Ignore '%s' %s configuration: "
                                            "another command configuration already added for arbitration_id %d",
                                            self.get_name(), tb_key, config_key, node_id)
                                continue
                    else:
                        cmd_id = self.NO_CMD_ID
                        self.__commands[node_id] = None

                    if node_id not in self.__nodes:
                        self.__nodes[node_id] = {}

                    if cmd_id not in self.__nodes[node_id]:
                        self.__nodes[node_id][cmd_id] = {
                            "deviceName": device_name,
                            "deviceType": device_type,
                            "sendOnChange": device_config.get("sendDataOnlyOnChange", self.DEFAULT_SEND_IF_CHANGED),
                            "configs": []}

                    self.__nodes[node_id][cmd_id]["configs"].append(msg_config)
                    self.__devices[device_name][tb_item][tb_key] = None

                    if "polling" in msg_config:
                        try:
                            polling_config = msg_config.get("polling")
                            polling_config["key"] = tb_key  # Just for logging
                            polling_config["type"] = polling_config.get("type", "always")
                            polling_config["period"] = polling_config.get("period", self.DEFAULT_POLL_PERIOD)
                            polling_config["nodeId"] = node_id
                            polling_config["isExtendedId"] = msg_config.get("isExtendedId",
                                                                            self.DEFAULT_EXTENDED_ID_FLAG)
                            polling_config["isFd"] = msg_config.get("isFd", self.DEFAULT_FD_FLAG)
                            polling_config["bitrateSwitch"] = msg_config.get("bitrateSwitch",
                                                                             self.DEFAULT_BITRATE_SWITCH_FLAG)
                            # Create CAN message object to validate its data
                            can_msg = Message(arbitration_id=polling_config["nodeId"],
                                              is_extended_id=polling_config["isExtendedId"],
                                              is_fd=polling_config["isFd"],
                                              bitrate_switch=polling_config["bitrateSwitch"],
                                              data=bytearray.fromhex(polling_config["dataInHex"]),
                                              check=True)
                            self.__polling_messages.append(polling_config)
                        except (ValueError, TypeError) as e:
                            log.warning("[%s] Ignore '%s' %s polling configuration, wrong CAN data: %s",
                                        self.get_name(), tb_key, config_key, str(e))
                            continue
            if is_device_config_valid:
                log.debug("[%s] Done parsing of '%s' device configuration", self.get_name(), device_name)
                self.__gateway.add_device(device_name, {"connector": self})
            else:
                log.warning("[%s] Ignore '%s' device configuration, because it doesn't have attributes,"
                            "attributeUpdates,timeseries or serverSideRpc", self.get_name(), device_name)

    def __parse_value_config(self, config):
        if config is None:
            log.warning("[%s] Wrong value configuration: no data", self.get_name())
            return

        if isinstance(config, str):
            value_matches = re.search(self.VALUE_REGEX, config)
            if not value_matches:
                log.warning("[%s] Wrong value configuration: '%s' doesn't match pattern", self.get_name(), config)
                return

            value_config = {
                "start": int(value_matches.group(1)),
                "length": int(value_matches.group(2)),
                "byteorder": value_matches.group(3) if value_matches.group(3) else self.DEFAULT_BYTEORDER,
                "type": value_matches.group(4)
                }

            if value_config["type"][0] == "i" or value_config["type"][0] == "l":
                value_config["signed"] = value_matches.group(5) == "signed" if value_matches.group(5) \
                    else self.DEFAULT_SIGNED_FLAG
            elif value_config["type"][0] == "s" or value_config["type"][0] == "r":
                value_config["encoding"] = value_matches.group(5) if value_matches.group(5) else self.DEFAULT_ENCODING

            return value_config
        elif isinstance(config, dict):
            try:
                value_config = {
                    "start": int(config["start"]),
                    "length": int(config["length"]),
                    "byteorder": config["byteorder"] if config.get("byteorder", "") else self.DEFAULT_BYTEORDER,
                    "type": config["type"]
                    }

                if value_config["type"][0] == "i" or value_config["type"][0] == "l":
                    value_config["signed"] = config.get("signed", self.DEFAULT_SIGNED_FLAG)
                elif value_config["type"][0] == "s":
                    value_config["encoding"] = config["encoding"] if config.get("encoding", "") else self.DEFAULT_ENCODING

                return value_config
            except (KeyError, ValueError) as e:
                log.warning("[%s] Wrong value configuration: %s", self.get_name(), str(e))
                return
        log.warning("[%s] Wrong value configuration: unknown type", self.get_name())
        return

    def __parse_command_config(self, config):
        if config is None:
            log.warning("[%s] Wrong command configuration: no data", self.get_name())
            return

        if isinstance(config, str):
            cmd_matches = re.search(self.CMD_REGEX, config)
            if not cmd_matches:
                log.warning("[%s] Wrong command configuration: '%s' doesn't match pattern", self.get_name(), config)
                return

            return {
                "start": int(cmd_matches.group(1)),
                "length": int(cmd_matches.group(2)),
                "byteorder": cmd_matches.group(3) if cmd_matches.group(3) else self.DEFAULT_BYTEORDER,
                "value": int(cmd_matches.group(4))
                }
        elif isinstance(config, dict):
            try:
                return {
                    "start": int(config["start"]),
                    "length": int(config["length"]),
                    "byteorder": config["byteorder"] if config.get("byteorder", "") else self.DEFAULT_BYTEORDER,
                    "value": int(config["value"])
                    }
            except (KeyError, ValueError) as e:
                log.warning("[%s] Wrong command configuration: %s", self.get_name(), str(e))
                return
        log.warning("[%s] Wrong command configuration: unknown type", self.get_name())
        return

    def __get_converter(self, config, need_uplink):
        if config is None:
            return BytesCanUplinkConverter() if need_uplink else BytesCanDownlinkConverter()
        else:
            if need_uplink:
                uplink = config.get("uplink")
                return BytesCanUplinkConverter() if uplink is None \
                    else TBModuleLoader.import_module(self._connector_type, uplink)
            else:
                downlink = config.get("downlink")
                return BytesCanDownlinkConverter() if downlink is None \
                    else TBModuleLoader.import_module(self._connector_type, downlink)


class Poller(Thread):
    def __init__(self, connector: CanConnector):
        super().__init__()
        self.connector = connector
        self.scheduler = sched.scheduler(time.time, time.sleep)
        self.events = []
        self.first_run = True
        self.daemon = True

    def poll(self):
        if self.first_run:
            log.info("[%s] Starting poller", self.connector.get_name())

        for polling_config in self.connector.get_polling_messages():
            key = polling_config["key"]
            if polling_config["type"] == "always":
                log.info("[%s] Polling '%s' key every %f sec", self.connector.get_name(), key, polling_config["period"])
                self.__poll_and_schedule(bytearray.fromhex(polling_config["dataInHex"]), polling_config)
            elif self.first_run:
                log.info("[%s] Polling '%s' key once", self.connector.get_name(), key)
                self.connector.send_data_to_bus(bytearray.fromhex(polling_config["dataInHex"]),
                                                polling_config,
                                                raise_exception=self.first_run)
        if self.first_run:
            self.first_run = False
            self.start()

    def run(self):
        self.scheduler.run()
        log.info("[%s] Poller stopped", self.connector.get_name())

    def stop(self):
        log.debug("[%s] Stopping poller", self.connector.get_name())
        for event in self.events:
            self.scheduler.cancel(event)

    def __poll_and_schedule(self, data, config):
        if self.connector.is_stopped():
            return
        if self.events:
            self.events.pop(0)

        log.debug("[%s] Sending periodic (%f sec) CAN message (arbitration_id=%d, data=%s)",
                  self.connector.get_name(), config["period"], config["nodeId"], data)
        self.connector.send_data_to_bus(data, config, raise_exception=self.first_run)

        event = self.scheduler.enter(config["period"], 1, self.__poll_and_schedule, argument=(data, config))
        self.events.append(event)
