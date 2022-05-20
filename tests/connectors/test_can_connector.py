#     Copyright 2020. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License"];
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

import logging
import re
import struct
import unittest
from os import path
from random import choice, randint, uniform
from string import ascii_lowercase
from time import sleep
from unittest.mock import Mock

import thingsboard_gateway
from can import Notifier, BufferedReader, Bus, Message
from simplejson import load

from thingsboard_gateway.connectors.can.can_connector import CanConnector


def assert_not_called_with(self, *args, **kwargs):
    try:
        self.assert_called_with(*args, **kwargs)
    except AssertionError:
        return
    raise AssertionError('Expected %s to not have been called.' % self._format_mock_call_signature(args, kwargs))


Mock.assert_not_called_with = assert_not_called_with

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


class CanConnectorTestsBase(unittest.TestCase):
    CONFIG_PATH = path.join(path.dirname(path.dirname(path.abspath(__file__))),
                            "data" + path.sep + "can" + path.sep)

    def setUp(self):
        self.bus = self._create_bus()
        self.gateway = Mock(spec=thingsboard_gateway.TBGatewayService)
        self.connector = None
        self.config = None

    def tearDown(self):
        self.connector.close()
        self.bus.shutdown()

    def _create_bus(self):
        return Bus(
            channel="virtual_channel",
            bustype="virtual",
            receive_own_messages=False
        )

    def _create_connector(self, config_file_name):
        with open(self.CONFIG_PATH + config_file_name, 'r', encoding="UTF-8") as file:
            self.config = load(file)
            self.connector = CanConnector(self.gateway, self.config, "can")
            self.connector.open()
            sleep(1)  # some time to init


class CanConnectorPollingTests(CanConnectorTestsBase):

    def test_polling_once(self):
        self._create_connector("polling_once.json")
        config = self.config["devices"][0]["attributes"][0]

        message = self.bus.recv(self.connector.DEFAULT_POLL_PERIOD)

        self.assertEqual(message.arbitration_id, config["nodeId"])
        self.assertEqual(message.is_extended_id, config["isExtendedId"])
        self.assertEqual(message.is_fd, self.connector.DEFAULT_FD_FLAG)
        self.assertEqual(message.bitrate_switch, self.connector.DEFAULT_BITRATE_SWITCH_FLAG)
        self.assertEqual(message.data, bytearray.fromhex(config["polling"]["dataInHex"]))

        # Some buses may receive their own messages. Remove it from the queue
        self.bus.recv(0)

        # Check if no new polling messages
        sleep(self.connector.DEFAULT_POLL_PERIOD)
        message = self.bus.recv(self.connector.DEFAULT_POLL_PERIOD)
        self.assertIsNone(message)

    def test_polling_always(self):
        self._create_connector("polling_always.json")
        config = self.config["devices"][0]["attributes"][0]

        for _ in range(1, 5):
            # Timeout should be greater that polling period to prevent the case
            # when message is received earlier than time is out.
            message = self.bus.recv(config["polling"]["period"] + 0.2)
            self.assertIsNotNone(message)

            # Some buses may receive their own messages. Remove it from the queue
            self.bus.recv(0)

            self.assertEqual(message.arbitration_id, config["nodeId"])
            self.assertEqual(message.is_extended_id, self.connector.DEFAULT_EXTENDED_ID_FLAG)
            self.assertEqual(message.is_fd, self.connector.DEFAULT_FD_FLAG)
            self.assertEqual(message.bitrate_switch, self.connector.DEFAULT_BITRATE_SWITCH_FLAG)
            self.assertEqual(message.data, bytearray.fromhex(config["polling"]["dataInHex"]))

    def test_multiple_polling(self):
        reader = BufferedReader()
        bus_notifier = Notifier(self.bus, [reader])
        self._create_connector("multiple_polling.json")
        config1 = self.config["devices"][0]["timeseries"][0]
        config2 = self.config["devices"][0]["timeseries"][1]
        config3 = self.config["devices"][0]["timeseries"][2]

        time_to_wait = config2["polling"]["period"] * 4
        message_count = int(time_to_wait / config2["polling"]["period"]) + 1 + \
                        int(time_to_wait / config3["polling"]["period"]) + 1 + \
                        1  # one time polling task

        sleep(time_to_wait)
        self.connector.close()
        bus_notifier.stop()

        messages = []
        while True:
            msg = reader.get_message(time_to_wait)
            if msg is None:
                break
            messages.append(msg)

        self.assertEqual(len(messages), message_count)

        expected_message_ids = [config1["nodeId"], config2["nodeId"], config3["nodeId"],
                                config2["nodeId"], config3["nodeId"], config2["nodeId"],
                                config3["nodeId"], config2["nodeId"], config3["nodeId"],
                                config2["nodeId"]]
        for i in range(0, message_count):
            self.assertEqual(messages[i].arbitration_id, expected_message_ids[i])


class CanConnectorTsAndAttrTests(CanConnectorTestsBase):

    def _create_bus(self):
        return Bus(
            channel="virtual_channel",
            bustype="virtual",
            receive_own_messages=False,
            is_fd=True
        )

    def test_string_attribute_and_custom_device_type(self):
        self._create_connector("ts_and_attr.json")
        device_name = self.config["devices"][0]["name"]
        config = self.config["devices"][0]["attributes"][0]
        value_matches = re.search(self.connector.VALUE_REGEX, config["value"])

        string_value = ''.join(choice(ascii_lowercase) for _ in range(int(value_matches.group(2))))
        can_data = list(config["command"]["value"].to_bytes(config["command"]["length"],
                                                            config["command"]["byteorder"]))
        can_data.extend(string_value.encode(value_matches.group(5)))

        message_count = 5
        for _ in range(message_count):
            self.bus.send(Message(arbitration_id=config["nodeId"],
                                  is_fd=config["isFd"],
                                  data=can_data))

        sleep(1)  # Wait while connector process CAN message

        self.assertEqual(self.gateway.send_to_storage.call_count, message_count)
        self.gateway.send_to_storage.assert_called_with(self.connector.get_name(),
                                                        {"deviceName": device_name,
                                                         "deviceType": self.config["devices"][0]["type"],
                                                         "attributes": [{"serialNumber": string_value}],
                                                         "telemetry": []})

    def test_send_only_on_change_and_default_device_type(self):
        self._create_connector("ts_and_attr.json")
        config = self.config["devices"][1]["timeseries"][0]

        value_matches = re.search(self.connector.VALUE_REGEX, config["value"])
        value = randint(0, pow(2, int(value_matches.group(2))))
        can_data = list(bytearray.fromhex("0" * 2 * int(value_matches.group(1))))
        can_data.extend(value.to_bytes(int(value_matches.group(2)),
                                       value_matches.group(3) if value_matches.group(
                                           3) else self.connector.DEFAULT_BYTEORDER))

        for _ in range(5):
            self.bus.send(Message(arbitration_id=config["nodeId"],
                                  data=can_data))

        sleep(1)

        self.gateway.send_to_storage.assert_called_once_with(self.connector.get_name(),
                                                             {"deviceName": self.config["devices"][1]["name"],
                                                              "deviceType": self.connector._connector_type,
                                                              "attributes": [],
                                                              "telemetry": [{config["key"]: value}]})

    def test_telemetries_with_commands(self):
        self._create_connector("ts_and_attr.json")

        config1 = self.config["devices"][2]["timeseries"][0]
        cmd_matches1 = re.search(self.connector.CMD_REGEX, config1["command"])
        value_matches1 = re.search(self.connector.VALUE_REGEX, config1["value"])
        value1 = randint(0, pow(2, int(value_matches1.group(2))))
        can_data1 = list(int(cmd_matches1.group(4)).to_bytes(int(cmd_matches1.group(2)), cmd_matches1.group(3)))
        can_data1.extend(value1.to_bytes(int(value_matches1.group(2)),
                                         value_matches1.group(3) if value_matches1.group(
                                             3) else self.connector.DEFAULT_BYTEORDER))
        can_data1.extend(
            list(bytearray.fromhex("0" * 2 * (8 - int(value_matches1.group(2)) - int(cmd_matches1.group(2))))))

        config2 = self.config["devices"][2]["timeseries"][1]
        cmd_matches2 = re.search(self.connector.CMD_REGEX, config2["command"])
        value_matches2 = re.search(self.connector.VALUE_REGEX, config2["value"])
        value2 = randint(0, pow(2, int(value_matches2.group(2))))
        can_data2 = list(int(cmd_matches2.group(4)).to_bytes(int(cmd_matches2.group(2)), cmd_matches2.group(3)))
        can_data2.extend(value2.to_bytes(int(value_matches2.group(2)),
                                         value_matches2.group(3) if value_matches2.group(
                                             3) else self.connector.DEFAULT_BYTEORDER))
        can_data2.extend(
            list(bytearray.fromhex("0" * 2 * (8 - int(value_matches2.group(2)) - int(cmd_matches2.group(2))))))

        self.bus.send(Message(arbitration_id=config1["nodeId"], data=can_data1))
        sleep(1)

        self.gateway.send_to_storage.assert_called_with(self.connector.get_name(),
                                                        {"deviceName": self.config["devices"][2]["name"],
                                                         "deviceType": self.connector._connector_type,
                                                         "attributes": [],
                                                         "telemetry": [{config1["key"]: value1}]})

        self.bus.send(Message(arbitration_id=config2["nodeId"], data=can_data2))
        sleep(1)

        self.gateway.send_to_storage.assert_called_with(self.connector.get_name(),
                                                        {"deviceName": self.config["devices"][2]["name"],
                                                         "deviceType": self.connector._connector_type,
                                                         "attributes": [],
                                                         "telemetry": [{config2["key"]: value2}]})

    def test_telemetries_with_different_commands_and_same_arbitration_node_id(self):
        self._create_connector("ts_and_attr.json")

        config1 = self.config["devices"][3]["timeseries"][0]
        cmd_matches1 = re.search(self.connector.CMD_REGEX, config1["command"])
        value_matches1 = re.search(self.connector.VALUE_REGEX, config1["value"])
        value1 = randint(0, pow(2, int(value_matches1.group(2))))
        can_data1 = list(int(cmd_matches1.group(4)).to_bytes(int(cmd_matches1.group(2)), cmd_matches1.group(3)))
        can_data1.extend(value1.to_bytes(int(value_matches1.group(2)),
                                         value_matches1.group(3) if value_matches1.group(
                                             3) else self.connector.DEFAULT_BYTEORDER))
        can_data1.extend(
            list(bytearray.fromhex("0" * 2 * (8 - int(value_matches1.group(2)) - int(cmd_matches1.group(2))))))

        config2 = self.config["devices"][2]["timeseries"][1]
        cmd_matches2 = re.search(self.connector.CMD_REGEX, config2["command"])
        value_matches2 = re.search(self.connector.VALUE_REGEX, config2["value"])
        value2 = randint(0, pow(2, int(value_matches2.group(2))))
        can_data2 = list(int(cmd_matches2.group(4)).to_bytes(int(cmd_matches2.group(2)), cmd_matches2.group(3)))
        can_data2.extend(value2.to_bytes(int(value_matches2.group(2)),
                                         value_matches2.group(3) if value_matches2.group(
                                             3) else self.connector.DEFAULT_BYTEORDER))
        can_data2.extend(
            list(bytearray.fromhex("0" * 2 * (8 - int(value_matches2.group(2)) - int(cmd_matches2.group(2))))))

        self.bus.send(Message(arbitration_id=config1["nodeId"], data=can_data1))
        sleep(1)

        self.gateway.send_to_storage.assert_called_with(self.connector.get_name(),
                                                        {"deviceName": self.config["devices"][3]["name"],
                                                         "deviceType": self.connector._connector_type,
                                                         "attributes": [],
                                                         "telemetry": [{config1["key"]: value1}]})

        self.bus.send(Message(arbitration_id=config2["nodeId"], data=can_data2))
        sleep(1)

        self.gateway.send_to_storage.assert_not_called_with(self.connector.get_name(),
                                                            {"deviceName": self.config["devices"][3]["name"],
                                                             "deviceType": self.connector._connector_type,
                                                             "attributes": [],
                                                             "telemetry": [{config2["key"]: value2}]})


class CanConnectorAttributeUpdatesTests(CanConnectorTestsBase):

    def test_update(self):
        reader = BufferedReader()
        bus_notifier = Notifier(self.bus, [reader])
        self._create_connector("attribute_updates.json")

        configs = self.config["devices"][0]["attributeUpdates"]
        updates = {"device": self.config["devices"][0]["name"],
                   "data": {
                       "boolAttr": True,
                       "intAttr": randint(-int(pow(2, configs[1]["dataLength"]) / 2),
                                          pow(2, configs[1]["dataLength"] - 1)),
                       "floatAttr": uniform(-3.1415926535, 3.1415926535),
                       "stringAttr": ''.join(choice(ascii_lowercase) for _ in range(8)),
                       "wrongConfigAttr": True
                   }}

        data_list = [[int(updates["data"]["boolAttr"])],
                     updates["data"]["intAttr"].to_bytes(configs[1]["dataLength"],
                                                         configs[1]["dataByteorder"],
                                                         signed=(updates["data"]["intAttr"] < 0)),
                     list(struct.pack(">f", updates["data"]["floatAttr"])),
                     list(str("Test" + updates["data"]["stringAttr"]).encode(self.connector.DEFAULT_ENCODING))
                     ]

        self.connector.on_attributes_update(updates)

        sleep(1)
        self.connector.close()
        bus_notifier.stop()

        messages = []
        while True:
            msg = reader.get_message(1)
            if msg is None:
                break
            messages.append(msg)

        self.assertEqual(len(messages), len(data_list))

        messages = sorted(messages, key=lambda message: message.arbitration_id)
        for i in range(len(messages)):
            self.assertTrue(messages[i].equals(Message(arbitration_id=configs[i]["nodeId"],
                                                       is_extended_id=configs[i].get("isExtendedId",
                                                                                     self.connector.DEFAULT_EXTENDED_ID_FLAG),
                                                       is_fd=configs[i].get("isFd", self.connector.DEFAULT_FD_FLAG),
                                                       bitrate_switch=configs[i].get("bitrateSwitch",
                                                                                     self.connector.DEFAULT_BITRATE_SWITCH_FLAG),
                                                       data=data_list[i],
                                                       timestamp=messages[i].timestamp,
                                                       channel=messages[i].channel)))


class CanConnectorRpcTests(CanConnectorTestsBase):

    def _create_bus(self):
        return Bus(
            channel="virtual_channel",
            bustype="virtual",
            receive_own_messages=False,
            is_fd=True
        )

    def test_rpc_with_hex_data_in_config(self):
        self._create_connector("rpc.json")
        config = self.config["devices"][0]["serverSideRpc"][0]

        self.connector.server_side_rpc_handler({"device": self.config["devices"][0]["name"],
                                                "data": {
                                                    "id": 1,
                                                    "method": config["method"]
                                                }})

        actual_message = self.bus.recv(1)
        self.assertTrue(actual_message.equals(Message(arbitration_id=config["nodeId"],
                                                      is_fd=config["isFd"],
                                                      bitrate_switch=config["bitrateSwitch"],
                                                      data=bytearray.fromhex(config["dataInHex"]),
                                                      timestamp=actual_message.timestamp,
                                                      channel=actual_message.channel)))

    def test_rpc_with_hex_data_in_params(self):
        self._create_connector("rpc.json")
        config = self.config["devices"][1]["serverSideRpc"][0]
        hex_data = "1234 abcd"

        self.assertNotEqual(hex_data, config["dataInHex"])

        self.connector.server_side_rpc_handler({"device": self.config["devices"][1]["name"],
                                                "data": {
                                                    "id": 1,
                                                    "method": config["method"],
                                                    "params": {
                                                        "dataInHex": hex_data
                                                    }
                                                }})

        actual_message = self.bus.recv(1)
        self.assertTrue(actual_message.equals(Message(arbitration_id=config["nodeId"],
                                                      is_fd=config["isFd"],
                                                      bitrate_switch=config["bitrateSwitch"],
                                                      data=bytearray.fromhex(hex_data),
                                                      timestamp=actual_message.timestamp,
                                                      channel=actual_message.channel)))

    def test_rpc_expression_in_config(self):
        self._create_connector("rpc.json")
        config = self.config["devices"][0]["serverSideRpc"][1]

        max_allowed_speed = randint(100, 200)
        user_speed = randint(150, 250)
        self.connector.server_side_rpc_handler({"device": self.config["devices"][0]["name"],
                                                "data": {
                                                    "id": 1,
                                                    "method": config["method"],
                                                    "params": {
                                                        "userSpeed": user_speed,
                                                        "maxAllowedSpeed": max_allowed_speed
                                                    }
                                                }})

        can_data = int(user_speed if max_allowed_speed > user_speed else max_allowed_speed) \
            .to_bytes(config["dataLength"], "little")
        actual_message = self.bus.recv(1)
        self.assertTrue(actual_message.equals(Message(arbitration_id=config["nodeId"],
                                                      is_extended_id=config.get("isExtendedId",
                                                                                self.connector.DEFAULT_EXTENDED_ID_FLAG),
                                                      data=can_data,
                                                      timestamp=actual_message.timestamp,
                                                      channel=actual_message.channel)))

    def test_deny_unknown_rpc(self):
        self._create_connector("rpc.json")

        self.connector.server_side_rpc_handler({"device": self.config["devices"][0]["name"],
                                                "data": {
                                                    "id": 1,
                                                    "method": ''.join(choice(ascii_lowercase) for _ in range(8))
                                                }})

        self.assertIsNone(self.bus.recv(5))

    def test_enable_unknown_rpc(self):
        self._create_connector("rpc.json")

        max_not_extended_node_id = 0x800
        node_id = randint(0, 0x20000000)

        data_before = "aa bb"
        data_after = "cc dd ee ff"

        data_length = 4
        integer_value = randint(-int(pow(2, 8 * data_length) / 2), pow(2, 8 * data_length) - 1)
        can_data = list(bytearray.fromhex(data_before))
        can_data.extend(integer_value.to_bytes(data_length, "big", signed=(integer_value < 0)))
        can_data.extend(bytearray.fromhex(data_after))

        self.connector.server_side_rpc_handler({"device": self.config["devices"][2]["name"],
                                                "data": {
                                                    "id": 1,
                                                    "method": ''.join(choice(ascii_lowercase) for _ in range(8)),
                                                    "params": {
                                                        "value": integer_value,
                                                        "nodeId": node_id,
                                                        "isExtendedId": (node_id > max_not_extended_node_id),
                                                        "isFd": (len(can_data) > 8),
                                                        "dataLength": data_length,
                                                        # Actually value may be either signed or unsigned,
                                                        # connector should process this case correctly
                                                        "dataSigned": False,
                                                        "dataBefore": data_before,
                                                        "dataAfter": data_after,
                                                        "response": True
                                                    }
                                                }})

        actual_message = self.bus.recv(1)
        self.assertTrue(actual_message.equals(Message(arbitration_id=node_id,
                                                      is_extended_id=(node_id > max_not_extended_node_id),
                                                      is_fd=(len(can_data) > 8),
                                                      data=can_data,
                                                      timestamp=actual_message.timestamp,
                                                      channel=actual_message.channel)))

        self.gateway.send_rpc_reply.assert_called_once_with(self.config["devices"][2]["name"],
                                                            1,
                                                            {"success": True})

    def test_rpc_response_failed(self):
        self._create_connector("rpc.json")
        config = self.config["devices"][3]["serverSideRpc"][0]

        self.connector.server_side_rpc_handler({"device": self.config["devices"][3]["name"],
                                                "data": {
                                                    "id": 1,
                                                    "method": config["method"]
                                                }})
        sleep(1)
        self.gateway.send_rpc_reply.assert_called_once_with(self.config["devices"][3]["name"],
                                                            1,
                                                            {"success": False})


if __name__ == '__main__':
    unittest.main()
