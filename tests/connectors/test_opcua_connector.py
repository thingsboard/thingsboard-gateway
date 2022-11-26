#      Copyright 2022. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

import uuid
from threading import Thread
from time import sleep, time
from math import sin

from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from tests.connectors.connector_tests_base import ConnectorTestBase, log

try:
    from opcua.ua import NodeId, NodeIdType
    from opcua import ua, uamethod, Server
except ImportError:
    log.error("OpcUa library - not found. Installing...")
    TBUtility.install_package("opcua")
    from opcua.ua import NodeId, NodeIdType
    from opcua import ua, uamethod, Server

class OpcUaConnectorGeneralTest(ConnectorTestBase):
    def test_number_one(self):
        self._create_connector("connection_test.json")
        self.assertTrue(self.connector is not None)
        self.check_or_create_server()
        self.connector.open()



    def check_or_create_server(self):
        if not hasattr(self, "test_server"):
            self.test_server = Server()
            self.__server_thread = Thread(target=self.__server_run, name="Test OPC UA server", args=(self.test_server,))
        self.assertTrue(self.test_server is not None)


    def __server_run(self, test_server):
        self.test_server = test_server
        class SubHandler(object):
            
            def datachange_notification(self, node, val, data):
                print("Python: New data change event", node, val)
                
            def event_notification(self, event):
                print("Python: New event", event)

        @uamethod
        def multiply(parent, x, y):
            return x * y

        self.test_server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")
        self.test_server.set_server_name("Test Server")
        self.test_server.set_security_policy([
            ua.SecurityPolicyType.NoSecurity,
            ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt,
            ua.SecurityPolicyType.Basic256Sha256_Sign])

        uri = "http://127.0.0.1"

        idx = self.test_server.register_namespace(uri)
        device = self.test_server.nodes.objects.add_object(idx, "Device1")
        name = self.test_server.nodes.objects.add_variable(idx, "serialNumber", "TEST")
        name.set_writable()
        temperature_and_humidity = device.add_object(idx, "TemperatureAndHumiditySensor")
        temperature = temperature_and_humidity.add_variable(idx, "Temperature", 56.7)
        humidity = temperature_and_humidity.add_variable(idx, "Humidity", 68.7)
        battery = device.add_object(idx, "Battery")
        battery_level = battery.add_variable(idx, "batteryLevel", 24)
        device.add_method(idx, "multiply", multiply, [ua.VariantType.Int64, ua.VariantType.Int64], [ua.VariantType.Int64])

        self.test_server.start()
        try:
            while self.server_running:
                sleep(.1)
        finally:
            self.test_server.stop()

    def stop_test_server(self):
        self.server_running = False

    def tearDown(self):
        super().tearDown()
        self.stop_test_server()

