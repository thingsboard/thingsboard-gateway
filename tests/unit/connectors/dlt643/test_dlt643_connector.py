import unittest
import serial
from unittest.mock import MagicMock, patch
from thingsboard_gateway.connectors.dlt643.dlt643_connector import DLT643Connector

class TestDLT643Connector(unittest.TestCase):

    @patch("serial.Serial")
    def test_connection(self, serial_mock):
        config = {
            "connection": {
                "port": "COM1",
                "baudrate": 9600
            },
            "slave": {
                "addr": 1
            },
            "mapping": {
                "device_name": "Device 1"
            }
        }
        gateway = MagicMock()
        connector = DLT643Connector(gateway, config, "DLT643Connector")
        connector.open()
        serial_mock.assert_called_once_with(
            port="COM1",
            baudrate=9600,
            bytesize=8,
            parity="E",
            stopbits=1,
            timeout=5
        )
        connector.close()

    @patch("serial.Serial")
    def test_polling(self, serial_mock):
        config = {
            "master": {
                "command_table": [
                    {
                        "code": "0x0001",
                        "type": "request_data_1"  
                    }
                ]
            },
            "slave": {
                "addr": 1,
                "object_table": []
            },
            "mapping": {
                "device_name": "Device 1",
                "timeseries": []
            }
        }
        gateway = MagicMock()
        connector = DLT643Connector(gateway, config, "DLT643Connector")
        request_mock = MagicMock()
        connector._DLT643Connector__send_request = request_mock
        connector._DLT643Connector__polling_interval = 0.1
        connector.open()
        time.sleep(0.5)
        request_mock.assert_called()

    @patch("serial.Serial")  
    def test_rpc(self, serial_mock):
        config = {
            "slave": {
                "addr": 1,
                "object_table": []
            },  
            "mapping": {
                "device_name": "Device 1",
                "timeseries": []  
            }
        }
        gateway = MagicMock()
        connector = DLT643Connector(gateway, config, "DLT643Connector") 
        set_mock = MagicMock()
        connector._DLT643Connector__set_object_value = set_mock
        rpc_request = {
            "device": "Device 1",
            "data": {
                "id": 1,
                "method": "write",
                "params": {
                    "object": "switch",
                    "value": True  
                }
            }
        }
        connector.server_side_rpc_handler(rpc_request)
        set_mock.assert_called_once_with("switch", True)
        gateway.send_rpc_reply.assert_called_once_with(
            "Device 1", 1, success=True
        )

if __name__ == "__main__":
    unittest.main()