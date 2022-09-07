import unittest
from unittest.mock import Mock, patch

from simplejson import dumps

from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService


def assert_not_called_with(self, *args, **kwargs):
    try:
        self.assert_called_with(*args, **kwargs)
    except AssertionError:
        return
    raise AssertionError('Expected %s to not have been called.' % self._format_mock_call_signature(args, kwargs))


Mock.assert_not_called_with = assert_not_called_with


class SDKTests(unittest.TestCase):
    """
    Before running tests: do the next steps:
    1. Make sure you are using right config files in tests/data/gateway/
    2. Run your local MQTT broker
    """

    gateway = None
    
    DEVICE_NAME = 'Example Name'
    DEVICE_TYPE = 'default'

    @classmethod
    def setUpClass(cls) -> None:
        cls.gateway = TBGatewayService('data/gateway/gateway.yaml')

    @classmethod
    def tearDownClass(cls) -> None:
        cls.gateway._TBGatewayService__stop_gateway()

    @patch('tb_gateway_mqtt.TBGatewayMqttClient.gw_connect_device')
    def test_add_device(self, mock_func):
        self.gateway.add_device(self.DEVICE_NAME,
                                {'connector': self.gateway.available_connectors['MQTT Broker Connector']}, self.DEVICE_TYPE)
        mock_func.assert_called_with(self.DEVICE_NAME, self.DEVICE_TYPE)

    @patch('tb_gateway_mqtt.TBGatewayMqttClient.gw_disconnect_device')
    def test_delete_device(self, mock_func):
        self.gateway.del_device(self.DEVICE_NAME)
        mock_func.assert_called_with(self.DEVICE_NAME)

    @patch('tb_gateway_mqtt.TBGatewayMqttClient.gw_send_attributes')
    def test_send_attributes(self, mock_func):
        data = {self.DEVICE_NAME: {'attributes': {"atr1": 1, "atr2": True, "atr3": "value3"}}}

        self.gateway.add_device(self.DEVICE_NAME,
                                {'connector': self.gateway.available_connectors['MQTT Broker Connector']}, self.DEVICE_TYPE)
        self.gateway._TBGatewayService__send_data(data)

        mock_func.assert_called_with(self.DEVICE_NAME, {"atr1": 1, "atr2": True, "atr3": "value3"})

    @patch('tb_gateway_mqtt.TBGatewayMqttClient.gw_send_telemetry')
    def test_send_telemetry(self, mock_func):
        data = {self.DEVICE_NAME: {'telemetry': {"key1": "11"}}}

        self.gateway.add_device(self.DEVICE_NAME,
                                {'connector': self.gateway.available_connectors['MQTT Broker Connector']}, self.DEVICE_TYPE)
        self.gateway._TBGatewayService__send_data(data)

        mock_func.assert_called_with(self.DEVICE_NAME, {"key1": "11"})

    @patch('tb_gateway_mqtt.TBGatewayMqttClient.gw_subscribe_to_all_attributes')
    def test_subscribe_to_all_gateway_topics(self, mock_func):
        self.gateway.subscribe_to_required_topics()

        mock_func.assert_called_with(self.gateway._attribute_update_callback)

    @patch('tb_device_mqtt.TBDeviceMqttClient.subscribe_to_all_attributes')
    def test_subscribe_to_all_device_attributes(self, mock_func):
        self.gateway.subscribe_to_required_topics()

        mock_func.assert_called_with(self.gateway._attribute_update_callback)

    @patch('tb_device_mqtt.TBDeviceMqttClient.set_server_side_rpc_request_handler')
    def test_set_server_side_rpc_request_handler(self, mock_func):
        self.gateway.subscribe_to_required_topics()

        mock_func.assert_called_with(self.gateway._rpc_request_handler)

    @patch('tb_gateway_mqtt.TBGatewayMqttClient.gw_set_server_side_rpc_request_handler')
    def test_gw_set_server_side_rpc_request(self, mock_func):
        self.gateway.subscribe_to_required_topics()

        mock_func.assert_called_with(self.gateway._rpc_request_handler)

    @patch('tb_gateway_mqtt.TBGatewayMqttClient.gw_send_rpc_reply')
    def test_gw_send_rpc_reply_with_content(self, mock_func):
        self.gateway._TBGatewayService__send_rpc_reply(self.DEVICE_NAME, 1,
                                                       {'device': self.DEVICE_NAME, 'data': {'id': 1, 'method': 'echo'}})

        mock_func.assert_called_with(self.DEVICE_NAME, 1,
                                     {'device': self.DEVICE_NAME, 'data': {'id': 1, 'method': 'echo'}},
                                     quality_of_service=0)

    @patch('tb_gateway_mqtt.TBGatewayMqttClient.gw_send_rpc_reply')
    def test_gw_send_rpc_reply_without_content(self, mock_func):
        self.gateway._TBGatewayService__send_rpc_reply(self.DEVICE_NAME, 1, success_sent=True)

        mock_func.assert_called_with(self.DEVICE_NAME, 1, dumps({'success': True}), quality_of_service=0)

    @patch('tb_device_mqtt.TBDeviceMqttClient.send_rpc_reply')
    def test_send_device_rpc_reply_without_content(self, mock_func):
        self.gateway._TBGatewayService__send_rpc_reply(req_id=1, success_sent=True)

        mock_func.assert_called_with(1, dumps({'success': True}), quality_of_service=0, wait_for_publish=None)

    @patch('tb_device_mqtt.TBDeviceMqttClient.send_rpc_reply')
    def test_send_device_rpc_reply_with_content(self, mock_func):
        self.gateway._TBGatewayService__send_rpc_reply(req_id=1, content={'device': self.DEVICE_NAME,
                                                                          'data': {'id': 1, 'method': 'echo'}},
                                                       success_sent=True)

        mock_func.assert_called_with(1, dumps({'success': True}), quality_of_service=0, wait_for_publish=None)

    @patch('tb_gateway_mqtt.TBGatewayMqttClient.gw_request_shared_attributes')
    @patch('tb_gateway_mqtt.TBGatewayMqttClient.gw_request_client_attributes')
    def test_request_device_client_attributes(self, mock_func, mock_func1):
        def callback(result):
            print(result)

        client_keys = ['attr1']
        shared_keys = ['attr2']

        self.gateway.request_device_attributes(self.DEVICE_NAME, client_keys=client_keys, shared_keys=shared_keys,
                                               callback=callback)

        mock_func.assert_called_with(self.DEVICE_NAME, client_keys, callback)
        mock_func1.assert_called_with(self.DEVICE_NAME, shared_keys, callback)


if __name__ == '__main__':
    unittest.main()
