import logging

from simplejson import loads
from tb_rest_client.rest import ApiException
from tb_rest_client.rest_client_ce import *

LOG = logging.getLogger("TEST")


class GatewayDeviceUtil:
    DEFAULT_URL = "http://localhost:9090"

    DEFAULT_USERNAME = "tenant@thingsboard.org"
    DEFAULT_PASSWORD = "tenant"

    GATEWAY_DEVICE_NAME = "Test Gateway device"
    GATEWAY_DEVICE = None

    GATEWAY_ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"

    @classmethod
    def get_gateway_device(cls, url=DEFAULT_URL, username=DEFAULT_USERNAME, password=DEFAULT_PASSWORD) -> Device:
        if cls.GATEWAY_DEVICE is None:
            cls.create_gateway_device(url=url, username=username, password=password)
        return cls.GATEWAY_DEVICE

    @staticmethod
    def create_gateway_device(url=DEFAULT_URL, username=DEFAULT_USERNAME, password=DEFAULT_PASSWORD) -> Device:
        with RestClientCE(base_url=url) as rest_client:
            try:
                rest_client.login(username=username, password=password)
                gateway_device_profile = rest_client.get_default_device_profile_info()
                additional_info = {"gateway": True,
                                   "overwriteActivityTime": False,
                                   "description": ""}
                gateway_device = Device(name=GatewayDeviceUtil.GATEWAY_DEVICE_NAME,
                                        device_profile_id=gateway_device_profile.id,
                                        additional_info=additional_info)
                gateway_device = rest_client.save_device(gateway_device,
                                                         access_token=GatewayDeviceUtil.GATEWAY_ACCESS_TOKEN)
                GatewayDeviceUtil.GATEWAY_DEVICE = gateway_device

                logging.info("Gateway device was created: %r", gateway_device.name)

                return gateway_device
            except ApiException as e:
                response_body = loads(bytes.decode(e.body, encoding='UTF-8'))
                if response_body:
                    if not response_body.get("status") == 400 or not response_body.get(
                            "message") == "Device with such name already exists!":
                        logging.exception(e)
                        exit(1)
                    else:
                        logging.info("Gateway device already exists: %r", GatewayDeviceUtil.GATEWAY_DEVICE_NAME)
                        gateway_device = rest_client.get_tenant_device(GatewayDeviceUtil.GATEWAY_DEVICE_NAME)
                        GatewayDeviceUtil.GATEWAY_DEVICE = gateway_device
                        return gateway_device

    @staticmethod
    def delete_gateway_device():
        if GatewayDeviceUtil.GATEWAY_DEVICE is None:
            return

        with RestClientCE(base_url=GatewayDeviceUtil.DEFAULT_URL) as rest_client:
            try:
                rest_client.login(username=GatewayDeviceUtil.DEFAULT_USERNAME, password=GatewayDeviceUtil.DEFAULT_PASSWORD)
                rest_client.delete_device(GatewayDeviceUtil.GATEWAY_DEVICE.id)
                logging.info("Gateway device was deleted: %r", GatewayDeviceUtil.GATEWAY_DEVICE.name)
                GatewayDeviceUtil.GATEWAY_DEVICE = None
            except ApiException as e:
                logging.exception(e)
                exit(1)
