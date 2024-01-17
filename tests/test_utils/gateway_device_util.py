import logging
from tb_rest_client.rest_client_ce import *
from tb_rest_client.rest import ApiException

LOG = logging.getLogger("TEST")

DEFAULT_URL = "http://localhost:8080"

DEFAULT_USERNAME = "tenant@thingsboard.org"
DEFAULT_PASSWORD = "tenant"


class GatewayDeviceUtil:

    GATEWAY_DEVICE_NAME = "Test Gateway device"
    GATEWAY_DEVICE = None

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
                gateway_device = Device(name=GatewayDeviceUtil,
                                        device_profile_id=gateway_device_profile.id,
                                        additional_info=additional_info)
                gateway_device = rest_client.save_device(gateway_device,
                                                         access_token="GatewayAccessToken")
                GatewayDeviceUtil.GATEWAY_DEVICE = gateway_device

                logging.info("Gateway device was created:\n%r\n", gateway_device.name)

                return gateway_device
            except ApiException as e:
                logging.exception(e)
                exit(1)

    @staticmethod
    def delete_gateway_device():
        if GatewayDeviceUtil.GATEWAY_DEVICE is None:

            return
        with RestClientCE(base_url=DEFAULT_URL) as rest_client:
            try:
                rest_client.login(username=DEFAULT_USERNAME, password=DEFAULT_PASSWORD)
                rest_client.delete_device(GatewayDeviceUtil.GATEWAY_DEVICE.id)
                logging.info("Gateway device was deleted:\n%r\n", GatewayDeviceUtil.GATEWAY_DEVICE.name)

                GatewayDeviceUtil.GATEWAY_DEVICE = None
            except ApiException as e:
                logging.exception(e)
                exit(1)
