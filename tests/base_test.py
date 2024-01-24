import logging
from sys import stdout
from unittest import TestCase
from tests.test_utils.gateway_device_util import GatewayDeviceUtil

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

LOG = logging.getLogger("TEST")
LOG.level = logging.DEBUG
stream_handler = logging.StreamHandler(stdout)
LOG.addHandler(stream_handler)


class BaseTest(TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log = LOG

    @classmethod
    def setUpClass(cls):
        GatewayDeviceUtil.get_gateway_device()

    @classmethod
    def tearDownClass(cls):
        GatewayDeviceUtil.delete_gateway_device()
        assert GatewayDeviceUtil.GATEWAY_DEVICE is None
