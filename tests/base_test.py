import logging
from unittest import TestCase
from test_utils.gateway_device_util import GatewayDeviceUtil
import sys
sys.path.append('..')

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

LOG = logging.getLogger("TEST")


class BaseTest(TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log = LOG

    def setUp(self):
        device = GatewayDeviceUtil.create_gateway_device()
        self.assertIsNotNone(device)

    def tearDown(self):
        GatewayDeviceUtil.delete_gateway_device()
        self.assertIsNone(GatewayDeviceUtil.GATEWAY_DEVICE)