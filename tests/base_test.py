import logging
import signal
from sys import platform, stdout
from unittest import TestCase
from tests.test_utils.gateway_device_util import GatewayDeviceUtil

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

LOG = logging.getLogger("TEST")
LOG.level = logging.DEBUG
stream_handler = logging.StreamHandler(stdout)
LOG.addHandler(stream_handler)
LOG.trace = LOG.debug


class BaseTest(TestCase):
    TIMEOUT = 600  # 10 minutes in seconds

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log = LOG
        self.log.trace = self.log.debug

    @classmethod
    def setUpClass(cls):
        GatewayDeviceUtil.get_gateway_device()

    @classmethod
    def tearDownClass(cls):
        GatewayDeviceUtil.delete_gateway_device()
        assert GatewayDeviceUtil.GATEWAY_DEVICE is None

    def setUp(self):
        if platform.system() != "Windows":
            signal.signal(signal.SIGALRM, self._timeout_handler)
            signal.alarm(self.TIMEOUT)

    def tearDown(self):
        signal.alarm(0)  # Disable the alarm

    def _timeout_handler(self, signum, frame):
        raise TimeoutError(f"Test exceeded the time limit of {self.TIMEOUT} seconds")