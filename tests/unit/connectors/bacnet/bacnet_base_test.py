import logging
from asyncio import Queue, new_event_loop
from os import path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock
from bacpypes3.apdu import IAmRequest
from bacpypes3.pdu import Address
from simplejson import load
from thingsboard_gateway.connectors.bacnet.device import Device, Devices
from thingsboard_gateway.connectors.bacnet.bacnet_connector import AsyncBACnetConnector


class BacnetBaseTestCase(IsolatedAsyncioTestCase):
    CONFIG_PATH = path.join(path.dirname(path.abspath(__file__)), 'data')
    DEVICE_NAME = 'test emulator device'
    CONNECTOR_TYPE = 'bacnet'

    async def asyncSetUp(self):
        self.connector: AsyncBACnetConnector = AsyncBACnetConnector.__new__(AsyncBACnetConnector)
        self.connector._AsyncBACnetConnector__log = logging.getLogger('Bacnet test')
        self.connector.loop = new_event_loop()
        self.connector._AsyncBACnetConnector__process_device_queue = Queue(1_000_000)
        self.connector._AsyncBACnetConnector__process_device_rescan_queue = Queue(1_000_000)
        self.connector._AsyncBACnetConnector__devices = Devices()
        self.device = self.create_fake_device(
            attribute_update_config_path='attribute_updates/on_attribute_updates_bacnet_config.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)
        if not hasattr(self.connector, '_AsyncBACnetConnector__gateway'):
            self.connector._AsyncBACnetConnector__gateway = MagicMock()

    async def asyncTearDown(self):
        log = logging.getLogger('Bacnet test')
        for handler in list(log.handlers):
            log.removeHandler(handler)
        self.connector = None
        self.device = None
        await super().asyncTearDown()

    @staticmethod
    def convert_json(config_path):
        with open(config_path, 'r') as config_file:
            config = load(config_file)
        return config

    @staticmethod
    def create_fake_apdu_i_am_request(
            addr: str = "192.168.1.136:47809",
            device_id: int = 1234,
            device_name: str = "test emulator device",
            max_apdu: int = 1476,
            segmentation: str = "segmentedBoth",
            vendor_id: int = 15,
    ):
        apdu = IAmRequest(
            iAmDeviceIdentifier=("device", device_id),
            maxAPDULengthAccepted=max_apdu,
            segmentationSupported=segmentation,
            vendorID=vendor_id,
        )
        apdu.pduSource = Address(addr)
        apdu.deviceName = device_name
        apdu.routerName = None
        apdu.routerAddress = None
        apdu.routerId = None
        apdu.routerVendorId = None

        return apdu

    def create_fake_device(self, attribute_update_config_path):
        device = Device(
            connector_type=self.CONNECTOR_TYPE,
            config=self.convert_json(path.join(self.CONFIG_PATH, attribute_update_config_path)),
            i_am_request=self.create_fake_apdu_i_am_request(),
            reading_queue=self.connector._AsyncBACnetConnector__process_device_queue,
            rescan_queue=self.connector._AsyncBACnetConnector__process_device_rescan_queue,
            logger=self.connector._AsyncBACnetConnector__log,
            converter_logger=MagicMock(),
        )
        return device
