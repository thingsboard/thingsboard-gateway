#     Copyright 2026. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
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
import socket
from asyncio import create_task, sleep, wait_for
from unittest import IsolatedAsyncioTestCase

from bacpypes3.apdu import AbortPDU, AbortReason, ReadPropertyMultipleRequest
from bacpypes3.basetypes import PropertyIdentifier, ReadAccessSpecification
from bacpypes3.constructeddata import SequenceOf
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import ObjectIdentifier

from thingsboard_gateway.connectors.bacnet.application import Application
from thingsboard_gateway.connectors.bacnet.entities.device_object_config import DeviceObjectConfig


class BacnetConfirmationHandlerTestCase(IsolatedAsyncioTestCase):
    """
    Application.confirmation_handler is the only task that resolves the futures returned by
    Application.request(). It has to keep running after a confirmation that matches no pending
    request, for example the AbortPDU that bacpypes3 delivers for a request whose task the
    connector cancelled on its own RPC/attribute-update timeout. If the handler stops, every
    following read hangs, Application._requests grows by one entry per poll and, once all 256
    invoke ids are taken, bacpypes3's Application.request() spins forever on one CPU core.
    """

    async def asyncSetUp(self):
        self.log = logging.getLogger('Bacnet test')

        # A UDP socket that never answers plays the controller during a "no-response" window.
        self.silent_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.silent_socket.bind(('127.0.0.1', 0))
        self.controller = Address('127.0.0.1:%d' % self.silent_socket.getsockname()[1])

        self.application = Application(
            DeviceObjectConfig({
                'host': '127.0.0.1',
                'port': str(self._free_udp_port()),
                'objectIdentifier': 599,
                'objectName': 'TB_gateway',
                'maxApduLengthAccepted': 1024,
                'segmentationSupported': 'segmentedBoth',
                'vendorIdentifier': 15,
            }),
            lambda apdu: None,
            self.log,
        )
        # Give up on an unanswered request after 200 ms instead of 3000 ms x 4 retries.
        self.application.device_object.apduTimeout = 200
        self.application.device_object.numberOfApduRetries = 0

        self.handler = create_task(self.application.confirmation_handler())

    async def asyncTearDown(self):
        self.handler.cancel()
        self.application.close()
        self.silent_socket.close()

    @staticmethod
    def _free_udp_port():
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind(('127.0.0.1', 0))
            return sock.getsockname()[1]

    def _read_request(self):
        return ReadPropertyMultipleRequest(
            listOfReadAccessSpecs=SequenceOf(ReadAccessSpecification)([
                ReadAccessSpecification(
                    objectIdentifier=ObjectIdentifier('analog-value,1'),
                    listOfPropertyReferences=[PropertyIdentifier('present-value')],
                ),
            ]),
            destination=self.controller,
        )

    async def _assert_requests_are_still_resolved(self):
        # A new request to the silent controller must come back as "no-response" and be removed
        # from the pending list. Both only happen while the confirmation handler is running.
        with self.assertRaises(AbortPDU):
            await wait_for(self.application.request(self._read_request()), timeout=2)
        await sleep(0)
        self.assertNotIn(self.controller, self.application._requests)

    async def test_handler_keeps_running_after_unmatched_confirmation(self):
        stray = AbortPDU(True, 250, AbortReason.noResponse)
        stray.pduSource = self.controller

        await self.application.confirmation(stray)
        await sleep(0.3)

        self.assertFalse(self.handler.done(), 'confirmation handler stopped after an unmatched confirmation')
        await self._assert_requests_are_still_resolved()

    async def test_handler_keeps_running_after_cancelled_request(self):
        # The connector cancels the task of an RPC or attribute update that exceeds its own
        # timeout (5 s) while bacpypes3 is still retrying (3 s x 4). Cancelling the task cancels
        # the request future, which removes it from Application._requests before the AbortPDU
        # for that request arrives.
        future = self.application.request(self._read_request())
        await sleep(0.05)
        future.cancel()
        await sleep(0.5)

        self.assertFalse(self.handler.done(), 'confirmation handler stopped after a cancelled request')
        await self._assert_requests_are_still_resolved()
