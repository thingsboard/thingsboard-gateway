#     Copyright 2025. ThingsBoard
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

from bacpypes3.apdu import IAmRequest


class BACnetDeviceDetails:
    def __init__(self, i_am_request: IAmRequest):
        self.address = i_am_request.pduSource.__str__()
        self.__object_identifier = i_am_request.iAmDeviceIdentifier[1]
        self.__vendor_id = i_am_request.vendorID
        self.__object_name = i_am_request.deviceName

    def __str__(self):
        return (f"DeviceDetails(address={self.address}, objectIdentifier={self.__object_identifier}, "
                f"vendorId={self.__vendor_id}, objectName={self.__object_name}")

    @property
    def as_dict(self):
        return {
            "address": self.address,
            "objectIdentifier": self.__object_identifier,
            "vendorId": self.__vendor_id,
            "objectName": self.__object_name
        }
