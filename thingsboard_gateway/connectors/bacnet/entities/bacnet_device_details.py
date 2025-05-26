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
from bacpypes3.basetypes import Segmentation

MAX_REQUEST_SIZE = 26


class BACnetDeviceDetails:
    def __init__(self, i_am_request: IAmRequest):
        self.address = i_am_request.pduSource.__str__()
        self.__identifier = i_am_request.iAmDeviceIdentifier
        self.__object_identifier = i_am_request.iAmDeviceIdentifier[1]
        self.__vendor_id = i_am_request.vendorID
        self.__object_name = i_am_request.deviceName if hasattr(i_am_request, 'deviceName') else i_am_request.iAmDeviceIdentifier[1]
        self.__max_apdu_length = i_am_request.maxAPDULengthAccepted
        self.__segmentation = i_am_request.segmentationSupported

        self.__router_id = i_am_request.routerId if hasattr(i_am_request, 'routerId') else None
        self.__router_name = i_am_request.routerName if hasattr(i_am_request, 'routerName') else None
        self.__router_address = i_am_request.routerAddress if hasattr(i_am_request, 'routerAddress') else None
        self.__router_vendor_id = i_am_request.routerVendorId if hasattr(i_am_request, 'routerVendorId') else None

    def __str__(self):
        return (f"DeviceDetails(address={self.address}, objectIdentifier={self.__object_identifier}, "
                f"vendorId={self.__vendor_id}, objectName={self.__object_name}")

    @property
    def object_id(self):
        return self.__object_identifier

    @property
    def identifier(self):
        return self.__identifier

    @property
    def max_apdu_length(self):
        return self.__max_apdu_length

    @property
    def vendor_id(self):
        return self.__vendor_id

    @property
    def as_dict(self):
        return {
            "address": self.address,
            "objectId": self.__object_identifier,
            "vendorId": self.__vendor_id,
            "objectName": self.__object_name,
            "routerId": self.__router_id,
            "routerName": self.__router_name,
            "routerAddress": self.__router_address,
            "routerVendorId": self.__router_vendor_id,
        }

    def is_segmentation_supported(self):
        return self.__segmentation in (Segmentation.segmentedBoth, Segmentation.segmentedTransmit)

    def get_max_apdu_count(self) -> int:
        return int(self.__max_apdu_length / MAX_REQUEST_SIZE)
