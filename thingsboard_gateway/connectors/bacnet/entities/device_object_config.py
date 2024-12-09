#     Copyright 2024. ThingsBoard
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

class DeviceObjectConfig:
    def __init__(self, config):
        self.update_address_in_config_util(config)
        self.address = config['address']
        self.device_discovery_timeout = int(config.get('deviceDiscoveryTimeoutInSec', 5))
        self.__object_identifier = int(config['objectIdentifier'])
        self.__object_name = config.get('objectName', 'Gateway')
        self.__network_number = int(config.get('networkNumber', 3))
        self.__network_number_quality = config.get('networkNumberQuality', 'configured')
        self.__max_apdu_length_accepted = int(config.get('maxApduLengthAccepted', 1024))
        self.__segmentation_supported = config.get('segmentationSupported', 'segmentedBoth')
        self.__vendor_identifier = int(config.get('vendorIdentifier', 15))

    @property
    def device_object_config(self):
        return {
            'objectIdentifier': ('device', self.__object_identifier),
            'objectName': self.__object_name,
            'maxApduLengthAccepted': self.__max_apdu_length_accepted,
            'segmentationSupported': self.__segmentation_supported,
            'vendorIdentifier': self.__vendor_identifier
        }

    @property
    def network_port_object_config(self):
        return {
            'objectIdentifier': ('network-port', self.__object_identifier + 1),
            'objectName': 'NetworkPort-1',
            'networkNumber': self.__network_number,
            'networkNumberQuality': self.__network_number_quality
        }

    @staticmethod
    def update_address_in_config_util(config):
        address = config.get('address', '')
        if address:
            return config
        if 'host' in config:
            address += config.pop('host')
            address = address.rstrip('/')
        if 'mask' in config:
            network_mask = config.pop('mask', None)
            if network_mask:
                address += '/' + network_mask
        if 'port' in config:
            address += ':' + str(config.pop('port', 47808))
        config['address'] = address
