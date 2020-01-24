#     Copyright 2020. ThingsBoard
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

from pprint import pformat
import codecs
from thingsboard_gateway.connectors.ble.ble_uplink_converter import BLEUplinkConverter, log
import struct
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class BytesBLEUplinkConverter(BLEUplinkConverter):
    def __init__(self, config):
        self.__config = config
        self.dict_result = {"deviceName": config.get('name', config['MACAddress']),
                            "deviceType": config.get('deviceType', 'BLEDevice'),
                            "telemetry": [],
                            "attributes": []
                            }

    def convert(self, config, data):
        try:
            if config.get('clean', True):
                self.dict_result["telemetry"] = []
                self.dict_result["attributes"] = []
            try:
                byte_from = config['section_config'].get('byteFrom')
                byte_to = config['section_config'].get('byteTo')
                try:
                    if data is None:
                        return {}
                    byte_to = byte_to if byte_to != -1 else len(data)
                    converted_data = data[byte_from: byte_to]
                    try:
                        converted_data = converted_data.replace(b"\x00", b'').decode('UTF-8')
                    except UnicodeDecodeError:
                        converted_data = str(converted_data)
                    if config['section_config'].get('key') is not None:
                        self.dict_result[config['type']].append({config['section_config'].get('key'): converted_data})
                    else:
                        log.error('Key for %s not found in config: %s', config['type'], config['section_config'])
                except Exception as e:
                    log.error('\nException catched when processing data for %s\n\n', pformat(config))
                    log.exception(e)
            except Exception as e:
                log.exception(e)
        except Exception as e:
            log.exception(e)
        log.debug(self.dict_result)
        return self.dict_result
