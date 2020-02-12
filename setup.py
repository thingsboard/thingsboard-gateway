#     Copyright 2019. ThingsBoard
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

from setuptools import setup

setup(
    long_description_content_type="text/markdown",
    packages=['thingsboard_gateway', 'thingsboard_gateway.gateway', 'thingsboard_gateway.storage',
              'thingsboard_gateway.tb_client', 'thingsboard_gateway.connectors', 'thingsboard_gateway.connectors.ble',
              'thingsboard_gateway.connectors.mqtt', 'thingsboard_gateway.connectors.opcua', 'thingsboard_gateway.connectors.request',
              'thingsboard_gateway.connectors.modbus', 'thingsboard_gateway.tb_utility', 'thingsboard_gateway.extensions',
              'thingsboard_gateway.extensions.mqtt', 'thingsboard_gateway.extensions.modbus', 'thingsboard_gateway.extensions.opcua',
              'thingsboard_gateway.extensions.ble', 'thingsboard_gateway.extensions.serial', 'thingsboard_gateway.extensions.request'
              ],
    install_requires=[
        'cffi',
        'jsonpath-rw',
        'pip',
        'jsonschema==3.1.1',
        'lxml',
        'opcua',
        'paho-mqtt',
        'pymodbus',
        'pyserial',
        'pytz',
        'PyYAML',
        'simplejson',
        'pyrsistent'
    ],
    download_url='https://github.com/thingsboard/thingsboard-gateway/archive/2.1.0.5.tar.gz',
    entry_points={
        'console_scripts': [
            'thingsboard-gateway = thingsboard_gateway.tb_gateway:daemon'
        ]
    })



