# -*- coding: utf-8 -*-

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

from setuptools import setup
from os import path

from thingsboard_gateway import version

current_directory = path.abspath(path.dirname(__file__))
with open(path.join(current_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    version=version.VERSION,
    name="thingsboard-gateway",
    author="ThingsBoard",
    author_email="info@thingsboard.io",
    license="Apache Software License (Apache Software License 2.0)",
    description="Thingsboard Gateway for IoT devices.",
    url="https://github.com/thingsboard/thingsboard-gateway",
    long_description=long_description,
    long_description_content_type="text/markdown",
    include_package_data=True,
    python_requires=">=3.7",
    packages=['thingsboard_gateway', 'thingsboard_gateway.gateway', 'thingsboard_gateway.gateway.proto', 'thingsboard_gateway.gateway.grpc_service',
              'thingsboard_gateway.storage', 'thingsboard_gateway.storage.memory', 'thingsboard_gateway.gateway.shell',
              'thingsboard_gateway.storage.file', 'thingsboard_gateway.storage.sqlite',
              'thingsboard_gateway.connectors', 'thingsboard_gateway.connectors.ble', 'thingsboard_gateway.connectors.socket',
              'thingsboard_gateway.connectors.mqtt',  'thingsboard_gateway.connectors.opcua_asyncio', 'thingsboard_gateway.connectors.xmpp',
              'thingsboard_gateway.connectors.opcua', 'thingsboard_gateway.connectors.request', 'thingsboard_gateway.connectors.ocpp',
              'thingsboard_gateway.connectors.modbus', 'thingsboard_gateway.connectors.can', 'thingsboard_gateway.connectors.bacnet',
              'thingsboard_gateway.connectors.bacnet.bacnet_utilities', 'thingsboard_gateway.connectors.odbc',
              'thingsboard_gateway.connectors.rest', 'thingsboard_gateway.connectors.snmp', 'thingsboard_gateway.connectors.ftp',
              'thingsboard_gateway.tb_utility', 'thingsboard_gateway.extensions',
              'thingsboard_gateway.extensions.mqtt', 'thingsboard_gateway.extensions.modbus', 'thingsboard_gateway.extensions.opcua',
              'thingsboard_gateway.extensions.opcua_asyncio', 'thingsboard_gateway.extensions.ocpp',
              'thingsboard_gateway.extensions.ble', 'thingsboard_gateway.extensions.serial', 'thingsboard_gateway.extensions.request',
              'thingsboard_gateway.extensions.can', 'thingsboard_gateway.extensions.bacnet', 'thingsboard_gateway.extensions.odbc',
              'thingsboard_gateway.extensions.rest',  'thingsboard_gateway.extensions.snmp', 'thingsboard_gateway.extensions.ftp',
              'thingsboard_gateway.extensions.socket', 'thingsboard_gateway.extensions.xmpp',
              ],
    install_requires=[
        'cryptography==3.4.*',
        'jsonpath-rw',
        'regex',
        'pip',
        'PyYAML',
        'simplejson',
        'urllib3<=1.26.15',
        'requests>=2.31.0',
        'questionary',
        'pyfiglet',
        'termcolor',
        'mmh3',
        'grpcio==1.58.0',
        'protobuf',
        'cachetools',
        'tb-mqtt-client>=1.9.1',
        'packaging==23.1',
        'service-identity'
    ],
    download_url='https://github.com/thingsboard/thingsboard-gateway/archive/%s.tar.gz' % version.VERSION,
    entry_points={
        'console_scripts': [
            'thingsboard-gateway = thingsboard_gateway.tb_gateway:daemon',
            'tb-gateway-configurator = thingsboard_gateway.gateway.configuration_wizard:configure',
            'tb-gateway-shell = thingsboard_gateway.gateway.shell:main'
        ]
    })
