
#      Copyright 2022. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

import sys

from tests.integration.integration_base_test import IntegrationBaseTest

from unittest.mock import Mock, patch
from os import path
from time import sleep
from simplejson import load

from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService
from thingsboard_gateway.gateway.tb_gateway_service import DEFAULT_CONNECTORS
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader


class ConnectorTestBase(IntegrationBaseTest):
    DATA_PATH = path.join(path.dirname(path.dirname(path.abspath(__file__))),
                          "data" + path.sep)

    def setUp(self):
        self.mock_logger = patch('thingsboard_gateway.tb_utility.tb_logger.TbLogger').start()
        self.gateway = Mock(spec=TBGatewayService)
        self.gateway.log = self.mock_logger
        self.connector = None
        self.config = None

    def tearDown(self):
        patch.stopall()
        self.connector.close()

    def _load_data_file(self, filename, connector_type):
        config_file_path = self.DATA_PATH + connector_type + path.sep + filename
        datafile = None
        with open(config_file_path, "r", encoding="UTF-8") as required_file:
            datafile = load(required_file)
        return datafile

    def _create_connector(self, config_filename, connector_type=None):
        if connector_type is None:
            class_name = self.__class__.__name__.lower()
            connector_type = class_name[0:class_name.find("connector")]
        self._connector_type = connector_type
        self.config = self._load_data_file(config_filename, connector_type)
        self.assertTrue(self.config is not None)
        connector = TBModuleLoader.import_module(connector_type, DEFAULT_CONNECTORS[connector_type])
        self.assertTrue(connector is not None)
        self.connector = connector(self.gateway, self.config, connector_type)
        sleep(1)
