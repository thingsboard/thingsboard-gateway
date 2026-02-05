# ------------------------------------------------------------------------------
#      Copyright 2026. ThingsBoard
#
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.
#
# ------------------------------------------------------------------------------

from time import time
from simplejson import dumps
from thingsboard_gateway.connectors.ftp.ftp_converter import FTPConverter
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class CustomFTPtUplinkConverter(FTPConverter):

    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    @property
    def config(self):
        return self.__config

    def convert(self, config, data):
        pass
