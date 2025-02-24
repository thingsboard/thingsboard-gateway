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

import sys
from os import curdir, listdir, mkdir, path, environ

from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService
from thingsboard_gateway.gateway.hot_reloader import HotReloader


def main():
    if "logs" not in listdir(curdir):
        mkdir("logs")

    try:
        hot_reload = bool(sys.argv[1])
    except IndexError:
        hot_reload = False

    if hot_reload:
        HotReloader(TBGatewayService)
    else:
        config_path = __get_config_path(path.dirname(path.abspath(__file__)) + '/config/'.replace('/', path.sep))
        TBGatewayService(config_path + 'tb_gateway.json')


def daemon():
    config_path = __get_config_path("/etc/thingsboard-gateway/config/".replace('/', path.sep))
    TBGatewayService(config_path + "tb_gateway.json")


def __get_config_path(default_config_path):
    config_path = environ.get("TB_GW_CONFIG_DIR", default_config_path)
    if not config_path.endswith(path.sep):
        config_path += path.sep
    return config_path


if __name__ == '__main__':
    main()
