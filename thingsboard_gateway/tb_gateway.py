#     Copyright 2021. ThingsBoard
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

from os import path, listdir, mkdir, curdir
from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService


def main():
    if "logs" not in listdir(curdir):
        mkdir("logs")
    TBGatewayService(path.dirname(path.abspath(__file__)) + '/config/tb_gateway.yaml'.replace('/', path.sep))


def daemon():
    TBGatewayService("/etc/thingsboard-gateway/config/tb_gateway.yaml".replace('/', path.sep))


if __name__ == '__main__':
    main()
