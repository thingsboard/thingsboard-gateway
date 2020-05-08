#      Copyright 2020. ThingsBoard
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

from requests import post
from uuid import uuid1
from platform import platform
from logging import getLogger
from pkg_resources import get_distribution
from threading import Thread
from time import sleep
from simplejson import loads
from base64 import b64encode

from thingsboard_gateway.tb_utility.tb_utility import TBUtility

log = getLogger("service")

UPDATE_SERVICE_BASE_URL = "https://updates.thingsboard.io"
# UPDATE_SERVICE_BASE_URL = "http://0.0.0.0:8090"


class TBUpdater(Thread):
    def __init__(self, gateway, auto_updates_enabled):
        super().__init__()
        self.__gateway = gateway
        # self.__version = get_distribution('thingsboard_gateway').version
        self.__version = "1.2.1"  #For test
        self.__instance_id = str(uuid1())
        self.__platform = "deb"
        self.__os_version = platform()
        self.__check_period = 3600
        self.__request_timeout = 5
        self.__auto_update = auto_updates_enabled
        self.start()

    def run(self):
        while True:
            self.check_for_new_version()
            sleep(self.__check_period)

    def get_version(self):
        return self.__version

    def check_for_new_version(self):
        log.debug("Checking for new version")
        request_args = self.form_request_params()
        try:
            response = post(**request_args)
            content = None
            content = loads(response.content)
            if content is not None and content.get("updateAvailable", False):
                new_version = content["message"].replace("New version ", "").replace(" is available!")
                log.info(content["message"])
                TBUtility.install_package("thingsboard-gateway", new_version)
        except Exception as e:
            log.exception(e)

    def form_request_params(self):
        json_data = {
            "version": self.__version,
            "platform": self.__platform,
            "instanceId": self.__instance_id,
            "osVersion": self.__os_version,
        }
        url = UPDATE_SERVICE_BASE_URL + "/api/tb-gateway/updates"
        request_args = {
            "url": url,
            "json": json_data,
            "timeout": self.__request_timeout

        }
        return request_args


if __name__ == '__main__':
    updater = TBUpdater("test", True)