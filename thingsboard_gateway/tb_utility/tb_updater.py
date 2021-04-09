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

from requests import post, ConnectionError
from uuid import uuid1
from platform import platform, system, release
from logging import getLogger
from pkg_resources import get_distribution
from threading import Thread
from time import time, sleep
from simplejson import loads

from thingsboard_gateway.tb_utility.tb_utility import TBUtility

log = getLogger("service")

UPDATE_SERVICE_BASE_URL = "https://updates.thingsboard.io"


class TBUpdater(Thread):
    def __init__(self):
        super().__init__()
        self.__version = {"current_version": get_distribution('thingsboard_gateway').version,
                          "latest_version": get_distribution('thingsboard_gateway').version}
        self.__instance_id = str(uuid1())
        self.__platform = system()
        self.__release = release()
        self.__os_version = platform()
        self.__previous_check = 0
        self.__check_period = 3600.0
        self.__request_timeout = 5
        self.__stopped = True
        self.start()

    def run(self):
        self.__stopped = False
        while not self.__stopped:
            if time() >= self.__previous_check + self.__check_period:
                self.check_for_new_version()
                self.__previous_check = time()
            else:
                sleep(1)

    def stop(self):
        self.__stopped = True

    def get_version(self):
        return self.__version

    def get_platform(self):
        return self.__platform

    def get_release(self):
        return self.__release

    def check_for_new_version(self):
        log.debug("Checking for new version")
        request_args = self.form_request_params()
        try:
            response = post(**request_args)
            content = None
            content = loads(response.content)
            if content is not None and content.get("updateAvailable", False):
                new_version = content["message"].replace("New version ", "").replace(" is available!", "")
                if new_version > self.__version["current_version"]:
                    log.info(content["message"])
                    self.__version["latest_version"] = new_version
                    log.info("\n\n[===UPDATE===]\n\n New version %s is available! \n\n[===UPDATE===]\n",
                             self.__version["latest_version"])
        except ConnectionRefusedError:
            log.warning("Cannot connect to the update service. PLease check your internet connection.")
        except ConnectionError:
            log.warning("Cannot connect to the update service. PLease check your internet connection.")
        except Exception as e:
            log.exception(e)

    def form_request_params(self):
        json_data = {
            "version": self.__version["current_version"],
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

    def update(self):
        if self.__version["latest_version"] != self.__version["current_version"]:
            result = TBUtility.install_package("thingsboard-gateway", self.__version["latest_version"])
        else:
            result = "Congratulations! You have the latest version."
        return result

if __name__ == '__main__':
    updater = TBUpdater()
