#     Copyright 2026. ThingsBoard
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

from typing import Optional

from snap7 import Logo


class LogoClient(Logo):
    def __init__(self,
                 auto_reconnect: bool = True,
                 max_retries: int = 3,
                 retry_delay: float = 1.0,
                 max_delay: float = 10.0,
                 heartbeat_interval: float = 30.0) -> None:
        super(Logo, self).__init__(auto_reconnect=auto_reconnect,
                                   max_retries=max_retries,
                                   retry_delay=retry_delay,
                                   max_delay=max_delay,
                                   heartbeat_interval=heartbeat_interval)
        self._logo_tsap_snap7: Optional[int] = None
        self._logo_tsap_logo: Optional[int] = None
