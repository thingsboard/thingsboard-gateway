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

from xknx import XKNX
from xknx.io.gateway_scanner import GatewayScanner


class GatewaysScanner(GatewayScanner):
    def __init__(self, scanner_config, logger):
        self.__log = logger
        timeout_in_seconds = int(scanner_config.get('scanPeriod', 3))
        stop_on_found = scanner_config.get('stopOnFound', False)

        self.__client = XKNX()
        super().__init__(xknx=self.__client,
                         timeout_in_seconds=timeout_in_seconds,
                         stop_on_found=stop_on_found)

    @staticmethod
    def is_configured(client_config):
        gateways_scanner_config = client_config.get('gatewaysScanner', {})
        return gateways_scanner_config and gateways_scanner_config.get('enabled', False)

    @staticmethod
    def get_tunneling_type(gateway):
        tunnelling = (
            "Secure"
            if gateway.tunnelling_requires_secure
            else "TCP"
            if gateway.supports_tunnelling_tcp
            else "UDP"
            if gateway.supports_tunnelling
            else "No"
        )

        return tunnelling

    @staticmethod
    def get_routing_type(gateway):
        routing = (
            "Secure"
            if gateway.routing_requires_secure
            else "Yes"
            if gateway.supports_routing
            else "No"
        )

        return routing

    async def scan(self):
        self.__log.info('Starting KNX gateways scan...')

        async for found_gateway in self.async_scan():
            self.__log.info(
                'Found KNX gateway:\n'
                '- %s %s\n'
                '  %s:%s\n'
                '  Tunneling: %s\n'
                '  Routing: %s',
                found_gateway.name,
                found_gateway.individual_address,
                found_gateway.ip_addr,
                found_gateway.port,
                self.get_tunneling_type(found_gateway),
                self.get_routing_type(found_gateway)
            )

        if not self.found_gateways:
            self.__log.info('No KNX gateways found')
