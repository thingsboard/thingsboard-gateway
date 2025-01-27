#      Copyright 2025. ThingsBoard
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

from xknx.telegram.address import GroupAddressType
from xknx.io.const import DEFAULT_MCAST_PORT, DEFAULT_MCAST_GRP
from xknx.io.connection import ConnectionConfig, ConnectionType, SecureConfig

ADDRESS_FORMAT = {
    "SHORT": GroupAddressType.SHORT,
    "LONG": GroupAddressType.LONG,
    "FREE": GroupAddressType.FREE
}

CONNECTION_TYPE = {
    "AUTOMATIC": ConnectionType.AUTOMATIC,
    "ROUTING": ConnectionType.ROUTING,
    "ROUTING_SECURE": ConnectionType.ROUTING_SECURE,
    "TUNNELING": ConnectionType.TUNNELING,
    "TUNNELING_TCP": ConnectionType.TUNNELING_TCP,
    "TUNNELING_TCP_SECURE": ConnectionType.TUNNELING_TCP_SECURE
}


class ClientConfig:
    def __init__(self, config):
        address_format_config = config.get('addressFormat', 'LONG').upper()
        self.address_format = ADDRESS_FORMAT.get(address_format_config, GroupAddressType.LONG)
        self.rate_limit = int(config.get('rateLimit', 0))
        self.multicast_group = config.get('multicastGroup', DEFAULT_MCAST_GRP)
        self.multicast_port = int(config.get('multicastPort', DEFAULT_MCAST_PORT))
        self.state_updater = bool(config.get('stateUpdater', False))
        self.connection_config = self.get_connection_config(config)

    def get_connection_config(self, config):
        security_config = self.get_security_config(config)

        connection_type = config.get('type', 'AUTOMATIC').upper()
        connection_config = ConnectionConfig(connection_type=CONNECTION_TYPE.get(connection_type, "AUTOMATIC"),
                                             individual_address=config.get('individualAddress'),
                                             local_ip=config.get('localIp'),
                                             local_port=int(config.get('localPort', 0)),
                                             gateway_ip=config.get('gatewayIp'),
                                             gateway_port=int(config.get('gatewayPort', DEFAULT_MCAST_PORT)),
                                             multicast_group=config.get('multicastGroup', DEFAULT_MCAST_GRP),
                                             multicast_port=int(config.get('multicastPort', DEFAULT_MCAST_PORT)),
                                             auto_reconnect=bool(config.get('autoReconnect', True)),
                                             auto_reconnect_wait=int(config.get('autoReconnectWait', 3)),
                                             secure_config=security_config)

        return connection_config

    def get_security_config(self, config):
        if config.get('connectionType') in ('AUTOMATIC', 'ROUTING_SECURE', 'TUNNELING_TCP_SECURE'):
            security_config_dict = config.get('security', {})
            security_config = SecureConfig(backbone_key=security_config_dict.get('backboneKey'),
                                           latency_ms=security_config_dict.get('latencyMs'),
                                           user_id=security_config_dict.get('userId'),
                                           device_authentication_password=security_config_dict.get('deviceAuthenticationPassword'),  # noqa
                                           user_password=security_config_dict.get('userPassword'),
                                           knxkeys_file_path=security_config_dict.get('knxkeysFilePath'),
                                           knxkeys_password=security_config_dict.get('knxkeysPassword'))
            return security_config

        return None
