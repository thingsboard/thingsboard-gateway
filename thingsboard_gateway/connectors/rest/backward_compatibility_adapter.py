from copy import deepcopy
import re
from typing import List

from thingsboard_gateway.tb_utility.tb_logger import TbLogger


class BackwardCompatibilityAdapter:

    def __init__(self, config, logger: TbLogger):
        self._config = deepcopy(config)
        self._log = logger

    def convert(self):
        # Move server-related keys under 'server'
        server_keys = {'host', 'port', 'SSL', 'security'}
        self._config['server'] = {k: self._config.pop(k) for k in list(self._config) if k in server_keys}

        # Process and convert 'mapping' section
        old_mappings = self._config.get('mapping')

        new_mappings = []
        if isinstance(old_mappings, list):
            for mapping in deepcopy(old_mappings):
                converter = mapping.get('converter', {})
                if not isinstance(converter, dict):
                    self._log.error("Invalid converter format in mapping: %r", mapping)
                    continue

                # Convert device info block
                device_name_expr = converter.pop('deviceNameExpression', None)
                device_type_expr = converter.pop('deviceTypeExpression', 'default')

                converter['deviceInfo'] = {
                    'deviceNameExpressionSource': self.get_value_source(device_name_expr),
                    'deviceNameExpression': device_name_expr,
                    'deviceProfileExpressionSource': self.get_value_source(device_type_expr),
                    'deviceProfileExpression': device_type_expr
                }

                # Rename extension-config → extensionConfig (for custom converters)
                if 'extension-config' in converter:
                    converter['extensionConfig'] = converter.pop('extension-config')

                mapping['converter'] = converter
                new_mappings.append(mapping)
        else:
            self._log.error("Invalid 'mapping' format: %r", old_mappings)

        self._config['mapping'] = new_mappings

        # Move requests-related sections under 'requestsMapping'
        requests_mapping = {}
        for key in ('attributeRequests', 'attributeUpdates', 'serverSideRpc'):
            value = self._config.pop(key, None)
            if value is not None:
                requests_mapping[key] = value

        if isinstance(self._config.get('requestsMapping'), list):
            self._config['requestsMapping'].extend(requests_mapping)
        else:
            self._config['requestsMapping'] = requests_mapping

        return self._config

    @staticmethod
    def get_value_source(value, possible_constant=True):
        if re.search(r"\${([A-Za-z.:\\\d]+)}", value) or not possible_constant:
            return 'request'
        else:
            return 'constant'

    @staticmethod
    def is_old_config(config):
        return 'server' not in config or 'requestsMapping' not in config
