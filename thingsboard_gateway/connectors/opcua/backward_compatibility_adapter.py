from copy import deepcopy
import re
from typing import List

from thingsboard_gateway.tb_utility.tb_logger import TbLogger


class BackwardCompatibilityAdapter:
    DATA_TYPES = {
        'int': 'integer',
        'str': 'string',
        'float': 'double',
        'bool': 'boolean',
    }

    def __init__(self, config, logger: TbLogger):
        self._config = deepcopy(config)
        self._log = logger

    def convert(self):
        errors = self.validate_incoming_config(self._config)
        if errors:
            for error in errors:
                self._log.error("Found error in incoming configuration (Config will be ignored): %s", error)
        disable_subscriptions = self._config['server'].pop('disableSubscriptions', None)
        if disable_subscriptions is not None:
            self._config['server']['enableSubscriptions'] = not disable_subscriptions

        mapping_configuration = deepcopy(self._config.get('server', {}).get('mapping', []))
        if not mapping_configuration:
            return self._config

        for node_config in mapping_configuration:
            try:
                node_config['deviceNodeSource'] = self.get_value_source(node_config['deviceNodePattern'], False)

                device_type_pattern = node_config.pop('deviceTypePattern', 'default')
                device_name_pattern = node_config.pop('deviceNamePattern', None)
                node_config['deviceInfo'] = {
                    'deviceNameExpressionSource': self.get_value_source(device_name_pattern),
                    'deviceNameExpression': device_name_pattern,
                    'deviceProfileExpressionSource': self.get_value_source(device_type_pattern),
                    'deviceProfileExpression': device_type_pattern
                }

                mapping_datapoints_configurations = {}

                # converting attributes and timeseries sections
                for config_section_name in ('attributes', 'timeseries'):
                    mapping_datapoints_configurations[config_section_name] = []
                    for section_config in node_config.get(config_section_name, []):
                        try:
                            path = section_config.pop('path', None)

                            section_config['type'] = self.get_value_source(path)
                            section_config['value'] = path
                            mapping_datapoints_configurations[config_section_name].append(section_config)
                        except Exception as e:
                            pass

                    node_config[config_section_name] = mapping_datapoints_configurations[config_section_name]

                # converting attributes_updates section
                for config in node_config.get('attributes_updates', []):
                    attribute_on_tb = config.pop('attributeOnThingsBoard', None)
                    attribute_on_device = config.pop('attributeOnDevice', None)

                    config['key'] = attribute_on_tb
                    config['type'] = self.get_value_source(attribute_on_device)
                    config['value'] = attribute_on_device

                # converting rpc section
                for config in node_config.get('rpc_methods', []):
                    arguments = config.pop('arguments', [])
                    config['arguments'] = []

                    for arg in arguments:
                        converted_argument = {
                            'type': self.DATA_TYPES.get(type(arg).__name__, 'string'),
                            'value': arg
                        }
                        config['arguments'].append(converted_argument)
            except Exception as e:
                self._log.error('Error during conversion to new configuration format: %r', e)
                self._log.trace('Config section: %r', node_config)

        # Removing old mapping section
        self._config['server'].pop('mapping')
        # Adding new mapping section
        self._config['mapping'] = mapping_configuration

        return self._config

    @staticmethod
    def get_value_source(value, possible_constant=True):
        if re.search(r"(ns=\d+;[isgb]=[^}]+)", value):
            return 'identifier'
        elif re.search(r"\${([A-Za-z.:\\\d]+)}", value) or not possible_constant:
            return 'path'
        else:
            return 'constant'

    @staticmethod
    def validate_incoming_config(config) -> List[str]:
        errors = []
        if 'server' not in config:
            errors.append('No server configuration found')
        if 'mapping' not in config['server'] or not config['server']['mapping']:
            errors.append('No mapping configuration found')
        else:
            for index, node_config in enumerate(config['server']['mapping']):
                object_number = index + 1
                if 'deviceNodePattern' not in node_config:
                    errors.append('No "deviceNodePattern" found in %i object in mapping configuration' % object_number)
                if 'deviceNamePattern' not in node_config:
                    errors.append('No "deviceNamePattern" found in %i object in mapping configuration' % object_number)
                if 'attributes' not in node_config:
                    errors.append('No "attributes" found in %i object in mapping configuration' % object_number)
                else:
                    for attribute_number, attribute_config in enumerate(node_config['attributes']):
                        attribute_errors = BackwardCompatibilityAdapter.validate_datapoint_config(attribute_config)
                        if attribute_errors:
                            for error in attribute_errors:
                                errors.append('Attribute configuration with number %i in %i mapping object has the following error: %s' % (attribute_number + 1, object_number, error))

                if 'timeseries' not in node_config:
                    errors.append('No "timeseries" found in %i object in mapping configuration' % object_number)
                else:
                    for timeseries_number, timeseries_config in enumerate(node_config['timeseries']):
                        timeseries_errors = BackwardCompatibilityAdapter.validate_datapoint_config(timeseries_config)
                        if timeseries_errors:
                            for error in timeseries_errors:
                                errors.append('Timeseries configuration with number %i in %i mapping object has the following error: %s' % (timeseries_number + 1, object_number, error))
        return errors

    @staticmethod
    def validate_datapoint_config(config) -> List[str]:
        errors = []
        if 'path' not in config:
            errors.append('No "path" found in configuration')
        if 'key' not in config:
            errors.append('No "key" found in configuration')
        return errors
