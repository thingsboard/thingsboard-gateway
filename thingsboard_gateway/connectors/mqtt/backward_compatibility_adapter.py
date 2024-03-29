from copy import copy


class BackwardCompatibilityAdapter:
    def __init__(self, config):
        self._config = copy(config)

    def convert(self):
        self._config['requestsMapping'] = {}
        self._config['dataMapping'] = {}
        mapping = {'requestsMapping': ('connectRequests', 'disconnectRequests', 'attributeRequests',
                                       'attributeUpdates', 'serverSideRpc'),
                   'dataMapping': ('mapping', )}

        for (map_section, map_type) in mapping.items():
            for t in map_type:
                try:
                    section_config = self._config.pop(t, None)
                    if isinstance(section_config, list):
                        for item in section_config:
                            (device_name_json_expression, device_type_json_expression, device_name_topic_expression,
                             device_type_topic_expression) = self._get_device_name_and_type(item)

                            self._parce_device_info(item, device_name_json_expression, device_type_json_expression,
                                                    device_name_topic_expression, device_type_topic_expression)
                    else:
                        (device_name_json_expression, device_type_json_expression, device_name_topic_expression,
                         device_type_topic_expression) = self._get_device_name_and_type(section_config)

                        self._parce_device_info(section_config, device_name_json_expression,
                                                device_type_json_expression,
                                                device_name_topic_expression, device_type_topic_expression)

                    if t == 'mapping':
                        self._config[map_section] = section_config
                    else:
                        self._config[map_section][t] = section_config
                except KeyError:
                    continue

        return self._config

    @staticmethod
    def _get_device_name_and_type(config):
        device_name_json_expression = config.get('converter', config).pop('deviceNameJsonExpression', None)
        device_type_json_expression = config.get('converter', config).pop('deviceTypeJsonExpression', None)

        device_name_topic_expression = config.get('converter', config).pop('deviceNameTopicExpression', None)
        device_type_topic_expression = config.get('converter', config).pop('deviceTypeTopicExpression', None)

        return (device_name_json_expression, device_type_json_expression,
                device_name_topic_expression, device_type_topic_expression)

    @staticmethod
    def _parce_device_info(config, device_name_json_expression=None, device_type_json_expression=None,
                           device_name_topic_expression=None, device_type_topic_expression=None):
        if device_name_json_expression:
            config['deviceNameExpressionSource'] = 'message'
            config['deviceNameExpression'] = device_name_json_expression

        if device_type_json_expression:
            config['deviceProfileExpressionSource'] = 'message'
            config['deviceProfileExpression'] = device_type_json_expression

        if device_name_topic_expression:
            config['deviceNameExpressionSource'] = 'topic'
            config['deviceNameExpression'] = device_name_topic_expression

        if device_type_topic_expression:
            config['deviceProfileExpressionSource'] = 'topic'
            config['deviceProfileExpression'] = device_type_topic_expression

        if config.get('converter', {}).get('extension-config'):
            extension_config = config.get('converter').pop('extension-config')
            config['converter']['extensionConfig'] = extension_config

    @staticmethod
    def is_old_config_format(config):
        return config.get('mapping') is not None
