from copy import copy


class BackwardCompatibilityAdapter:
    def __init__(self, config):
        self._config = copy(config)

    def convert(self):
        self._config['requestsMapping'] = {}
        self._config['dataMapping'] = []
        mapping = {'requestsMapping': ('connectRequests', 'disconnectRequests', 'attributeRequests',
                                       'attributeUpdates', 'serverSideRpc'),
                   'dataMapping': ('mapping', )}

        for (map_section, map_type) in mapping.items():
            for t in map_type:
                section_config = self._config.pop(t, {})
                try:
                    for item in section_config:
                        (device_name_json_expression, device_type_json_expression, device_name_topic_expression,
                         device_type_topic_expression, bytes_converter) = self._get_device_name_and_type(item)

                        self._parce_device_info(item['converter'] if item.get('converter') else item, device_name_json_expression,
                                                device_type_json_expression,
                                                device_name_topic_expression, device_type_topic_expression,
                                                bytes_converter)

                        if t == 'attributeRequests':
                            self._parce_attribute_info(item)

                    if t == 'mapping':
                        self._config[map_section] = section_config
                    else:
                        self._config[map_section][t] = section_config
                except KeyError:
                    continue
                except AttributeError as e:
                    print('Can\'t parce config section "{}", subsection "{}". '
                          'Please, check your configuration.\nError: {}'
                          .format(t, section_config, e))
                    continue

        return self._config

    @staticmethod
    def _get_device_name_and_type(config):
        if config.get('converter', {}).get('deviceNameExpression') and config.get('converter', {}).get(
                'deviceTypeExpression'):
            device_name_json_expression = config.get('converter', {}).pop('deviceNameExpression', None)
            device_type_json_expression = config.get('converter', {}).pop('deviceTypeExpression', None)

            return device_name_json_expression, device_type_json_expression, None, None, True

        device_name_json_expression = config.get('converter', config).pop('deviceNameJsonExpression', None)
        device_type_json_expression = config.get('converter', config).pop('deviceTypeJsonExpression', None)

        device_name_topic_expression = config.get('converter', config).pop('deviceNameTopicExpression', None)
        device_type_topic_expression = config.get('converter', config).pop('deviceTypeTopicExpression', None)

        return (device_name_json_expression, device_type_json_expression,
                device_name_topic_expression, device_type_topic_expression, False)

    @staticmethod
    def _parce_device_info(config, device_name_json_expression=None, device_type_json_expression=None,
                           device_name_topic_expression=None, device_type_topic_expression=None, bytes_converter=False):
        if bytes_converter:
            config['deviceInfo'] = {}
            config['deviceInfo']['deviceNameExpression'] = device_name_json_expression
            config['deviceInfo']['deviceProfileExpression'] = device_type_json_expression
            return

        if device_name_json_expression:
            if config.get('deviceInfo') is None:
                config['deviceInfo'] = {}

            config['deviceInfo']['deviceNameExpressionSource'] = 'message'
            config['deviceInfo']['deviceNameExpression'] = device_name_json_expression

        if device_type_json_expression:
            config['deviceInfo']['deviceProfileExpressionSource'] = 'message'
            config['deviceInfo']['deviceProfileExpression'] = device_type_json_expression

        if device_name_topic_expression:
            if config.get('deviceInfo') is None:
                config['deviceInfo'] = {}

            config['deviceInfo']['deviceNameExpressionSource'] = 'topic'
            config['deviceInfo']['deviceNameExpression'] = device_name_topic_expression

        if device_type_topic_expression:
            config['deviceInfo']['deviceProfileExpressionSource'] = 'topic'
            config['deviceInfo']['deviceProfileExpression'] = device_type_topic_expression

        if config.get('extension-config'):
            extension_config = config.pop('extension-config')
            config['extensionConfig'] = extension_config

    @staticmethod
    def _parce_attribute_info(config):
        attribute_name_expression = config.pop('attributeNameJsonExpression', None)
        if attribute_name_expression:
            config['attributeNameExpressionSource'] = 'message'
            config['attributeNameExpression'] = attribute_name_expression
            return

        attribute_name_expression = config.pop('attributeNameTopicExpression', None)
        if attribute_name_expression:
            config['attributeNameExpressionSource'] = 'topic'
            config['attributeNameExpression'] = attribute_name_expression

    @staticmethod
    def is_old_config_format(config):
        return config.get('mapping') is not None
