from copy import deepcopy


class BackwardCompatibilityAdapter:
    def __init__(self, config):
        self._config = deepcopy(config)

    def convert(self):
        self._config['requestsMapping'] = {}
        config_mapping = {'requestsMapping': ('connectRequests', 'disconnectRequests', 'attributeRequests',
                                       'attributeUpdates', 'serverSideRpc'),
                   'mapping': ('mapping', 'dataMapping')}

        has_mapping_section = False
        for (map_section, map_type) in config_mapping.items():
            for t in map_type:
                section_config = self._config.pop(t, {})
                try:
                    for item in section_config:
                        (device_name_json_expression, device_type_json_expression, device_name_topic_expression,
                         device_type_topic_expression, bytes_converter) = self._get_device_name_and_type(item)

                        self._parse_device_info(item['converter'] if item.get('converter') else item, device_name_json_expression,
                                                device_type_json_expression,
                                                device_name_topic_expression, device_type_topic_expression,
                                                bytes_converter)

                        if t == 'attributeRequests':
                            self._parce_attribute_info(item)
                    if not has_mapping_section and (t == 'mapping' or t == 'dataMapping') and section_config:
                        self._config[map_section] = section_config
                        has_mapping_section = True
                        break
                    else:
                        self._config[map_section][t] = section_config
                except KeyError:
                    continue
                except AttributeError as e:
                    print('Can\'t parce config section "{}", subsection "{}". '
                          'Please, check your configuration.\nError: {}'
                          .format(t, section_config, e))
                    continue

        if not self._config.get('mapping'):
            self._config['mapping'] = []

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
    def _parse_device_info(config, device_name_json_expression=None, device_type_json_expression=None,
                           device_name_topic_expression=None, device_type_topic_expression=None, bytes_converter=False):
        if bytes_converter:
            config['deviceInfo'] = {}
            config['deviceInfo']['deviceNameExpression'] = device_name_json_expression
            config['deviceInfo']['deviceProfileExpression'] = device_type_json_expression
            return
        if config.get('deviceInfo') is None:
            config['deviceInfo'] = {}

        if device_name_json_expression:
            config['deviceInfo']['deviceNameExpressionSource'] = 'message'
            config['deviceInfo']['deviceNameExpression'] = device_name_json_expression

        if device_type_json_expression:
            config['deviceInfo']['deviceProfileExpressionSource'] = 'message'
            config['deviceInfo']['deviceProfileExpression'] = device_type_json_expression

        if device_name_topic_expression:
            config['deviceInfo']['deviceNameExpressionSource'] = 'topic'
            config['deviceInfo']['deviceNameExpression'] = device_name_topic_expression

        if device_type_topic_expression:
            config['deviceInfo']['deviceProfileExpressionSource'] = 'topic'
            config['deviceInfo']['deviceProfileExpression'] = device_type_topic_expression

        if config.get('extension-config'):
            extension_config = config.pop('extension-config')
            config['extensionConfig'] = extension_config

        if len(config['deviceInfo']) == 0:
            config.pop('deviceInfo')

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
        mapping = config.get('mapping')
        return mapping is not None and len(mapping) > 0 and mapping[0].get('converter', {}).get('deviceInfo') is None
