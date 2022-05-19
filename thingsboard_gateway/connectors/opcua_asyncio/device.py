import re
from thingsboard_gateway.connectors.connector import log


class Device:
    def __init__(self, path, name, config, converter, converter_for_sub):
        self.path = path
        self.name = name
        self.config = config
        self.converter = converter
        self.converter_for_sub = converter_for_sub
        self.values = {
            'timeseries': [],
            'attributes': []
        }

        self.__load_values()

    def __load_values(self):
        for section in ('attributes', 'timeseries'):
            for node_config in self.config.get(section, []):
                try:
                    if child := re.search(r"(ns=\d*;[isgb]=.*\d)", node_config['path']):
                        self.values[section].append({'path': child.groups()[0], 'key': node_config['key']})
                    elif child := re.search(r"\${([A-Za-z.:\d]*)}", node_config['path']):
                        self.values[section].append(
                            {'path': self.path + child.groups()[0].split('.'), 'key': node_config['key']})

                except KeyError as e:
                    log.error('Invalid config for %s (key %s not found)', node_config, e)
