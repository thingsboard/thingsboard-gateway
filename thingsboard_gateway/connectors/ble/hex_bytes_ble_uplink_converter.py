from pprint import pformat
from re import findall

from thingsboard_gateway.connectors.ble.ble_uplink_converter import BLEUplinkConverter
from thingsboard_gateway.gateway.statistics_service import StatisticsService


class HexBytesBLEUplinkConverter(BLEUplinkConverter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config
        dict_result = {"deviceName": config['deviceName'],
                            "deviceType": config['deviceType']
                            }

    def convert(self, config, data):
        if data is None:
            return {}

        dict_result = {}

        try:
            dict_result["telemetry"] = []
            dict_result["attributes"] = []

            for section in ('telemetry', 'attributes'):
                for item in config[section]:
                    try:
                        expression_arr = findall(r'\[\S[0-9:]*]', item['valueExpression'])
                        value = item['valueExpression']

                        for exp in expression_arr:
                            indexes = exp[1:-1].split(':')

                            value = ''
                            if len(indexes) == 2:
                                from_index, to_index = indexes
                                from_index = int(from_index) * 2 if from_index != '' else None
                                to_index = from_index + (int(to_index) * 2 - from_index) if to_index != '' and from_index != '' else None
                                concat_arr = data.hex()[from_index:to_index]
                                value = int(concat_arr, 16)
                            else:
                                value += int(data.hex()[int(indexes[0]) * 2], 16)

                            if item.get('compute', False):
                                value = eval(item['compute'], globals(), {'value': value})

                        if item.get('key') is not None:
                            dict_result[section].append({item['key']: value})
                        else:
                            self._log.error('Key for %s not found in config: %s', config['type'], config[section])
                    except Exception as e:
                        self._log.error('\nException caught when processing data for %s\n\n %s', pformat(config), e)
        except Exception as e:
            self._log.exception(e)

        self._log.debug(dict_result)
        return dict_result
