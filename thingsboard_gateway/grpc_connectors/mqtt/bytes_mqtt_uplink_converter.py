import time
from re import findall

from simplejson import dumps

from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter, log
from thingsboard_gateway.gateway.statistics_service import StatisticsService


class BytesGrpcMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')

    @StatisticsService.CollectStatistics(start_stat_type='receivedBytesFromDevices',
                                         end_stat_type='convertedBytesFromDevice')
    def convert(self, topic, data):
        datatypes = {"attributes": "attributes",
                     "timeseries": "telemetry"}
        dict_result = {
            "deviceName": self.parse_data(self.__config['deviceNameExpression'], data),
            "deviceType": self.parse_data(self.__config['deviceTypeExpression'], data),
            "attributes": [],
            "telemetry": []
        }

        try:
            for datatype in datatypes:
                dict_result[datatypes[datatype]] = []
                for datatype_config in self.__config.get(datatype, []):
                    value_item = {datatype_config['key']: self.parse_data(datatype_config['value'], data)}
                    if datatype == 'timeseries':
                        dict_result[datatypes[datatype]].append({"ts": int(time.time()) * 1000, 'values': value_item})
                    else:
                        dict_result[datatypes[datatype]].append(value_item)
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(self.__config), str(data),
                      e)

        log.debug('Converted data: %s', dict_result)
        return dict_result

    @staticmethod
    def parse_data(expression, data):
        expression_arr = findall(r'\[\S[0-9:]*]', expression)
        converted_data = expression

        for exp in expression_arr:
            indexes = exp[1:-1].split(':')

            data_to_replace = ''
            if len(indexes) == 2:
                from_index, to_index = indexes
                concat_arr = data[
                             int(from_index) if from_index != '' else None:int(
                                 to_index) if to_index != '' else None]
                for sub_item in concat_arr:
                    data_to_replace += str(sub_item)
            else:
                data_to_replace += str(data[int(indexes[0])])

            converted_data = converted_data.replace(exp, data_to_replace)

        return converted_data
