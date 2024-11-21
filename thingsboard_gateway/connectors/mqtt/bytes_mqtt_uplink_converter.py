import time
from re import findall

from simplejson import dumps

from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class BytesMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config, logger):
        self.__config = config.get('converter')
        self._log = logger

    @property
    def config(self):
        return self.__config

    @config.setter
    def config(self, value):
        self.__config = value

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                       end_stat_type='convertedBytesFromDevice')
    def convert(self, topic, data):
        StatisticsService.count_connector_message(self._log.name, 'convertersMsgProcessed')

        datatypes = {"attributes": "attributes",
                     "timeseries": "telemetry"}

        device_name = self.parse_data(self.__config['deviceInfo']['deviceNameExpression'], data)

        device_report_strategy = None
        try:
            device_report_strategy = ReportStrategyConfig(self.__config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s: %s", device_name, e)

        converted_data = ConvertedData(device_name=device_name,
                                       device_type=self.parse_data(self.__config['deviceInfo']['deviceProfileExpression'], data))
        timestamp = int(time.time() * 1000)
        try:
            for datatype in datatypes:
                for datatype_config in self.__config.get(datatype, []):
                    key = self.parse_data(datatype_config['key'], data)
                    value = self.parse_data(datatype_config['value'], data)
                    datapoint_key = TBUtility.convert_key_to_datapoint_key(key,
                                                                           device_report_strategy,
                                                                           datatype_config,
                                                                           self._log)
                    if datatype == 'timeseries':
                        converted_data.add_to_telemetry(TelemetryEntry({datapoint_key: value}, ts=timestamp))
                    else:
                        converted_data.add_to_attributes({datapoint_key: value})
        except Exception as e:
            self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s',
                            dumps(self.__config),
                            str(data), e)
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')

        self._log.debug('Converted data: %s', converted_data)

        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)

        return converted_data

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
