import logging
from copy import deepcopy
from unittest import TestCase, main
import time
from dateutil import parser

from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class TestTSFormatResolver(TestCase):

    def setUp(self):
        self.config = {'dayfirst': False, 'key': 'temperature', 'tsField': '${timestampField}', 'type': 'double',
                       'value': '${temperature}', 'yearfirst': False}
        self.data = {'temperature': 25, 'timestampField': '10.11.25 10:35:12.123'}
        self.ts_resolver = TBUtility.resolve_different_ts_formats

    def test_convert_with_first_day_order_config(self):
        first_day_config = deepcopy(self.config)
        first_day_config['dayfirst'] = True
        parsed_timestamp = parser.parse(self.data.get('timestampField'), dayfirst=True)
        expected_ts = int(parsed_timestamp.timestamp() * 1000)
        new_timestamp = self.ts_resolver(data=self.data, config=first_day_config, logger=logging)
        self.assertEqual(new_timestamp,  expected_ts)

    def test_convert_with_first_year_order_config(self):
        first_year_config = deepcopy(self.config)
        first_year_config['yearfirst'] = True
        parsed_timestamp = parser.parse(self.data.get('timestampField'), yearfirst=True)
        expected_ts = int(parsed_timestamp.timestamp() * 1000)
        new_timestamp = self.ts_resolver(data=self.data, config=first_year_config, logger=logging)
        self.assertEqual(new_timestamp, expected_ts)

    def test_convert_with_no_order(self):
        parsed_timestamp = parser.parse(self.data.get('timestampField'))
        expected_ts = int(parsed_timestamp.timestamp() * 1000)
        new_timestamp = self.ts_resolver(data=self.data, config=self.config, logger=logging)
        self.assertEqual(new_timestamp, expected_ts)

    def test_without_timestamp(self):
        del self.config['tsField']
        del self.data['timestampField']
        self.data['ts'] = int(time.time() * 1000)
        timestamp = self.ts_resolver(data=self.data, config=self.config, logger=logging)
        self.assertEqual(timestamp, self.data['ts'])


if __name__ == '__main__':
    main()

