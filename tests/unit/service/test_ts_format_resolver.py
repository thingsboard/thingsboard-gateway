#     Copyright 2025. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License"];
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import logging
from copy import deepcopy
from unittest import TestCase, main
import time

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
        new_timestamp = self.ts_resolver(data=self.data, config=first_day_config, logger=logging)
        self.assertEqual(new_timestamp, 1762770912123)

    def test_convert_with_first_year_order_config(self):
        first_year_config = deepcopy(self.config)
        first_year_config['yearfirst'] = True
        new_timestamp = self.ts_resolver(data=self.data, config=first_year_config, logger=logging)
        self.assertEqual(new_timestamp, 1290681312123)

    def test_convert_with_no_order(self):
        new_timestamp = self.ts_resolver(data=self.data, config=self.config, logger=logging)
        self.assertEqual(new_timestamp, 1760178912123)

    def test_without_timestamp(self):
        del self.config['tsField']
        del self.data['timestampField']
        self.data['ts'] = int(time.time() * 1000)
        timestamp = self.ts_resolver(data=self.data, config=self.config, logger=logging)
        self.assertEqual(timestamp, self.data['ts'])


if __name__ == '__main__':
    main()