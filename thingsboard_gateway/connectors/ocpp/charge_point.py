#     Copyright 2026. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
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

import simplejson
from ocpp.v21 import ChargePoint as CP
from ocpp.routing import on
from ocpp.v21.enums import Action, RegistrationStatusEnumType, DataTransferStatusEnumType
from ocpp.v21 import call_result
from datetime import datetime, timezone

from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader


class ChargePoint(CP):
    MEASURAND_KEYS = {
        'SoC': 'soc',
        'Energy.Active.Import.Register': 'energy_active_import_register',
        'Power.Active.Import': 'power_active_import',
    }

    def __init__(self, charge_point_id, websocket, config, callback, logger):
        super(ChargePoint, self).__init__(charge_point_id, websocket)
        self._log = logger
        self._config = config
        self._callback = callback
        self._uplink_converter = self._load_converter(config['uplink_converter_name'])(self._config, self._log)
        self._profile = {}
        self.name = None
        self.type = None
        self._authorized = False
        self._stopped = False

    @property
    def config(self):
        return self._config

    @property
    def authorized(self):
        return self._authorized

    @authorized.setter
    def authorized(self, is_auth: bool):
        self._authorized = is_auth

    async def start(self):
        while not self._stopped:
            message = await self._connection.recv()

            await self.route_message(message)

    @staticmethod
    def _load_converter(converter_name):
        return TBModuleLoader.import_module('ocpp', converter_name)

    async def close(self):
        self._stopped = True
        return await self._connection.close()

    @staticmethod
    def _to_epoch_ms(iso_timestamp):
        try:
            return int(datetime.fromisoformat(iso_timestamp).timestamp() * 1000)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _extract_measurands(cls, data, meter_values):
        for meter_value in meter_values or []:
            for sampled_value in meter_value.get('sampled_value') or []:
                key = cls.MEASURAND_KEYS.get(sampled_value.get('measurand'))
                if key:
                    data[key] = sampled_value.get('value')

    @on(Action.boot_notification)
    def on_boot_notification(self, charging_station, reason, **kwargs):
        self._profile = {
            'Vendor': charging_station.get('vendor_name'),
            'Model': charging_station.get('model')
        }
        self.name = self._uplink_converter.get_device_name(self._profile)
        self.type = self._uplink_converter.get_device_type(self._profile)

        self._callback((self._uplink_converter,
                        {'deviceName': self.name, 'deviceType': self.type, 'messageType': Action.boot_notification,
                         'profile': self._profile},
                        {'reason': reason, **charging_station, **kwargs}))

        return call_result.BootNotification(
            current_time=datetime.now(timezone.utc).isoformat(),
            interval=10,
            status=RegistrationStatusEnumType.accepted
        )

    @on(Action.authorize)
    def on_authorize(self, id_token, **kwargs):
        if self.authorized:
            return call_result.Authorize(id_token_info={'status': 'Accepted'})

        return call_result.Authorize(id_token_info={'status': 'Not authorized'})

    @on(Action.heartbeat)
    @on(Action.heartbeat)
    def on_heartbeat(self):
        current_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S') + "Z"

        self._callback((self._uplink_converter,
                        {'deviceName': self.name, 'deviceType': self.type, 'messageType': Action.heartbeat,
                         'profile': self._profile}, {'current_time': current_time}))

        return call_result.Heartbeat(current_time=current_time)

    @on(Action.data_transfer)
    @on(Action.data_transfer)
    def on_data_transfer(self, **kwargs):
        for (key, value) in kwargs.items():
            try:
                kwargs[key] = simplejson.loads(value)
            except (TypeError, ValueError):
                continue

        self._callback((self._uplink_converter,
                        {'deviceName': self.name, 'deviceType': self.type, 'messageType': Action.data_transfer,
                        {'deviceName': self.name, 'deviceType': self.type, 'messageType': Action.data_transfer,
                         'profile': self._profile}, kwargs))
        return call_result.DataTransfer(status=DataTransferStatusEnumType.accepted)

    @on(Action.status_notification)
    def on_status_notification(self, **kwargs):
        data = {**kwargs, 'ts': self._to_epoch_ms(kwargs.get('timestamp'))}

        self._callback((self._uplink_converter,
                        {'deviceName': self.name, 'deviceType': self.type, 'messageType': Action.status_notification,
                         'profile': self._profile}, data))
        return call_result.StatusNotification()

    @on(Action.transaction_event)
    def on_transaction_event(self, event_type, seq_no, timestamp, transaction_info, trigger_reason, **kwargs):
        data = {'event_type': event_type, 'seq_no': seq_no, 'timestamp': timestamp,
                'transaction_info': transaction_info, 'trigger_reason': trigger_reason, **kwargs}
        data['ts'] = self._to_epoch_ms(timestamp)
        self._extract_measurands(data, kwargs.get('meter_value'))

        self._callback((self._uplink_converter,
                        {'deviceName': self.name, 'deviceType': self.type, 'messageType': Action.transaction_event,
                         'profile': self._profile}, data))

        return call_result.TransactionEvent()

    @on(Action.meter_values)
    def on_meter_values(self, evse_id, meter_value, **kwargs):
        data = {'evse_id': evse_id, 'meter_value': meter_value, **kwargs}
        if meter_value:
            data['timestamp'] = meter_value[0].get('timestamp')
            data['ts'] = self._to_epoch_ms(data['timestamp'])
        self._extract_measurands(data, meter_value)

        self._callback((self._uplink_converter,
                        {'deviceName': self.name, 'deviceType': self.type, 'messageType': Action.meter_values,
                         'profile': self._profile}, data))

        return call_result.MeterValues()
