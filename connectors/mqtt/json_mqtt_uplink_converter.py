from abc import ABC,abstractmethod

from connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter


class JsonMqttUplinkConverter(MqttUplinkConverter):

# Example

# Input:
# Topic: "/temperature-sensors/data"
# Body:
# {
#     "serialNumber": "SensorA",
#     "sensorType": "temperature-sensor"
#     "model": "T1000"
#     "t": 42
# }

# Config:
# {
#     "topicFilter": "/temperature-sensors/+",
#     "converter": {
#         "type": "json",
#         "filterExpression": "",
#         "deviceNameJsonExpression": "${$.serialNumber}",
#         "deviceTypeJsonExpression": "${$.sensorType}",
#         "timeout": 60000,
#         "attributes": [
#             {
#                 "type": "string",
#                 "key": "model",
#                 "value": "${$.model}"
#             }
#         ],
#         "timeseries": [
#             {
#                 "type": "double",
#                 "key": "temperature",
#                 "value": "${$.t}"
#             }
#         ]
#     }
# }

# Result:
# {
#     "deviceName": "SensorA",
#     "deviceType": "temperature-sensor",
#     "attributes": {
#         "model": "T1000",
#     },
#     "telemetry": {
#         "temperature": 42,
#     }
# }

    def convert(self, topic, body):
        pass
