from abc import ABC, abstractmethod


class MqttUplinkConverter(ABC):

    # During startup, MQTT connector subscribes to a list of topic filters.
    # When the new MQTT message arrives, it is passed to the converter based on the topic filter.
    # Output of the converter should match the following format:
    # {
    #     "deviceName": "Device A",
    #     "deviceType": "thermostat",
    #     "attributes": {
    #         "model": "Model A",
    #         "serialNumber": "SN-111",
    #         "integrationName": "Test integration"
    #     },
    #     "telemetry": {
    #         "temperature": 42,
    #         "humidity": 80
    #     }
    # }
    @abstractmethod
    def convert(self, body):
        pass
