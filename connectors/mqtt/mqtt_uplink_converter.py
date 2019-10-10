from abc import ABC, abstractmethod


class MqttUplinkConverter(ABC):

    @abstractmethod
    def convert(self, topic, body):
        pass
