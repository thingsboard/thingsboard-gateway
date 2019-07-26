# Generic interface to allow getting data from different MQTT devices
class ExtensionInterface:
    def convert_message_to_json_for_storage(self, topic, payload):
        pass

    def convert_message_to_atr_request(self, topic, payload):
        pass
