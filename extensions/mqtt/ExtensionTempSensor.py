from . import ExtensionInterface

class Extension:
    def convert_message_to_json_for_storage(self, topic, payload):
        return ["to_storage", [{"values": payload}, "ATTRIBUTES", "Temp Sensor"]]
    # todo add ts example
