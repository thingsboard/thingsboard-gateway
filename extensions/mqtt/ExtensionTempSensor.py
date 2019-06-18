from . import ExtensionInterface

class Extension:
    def convert_message_to_json_for_storage(self, topic, payload):
        return ["to_storage", [payload, "TELEMETRY", "Temp Sensor"]]