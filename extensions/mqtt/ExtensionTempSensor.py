from . import ExtensionInterface


class Extension(ExtensionInterface.ExtensionInterface):
    def convert_message_to_json_for_storage(self, topic, payload):
        return ["to_storage", [{"values": payload}, "ATTRIBUTES", "Temp Sensor"]]
    # todo add ts example, use "tms" type_of_data
