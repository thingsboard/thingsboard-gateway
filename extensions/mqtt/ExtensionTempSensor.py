from time import time

from . import ExtensionInterface


class Extension(ExtensionInterface.ExtensionInterface):
    def convert_message_to_json_for_storage(self, topic, payload):
        ts = int(round(time() * 1000))
        try:
            result = [
                {
                    "device_name": topic.split("/")[-1],
                    # todo remove after implementing device_type!!!!!
                    # "device_type": payload["type"],
                    "attributes": {
                        "model": payload["model"],
                        "serial_number": payload["serial"],
                        # todo what is integration?
                    },
                    "telemetry": [
                        {
                            "ts": ts,
                            "values": {
                                "temperature": payload["temperature1"],
                                "humidity": payload["humidity1"]
                            }
                        },
                        {
                            "ts": ts,
                            "values": {
                                "humidity": payload["humidity2"]
                            }
                        }
                    ]
                }
            ]
            return result
        except:
            pass

