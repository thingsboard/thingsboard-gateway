from copy import deepcopy
import json


class FTPBackwardCompatibilityAdapter:

    def __init__(self, config: dict) -> None:
        self._config = deepcopy(config)
        self._converted_config = {
            "parameters": {
            },
            "paths": [],
            "requestsMapping": {
                "attributeUpdates": [],
                "serverSideRpc": []
            }
        }

    def convert(self) -> dict:

        self._converted_config['parameters']['host'] = self._config.get('host')
        self._converted_config['parameters']['port'] = self._config.get('port')
        self._converted_config["parameters"]["TLSSupport"] = self._config.get("TLSSupport", False)
        security = self._config.get('security', {})
        self._converted_config['parameters']['security'] = security
        self._converted_config["parameters"]["security"]["type"] = security.get("type")
        self._converted_config["parameters"]["security"]["username"] = security.get("username")
        self._converted_config["parameters"]["security"]["password"] = security.get("password")
        old_paths = self._config.get('paths', [])
        new_paths = self._convert_path_entries(old_paths)
        self._converted_config["paths"] = new_paths
        self._converted_config["requestsMapping"]["attributeUpdates"] = self._config.get("attributeUpdates", [])
        self._converted_config["requestsMapping"]["serverSideRpc"] = self._config.get("serverSideRpc", [])

        return self._converted_config

    @staticmethod
    def _convert_path_entries(old_paths: list) -> list:
        new_paths = []
        for old_path in old_paths:
            new_path_data = FTPBackwardCompatibilityAdapter._convert_path(old_path)
            new_paths.append(new_path_data)
        return new_paths

    @staticmethod
    def _convert_path(old_path_data: dict) -> dict:
        new_path_data = old_path_data

        if "attributes" in new_path_data:

            for attribute in new_path_data["attributes"]:
                if "type" not in attribute:
                    attribute_dict = deepcopy(attribute)
                    attribute.clear()
                    attribute["type"] = "string"
                    for key, value in attribute_dict.items():
                        attribute[key] = value
                else:
                    if attribute["type"] == "int":
                        attribute["type"] = "integer"
                    elif attribute["type"] == "str":
                        attribute["type"] = "string"

        if "timeseries" in new_path_data:
            for ts in new_path_data["timeseries"]:
                if "type" not in ts:
                    ts["type"] = "string"
                else:
                    if ts["type"] == "int":
                        ts["type"] = "integer"
                    elif ts["type"] == "str":
                        ts["type"] = "string"
        return new_path_data

    @staticmethod
    def is_old_config_format(config: dict) -> bool:
        return True if config.get("parameters", {}) else False


if __name__ == "__main__":
    with open("old_format.json") as f:
        data = json.load(f)
        converter = FTPBackwardCompatibilityAdapter(data)
        print(converter.is_old_config_format(data))
        data = converter.convert()

    with open("some.json", "w") as file:
        file.write(json.dumps(data, indent=4))





