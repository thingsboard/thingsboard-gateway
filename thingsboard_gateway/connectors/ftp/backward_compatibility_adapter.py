from copy import deepcopy


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
        if not security.get('type') == 'basic':
            security.pop("username", None)
            security.pop("password", None)
        self._converted_config['parameters']['security'] = security
        old_paths = self._config.get('paths', [])
        new_paths = self._convert_path_entries(old_paths)
        self._converted_config["paths"] = new_paths
        self._converted_config["requestsMapping"]["attributeUpdates"] = self._config.get("attributeUpdates", [])
        self._converted_config["requestsMapping"]["serverSideRpc"] = self._config.get("serverSideRpc", [])

        return self._converted_config

    @staticmethod
    def _convert_path_entries(old_paths: list) -> list:
        new_paths = []
        for old_path_configuration in old_paths:
            new_path_configuration = FTPBackwardCompatibilityAdapter._convert_path(old_path_configuration)
            new_paths.append(new_path_configuration)
        return new_paths

    @staticmethod
    def _convert_path(old_path_config: dict) -> dict:
        converted_configuration = {}
        for key, value in old_path_config.items():
            converted_configuration[key] = old_path_config[key]

        if "attributes" in old_path_config:
            converted_configuration["attributes"] = []
            for attribute in old_path_config["attributes"]:
                new_attribute = {}
                attribute_type = attribute.get("type", "string")
                if attribute_type == "int":
                    attribute_type = "integer"
                elif attribute_type == "string":
                    attribute_type = "string"

                new_attribute["type"] = attribute_type

                for key, value in attribute.items():
                    if key != "type":
                        new_attribute[key] = value

                converted_configuration["attributes"].append(new_attribute)

            if "timeseries" in old_path_config:
                converted_configuration["timeseries"] = []
                for ts in old_path_config["timeseries"]:
                    new_ts = ts
                    if new_ts.get("type") == "int":
                        new_ts["type"] = "integer"
                    elif new_ts.get("type") == "str":
                        new_ts["type"] = "string"

                    converted_configuration["timeseries"].append(new_ts)

        return converted_configuration

    @staticmethod
    def is_old_config_format(config: dict) -> bool:
        return True if not config.get("parameters") else False
