from connectors.opcua.opcua_converter import OpcUaConverter, log
from tb_utility.tb_utility import TBUtility


class OpcUaUplinkConverter(OpcUaConverter):
    def __init__(self, config):
        self.__config = config

    def convert(self, path, data):
        device_name_pattern = self.__config["deviceNamePattern"]
        full_device_name = device_name_pattern.replace("${" + TBUtility.get_value(device_name_pattern, get_tag=True) + "}",
                                                       self.__config["deviceName"])
        result = {"deviceName": full_device_name,
                  "deviceType": self.__config.get("deviceType", "OPC-UA Device"),
                  "attributes": [],
                  "telemetry": [], }
        current_variable = path.split('.')[-1]
        try:
            for attr in self.__config["attributes"]:
                    if TBUtility.get_value(attr["value"], get_tag=True) == current_variable:
                        result["attributes"].append({attr["key"]: attr["value"].replace("${"+current_variable+"}", str(data))})
            for ts in self.__config["timeseries"]:
                    if TBUtility.get_value(ts["value"], get_tag=True) == current_variable:
                        result["telemetry"].append({ts["key"]: ts["value"].replace("${"+current_variable+"}", str(data))})
            return result
        except Exception as e:
            log.exception(e)
