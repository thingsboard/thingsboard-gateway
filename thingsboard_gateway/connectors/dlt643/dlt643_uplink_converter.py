"""
DL/T 643上行转换器
本模块提供了将DL/T 643原始数据转换为Thingsboard平台格式的功能。
"""

import time
from thingsboard_gateway.connectors.converter import Converter, log    
import dlt643_converter

class DLT643UplinkConverter(Converter):
    def __init__(self, config):
        """
        初始化转换器
        :param config: 转换器配置
        """
        self.__config = config.get("uplink_converter", {})
        self.rulesets = []
        self.__load_converters()
        
    def __load_converters(self):
        """
        加载转换规则
        """
        self.rulesets = self.__config.get("rulesets", [])
        log.info("Loaded %d converter rulesets", len(self.rulesets))

    def convert(self, config, data):
        """
        转换DL/T 643数据为Thingsboard格式
        :param config: 连接器配置
        :param data: 原始数据
        """
        if isinstance(data, bytes):
            data = data.decode("UTF-8")
            
        if data is None:
            log.error("Empty data received")
            return None
        
        telemetry = []
        attributes = []
        try:
            for ruleset in self.rulesets:
                name = ruleset["name"]
                dlt643_data = data.get(ruleset["datatype"])
                if dlt643_data is None:
                    continue
                
                # 调用对应的转换函数
                converter = getattr(dlt643_converter, name)
                converted_data = converter(dlt643_data, ruleset)
                
                if "target_attr" in ruleset:
                    attributes.append({ruleset["target_attr"]: converted_data})
                else:
                    telemetry.append(converted_data)
                        
            return {"telemetry": telemetry, "attributes": attributes}
        except Exception as e:
            log.exception(e)
            return {"telemetry": telemetry, "attributes": attributes}