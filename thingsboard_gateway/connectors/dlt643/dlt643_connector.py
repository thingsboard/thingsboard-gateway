"""
DL/T 643连接器
本模块提供了一个用于DL/T 643-2007协议的Thingsboard IoT Gateway连接器。
它通过RS485串口与DL/T 643设备通信,读取电能和功率数据,并将数据上传到Thingsboard平台。
它还处理来自平台的RPC命令。
"""

import serial
import struct
import json
import time
from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

class DLT643Connector(Connector):
    def __init__(self, gateway, config):
        """
        初始化连接器
        :param gateway: 网关对象
        :param config: 连接器配置
        """
        super().__init__()
        self.devices = []
        self.__config = config.get("connector", {})  # 获取连接器特定配置
        self.__gateway = gateway
        self.load_dlt643_devices()
        
    def open(self):
        """
        打开RS485串口
        """
        self.__stopped = False
        self.__connected = False
        self.port = serial.Serial(
            port=self.__config["port"],  # 串口名
            baudrate=self.__config["baudrate"],  # 波特率
            bytesize=self.__config["bytesize"],  # 数据位
            parity=self.__config["parity"],  # 校验位
            stopbits=self.__config["stopbits"],  # 停止位
            timeout=self.__config["timeout"]  # 超时时间
        )
        self.__connected = True
        self.daemon = True
        self.start()
        
    def close(self):
        """
        关闭RS485串口
        """
        self.__stopped = True
        if self.__connected:
            self.port.close()
            
    def get_name(self):
        """
        获取连接器名称
        """
        return self.name
    
    def is_connected(self):
        """
        检查连接状态
        """
        return self.__connected
    
    def load_dlt643_devices(self):
        """
        加载DL/T 643设备地址
        """
        self.devices.append(self.__config["address"])
        log.debug("Found devices: %s", self.devices)
        
    def run(self):
        """
        连接器主循环,定期从设备读取数据并上传
        """
        try:
            while not self.__stopped:
                for addr in self.devices:
                    # 查询电能数据
                    energy_data = self.send_dlt643_cmd(addr, "R", 129, 0)
                    
                    # 查询功率数据
                    power_data = self.send_dlt643_cmd(addr, "R", 150, 0)
                    
                    # 合并数据并上传
                    device_data = {**energy_data, **power_data}
                    self.__gateway.send_to_storage(device_data)
                    
                    # 按轮询周期延时
                    time.sleep(self.__config["poll_period"])
        except Exception as e:
            log.exception(e)
            
    def on_rpc_request(self, content):
        """
        处理RPC请求
        :param content: RPC请求内容
        """
        try:
            if content.method == "remote_disconnect":
                log.debug("Processing RPC command: remote_disconnect")
                self.send_dlt643_cmd(content.device, "W", 129, 1)
            elif content.method == "remote_reconnect":
                log.debug("Processing RPC command: remote_reconnect")
                self.send_dlt643_cmd(content.device, "W", 130, 1)
            elif content.method == "set_price":
                log.debug("Processing RPC command: set_price")
                prices = content.params["prices"]
                for i, price in enumerate(prices):
                    self.send_dlt643_cmd(content.device, "W", 145+i, float(price))
        except Exception as e:
            log.exception(e)
            
    def send_dlt643_cmd(self, addr, rw, datacode, data):
        """
        发送DL/T 643命令帧
        :param addr: 设备地址
        :param rw: "R"表示读,"W"表示写
        :param datacode: DL/T 643数据项编码
        :param data: 写入数据,读取时忽略
        """
        try:
            # 构建DL/T 643帧
            ctrl = 16 if rw == "W" else 1
            frame = self.build_dlt643_frame(addr, ctrl, datacode, data)
            log.debug("TX: %s", TBUtility.bytes_to_hex(frame))
            
            # 发送帧并等待响应
            self.port.write(frame)
            rx_frame = self.port.read(self.__config["timeout"])
            log.debug("RX: %s", TBUtility.bytes_to_hex(rx_frame))
            
            # 解析响应数据
            if rw == "R":
                data = self.parse_dlt643_data(rx_frame)
                return {str(datacode): data}
            else:
                return True
        except Exception as e:
            log.exception(e)
            return None
        
    @staticmethod    
    def build_dlt643_frame(addr, ctrl, datacode, data):
        """
        构建DL/T 643帧
        :param addr: 设备地址
        :param ctrl: 控制码
        :param datacode: 数据项编码
        :param data: 数据
        """
        frame = bytearray()
        frame.append(0x68)  # 起始符
        
        # 地址域
        frame.extend(struct.pack(">H", addr))
        
        # 控制码
        frame.append(ctrl)
        
        # 数据项编码
        frame.append(datacode)
        
        # 数据域
        if isinstance(data, int):
            frame.extend(struct.pack(">I", data))
        elif isinstance(data, float):
            frame.extend(struct.pack(">f", data))
        else:
            frame.extend(bytes(data))
            
        # 计算校验和
        check_sum = sum(frame[1:]) % 256
        frame.append(check_sum)
        
        # 结束符
        frame.append(0x16)

        return frame
    
    @staticmethod
    def parse_dlt643_data(frame):
        """
        解析DL/T 643响应帧数据
        :param frame: 响应帧
        """
        data_type = frame[8]
        data_bytes = frame[9:-2]
        
        if data_type == 0x09:   # 整型
            return struct.unpack(">i", data_bytes)[0]
        elif data_type == 0x0A: # 浮点型
            return struct.unpack(">f", data_bytes)[0]
        elif data_type == 0x0C: # 布尔型
            return data_bytes[0] != 0
        else: 
            return data_bytes