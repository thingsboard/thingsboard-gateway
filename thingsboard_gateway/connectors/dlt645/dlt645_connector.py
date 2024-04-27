import json
import socket
import threading
import time
from dlt645_converter import Dlt645Frame
from dlt645_uplink_converter import convert_frame
from dlt645_downlink_converter import convert_rpc

class Dlt645Connector:
    
    def __init__(self, config):
        self.config = config
        self.devices = {}
        self.socket = None
        
    def connect(self):
        # 创建与Gateway的Socket连接
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.config["host"], self.config["port"]))
        
        # 发送连接事件
        self.send_message("device_connected", {
            "device_name": self.config["device_name"]
        })
        
        # 启动读取数据的线程
        threading.Thread(target=self.read_data).start()
        
    def disconnect(self):
        if self.socket is not None:
            # 发送断开连接事件
            self.send_message("device_disconnected", {
                "device_name": self.config["device_name"]
            })
            self.socket.close()
            
    def send_message(self, type, data):
        # 发送JSON格式的消息
        message = {
            "type": type,
            "data": data
        }
        self.socket.sendall((json.dumps(message) + "\n").encode())
        
    def read_data(self):
        # 循环读取串口数据
        while True:
            try:
                frame = self.read_frame()
                if frame is not None:
                    self.handle_frame(frame)
            except Exception as e:
                self.handle_error(f"Read data failed: {e}")
            time.sleep(self.config["poll_interval"])
            
    def read_frame(self):
        # 从串口读取DLT645数据帧
        # 代码省略,与之前的实现相同
        pass
    
    def write_frame(self, frame):
        # 向串口写入DLT645数据帧  
        # 代码省略,与之前的实现相同
        pass
        
    def handle_frame(self, frame):
        # 处理DLT645数据帧
        device_name = self.config["device_name"]
        for data in convert_frame(frame):
            self.send_message("telemetry", {
                "device": device_name,
                "ts": data["ts"],
                "values": data["values"]
            })
            
    def handle_attributes(self, data):
        # 处理属性更新
        pass
    
    def handle_rpc_request(self, data):
        # 处理RPC请求
        device_name = data["device"]
        method = data["data"]["method"]
        params = data["data"]["params"]
        request_id = data["data"]["id"]
        
        device = self.devices[device_name]
        frame = convert_rpc(device, method, params)
        self.write_frame(frame)
        
        # 等待响应
        time.sleep(1)
        frame = self.read_frame()
        if frame is not None:
            result = self.handle_rpc_response(frame)
        else:
            result = {"success": False, "error": "Timeout"}
            
        self.send_message("rpc_response", {
            "device": device_name,
            "id": request_id,
            "data": result
        })
        
    def handle_rpc_response(self, frame):
        # 处理RPC响应
        # 根据实际需求解析RPC响应数据帧,返回结果
        pass
        
    def handle_error(self, error):
        # 处理错误
        self.send_message("error", {"message": error})
        
def main():
    config = {
        "host": "localhost",
        "port": 10000,
        "device_name": "DLT645_DEVICE", 
        "poll_interval": 5
    }
    connector = Dlt645Connector(config)
    connector.connect()
    
    # 等待终止信号
    event = threading.Event()
    event.wait()
    
    connector.disconnect()

if __name__ == '__main__':
    main()