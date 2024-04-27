from dlt645_converter import decode_addr, decode_value, decode_time

def convert_frame(device_config, frame):
    """转换DLT645上行数据帧为遥测数据"""
    if frame.ctrl_code != 0x91: 
        return None

    data_items = {}
    addr = decode_addr(frame.data[1:7])
    
    if addr == 0x00010000: # 电表地址
        pass
    elif addr == 0x02010100: # 电表型号
        data_items["model"] = frame.data[8:-2].decode("ascii")
    elif addr == 0x0201FF00: # 软件版本号  
        ver = f"{frame.data[8]}.{frame.data[9]}"
        data_items["version"] = ver
    elif addr == 0x04000101: # 正向有功总电能  
        value = decode_value(frame.data[8:12])
        data_items["total_active_power"] = value
    elif addr == 0x04000102: # 反向有功总电能
        value = decode_value(frame.data[8:12]) 
        data_items["total_reverse_active_power"] = value
    elif addr == 0x0400020F: # 日期时间
        dt = decode_time(frame.data[8:12])
        data_items["timestamp"] = int(dt.timestamp() * 1000)
    
    ts = data_items.pop("timestamp", None)
    for k, v in data_items.items():
        yield {
            "device": device_config["info_values"][0],
            "ts": ts,
            "values": {k: v}
        }