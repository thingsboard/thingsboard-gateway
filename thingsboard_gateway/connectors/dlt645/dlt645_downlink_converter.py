from dlt645_converter import Dlt645Frame, encode_addr, encode_time
import datetime

def convert_rpc(device_config, rpc_request):
    """转换Thingsboard RPC请求为DLT645下行帧""" 
    
    method = rpc_request["method"]
    params = rpc_request["params"]

    slave_addr = device_config["slave_id"]
    addr_bcd = encode_addr(slave_addr)
    
    if method == "set_addr":
        new_addr = params["addr"] 
        data = addr_bcd + b'\x04\x06' + encode_addr(new_addr)
        return Dlt645Frame(0x15, 0x0A, data)
    elif method == "set_time":
        dt = datetime.datetime.utcfromtimestamp(params["ts"] / 1000)
        data = addr_bcd + b'\x04\x00\x02\x0F' + encode_time(dt)
        return Dlt645Frame(0x15, 0x0E, data)
    elif method == "get_data":
        data_addr = params["addr"]
        data = addr_bcd + b'\x11\x04' + encode_addr(data_addr)
        return Dlt645Frame(0x11, len(data), data)
    else:
        raise ValueError(f"Unsupported RPC method: {method}")