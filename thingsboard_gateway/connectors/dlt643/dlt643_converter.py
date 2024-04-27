"""
DL/T 643数据转换函数
本模块定义了DL/T 643原始数据与Thingsboard数据格式之间的转换函数。
"""

def energy(data, config):
    """
    转换电能数据
    """
    return {
        "ts": int(time.time() * 1000),
        "values": {
            config["name"]: round(data * config["scale"], config.get("round", 3)),
        },
        "unit": config.get("unit", "MWh")
    }

def power(data, config):
    """  
    转换功率数据
    """
    return {
        "ts": int(time.time() * 1000),
        "values": {
            config["name"]: round(data * config["scale"], config.get("round", 4)),  
        },
        "unit": config.get("unit", "MW")
    }

def attr_packer(data, config):
    """
    打包多个字段为一个属性
    """
    attribute = {}
    for field in config["source_fields"]:
        attribute[field] = data[field]
    return attribute
        
def remote_disconnect(data, config):
    """
    构建远程断电命令
    """
    return {
        "function": int(config["dlt643_funcode"]),
        "datacode": int(config["dlt643_datacode"]),
        "data": 1
    }
    
def remote_reconnect(data, config):
    """
    构建远程复电命令
    """ 
    return {
        "function": int(config["dlt643_funcode"]),
        "datacode": int(config["dlt643_datacode"]),
        "data": 1
    }

def set_price(data, config):
    """
    构建设置电价命令
    """
    prices = data["prices"]
    if len(prices) != config["num_price"]:
        raise ValueError(f"Expected {config['num_price']} prices, got {len(prices)}")
    
    rpc_cmds = []
    for i, price in enumerate(prices):
        rpc_cmds.append({
            "function": int(config["dlt643_funcode"]),
            "datacode": int(config["dlt643_datacode"]) + i,
            "data": float(price)
        })
        
    return rpc_cmds