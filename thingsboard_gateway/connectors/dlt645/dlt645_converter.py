import struct
import crc16

DLT645_FRAME_FORMAT = "BBBBBBBB" 

def checksum(data):
    """计算DLT645校验和"""
    crc = crc16.crc16xmodem(data)
    return struct.pack(">H", crc)

class Dlt645Frame:
    
    def __init__(self, ctrl_code, data_len, data):
        self.start_code = 0x68
        self.ctrl_code = ctrl_code
        self.data_len = data_len
        self.data = data
    
    def pack(self):
        """封装DLT645数据帧"""
        frame_data = struct.pack(
            DLT645_FRAME_FORMAT,
            self.start_code,
            self.ctrl_code,
            self.data_len,
            *self.data,
            self.data_len,
            self.start_code  
        )
        frame_data += checksum(frame_data)
        frame_data += b'\x16'
        return frame_data
    
    @staticmethod
    def unpack(frame_data):
        """解析DLT645数据帧"""
        fields = struct.unpack(DLT645_FRAME_FORMAT, frame_data[:8])
        start_code1 = fields[0]
        ctrl_code = fields[1]
        data_len1 = fields[2]
        data = fields[3:-2]
        data_len2 = fields[-2]
        start_code2 = fields[-1]
        
        if start_code1 != 0x68 or start_code2 != 0x68:
            raise ValueError("Invalid start code")
        if data_len1 != data_len2:
            raise ValueError("Invalid data length")
        
        checksum_recv = frame_data[-3:-1]
        checksum_calc = checksum(frame_data[:-3])
        if checksum_recv != checksum_calc:
            raise ValueError("Invalid checksum")
        
        return Dlt645Frame(ctrl_code, data_len1, data)

def encode_addr(bcd_addr):
    """将BCD编码的表地址转为整型地址"""
    addr_str = ""
    for bcd in bcd_addr:
        addr_str += f"{bcd:02X}"
    return int(addr_str)
    
def decode_addr(int_addr):
    """将整型地址转为BCD编码的表地址"""
    addr_str = f"{int_addr:012d}"
    bcd_addr = [int(addr_str[i:i+2]) for i in range(0, len(addr_str), 2)]
    return bytes(bcd_addr)

def decode_value(data):
    """解析数据项的值"""
    value = struct.unpack(">f", data)[0]  
    return value

def encode_time(dt):
    """将datetime对象转为DLT645的时间格式"""
    bcd = ((dt.year % 100) << 24 |
           dt.month << 20 |
           dt.day << 16 |
           dt.hour << 12 |
           dt.minute << 8 |
           dt.second << 4 |
           dt.weekday())
    return struct.pack(">I", bcd)

def decode_time(data):
    """将DLT645的时间格式转为datetime对象"""
    bcd = struct.unpack(">I", data)[0]
    year = ((bcd >> 24) & 0x3F) + 2000
    month = (bcd >> 20) & 0xF
    day = (bcd >> 16) & 0x1F
    hour = (bcd >> 12) & 0xF
    minute = (bcd >> 8) & 0x3F     
    second = (bcd >> 4) & 0xF
    return datetime.datetime(year, month, day, hour, minute, second)