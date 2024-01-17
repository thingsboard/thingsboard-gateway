from pymodbus.server import StartTcpServer, ServerStop
from pymodbus.datastore import ModbusSequentialDataBlock
from pymodbus.datastore import ModbusSlaveContext, ModbusServerContext
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadBuilder
from time import sleep
import signal
import logging

FORMAT = ('%(asctime)-15s %(threadName)-15s'
          ' %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s')
logging.basicConfig(format=FORMAT)
log = logging.getLogger("TEST")
log.setLevel(logging.DEBUG)


def handler(signum, frame):
    ServerStop()
    sleep(1)


def run_server():
    builder = BinaryPayloadBuilder(byteorder=Endian.Little,
                                   wordorder=Endian.Little)
    builder.add_string('abcd')
    builder.add_bits([0, 1, 0, 1, 1, 0, 1, 0])
    builder.add_8bit_int(-0x12)
    builder.add_8bit_uint(0x12)
    builder.add_16bit_int(-0x5678)
    builder.add_16bit_uint(0x1234)
    builder.add_32bit_int(-0x1234)
    builder.add_32bit_uint(0x12345678)
    builder.add_16bit_float(12.34)
    builder.add_16bit_float(-12.34)
    builder.add_32bit_float(22.34)
    builder.add_32bit_float(-22.34)
    builder.add_64bit_int(-0xDEADBEEF)
    builder.add_64bit_uint(0x12345678DEADBEEF)
    builder.add_64bit_uint(0xDEADBEEFDEADBEED)
    builder.add_64bit_float(123.45)
    builder.add_64bit_float(-123.45)
    block = ModbusSequentialDataBlock(1, builder.to_registers())
    store = ModbusSlaveContext(di=block, co=block, hr=block, ir=block)
    builder1 = BinaryPayloadBuilder(byteorder=Endian.Little,
                                    wordorder=Endian.Little)
    builder1.add_string('ghdf')
    builder1.add_bits([1, 1, 1, 1, 1, 0, 1, 0])
    builder1.add_8bit_int(-0x15)
    builder1.add_8bit_uint(0x16)
    builder1.add_16bit_int(-0x5778)
    builder1.add_16bit_uint(0x1434)
    builder1.add_32bit_int(-0x1434)
    builder1.add_32bit_uint(0x13345678)
    builder1.add_16bit_float(18.34)
    builder1.add_16bit_float(-18.34)
    builder1.add_32bit_float(25.34)
    builder1.add_32bit_float(-25.34)
    builder1.add_64bit_int(-0xDFADBEEF)
    builder1.add_64bit_uint(0x12345678DEADBEEF)
    builder1.add_64bit_uint(0xDEADBEEFDEADBEED)
    builder1.add_64bit_float(1233.25)
    builder1.add_64bit_float(-1233.25)
    block1 = ModbusSequentialDataBlock(1, builder1.to_registers())
    store1 = ModbusSlaveContext(di=block1, co=block1, hr=block1, ir=block1)
    slaves = {
        0x01: store,
        0x05: store1
    }
    context = ModbusServerContext(slaves=slaves, single=False)
    StartTcpServer(context=context, address=("127.0.0.1", 5021))


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handler)
    run_server()
