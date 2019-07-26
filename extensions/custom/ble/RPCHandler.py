# we expect to get only one parameter in float format
def rpc_handler(method_name, param):
    return 0x2c, (0).to_bytes(1, byteorder='big'), True
