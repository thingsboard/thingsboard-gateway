from enum import Enum, auto


class DatabaseActionType(Enum):
    WRITE_DATA_STORAGE = auto()  # Writes do not require a response on the request
    WRITE_STORAGE_INDEX = auto()
    READ_CONNECTED_DEVICES = auto()  # Reads need response to get requested data
    READ_DEVICE = auto()  # RPC CALL
    DELETE_OLD_DATA = auto()
