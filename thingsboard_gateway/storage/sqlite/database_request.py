
from enum import Enum, auto
from thingsboard_gateway.storage.sqlite.database_action_type import DatabaseActionType


class DatabaseRequest:

    # Wrap data and write intention to better controll
    # Writes. They need to be atomic so we don't corrupt DB
    def __init__(self, _type: DatabaseActionType, data):
        self.type = _type
        self.data = data

    def get_type(self):
        return self.type

    def get_data(self):
        return self.data
    