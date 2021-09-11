import unittest
from simplejson import loads
import os

from sys import path

path.insert(0, "../../")

from thingsboard_gateway.new_storage.database_connector import DatabaseConnector
from thingsboard_gateway.new_storage.storage_settings import StorageSettings

class TestDatabaseConnector(unittest.TestCase):

    def setUp(self):
        self.config = {
            "data_file_path": "./testing.db",
            "max_read_records_count": 100
        }
        self.settings = StorageSettings(self.config)


    def test_database_creation(self):
        db = DatabaseConnector(self.settings)
        db.connect()
        
        is_created = os.access(self.config["data_file_path"], os.F_OK)

        self.assertTrue(is_created)


if __name__ == "__main__":
    unittest.main()