import unittest
from time import time
from sys import path
import os
from simplejson import loads
path.insert(0, '../../')
from thingsboard_gateway.new_storage.database import Database

class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.config = {
            "data_file_path": "./testing.db",
            "max_read_records_count": 100,
            "max_days_to_store_data": 1
        }
        self.db = Database(self.config)


    def test_create_device_table(self):

        device_name = "DraginoLHT"

        self.db.create_device_table(device_name)
        
        self.db.cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        device_table = "device_" + device_name
        self.assertIn(device_table, self.db.cur.fetchall()[0])


    def test_write_to_device_table(self):

        data = {
            "deviceName": "DraginoLHT",
            "ts": 16888888874,
            "telemetry": [
                {"temp": 24.30},
               { "humi": 90}
            ]
        }

        self.db.create_device_table(data["deviceName"])

        self.db.write(data)

        ret = self.db.readAll(data["deviceName"])

        self.assertDictEqual(data, loads(ret[0][1]))

    def test_create_connected_devices_table(self):
        
        self.db.create_connected_devices_table()

        tables = self.db.get_all_tables()

        self.assertIn('connected_devices', tables)

    def test_get_connected_devices(self):

        # create table for this test
        self.db.create_connected_devices_table()
        returned_devices = []
        devices = [
            {
                "deviceName": "device1",
                "deviceType": "humiSensor",
                "connector": "MQTT"
            },
            {
                "deviceName": "device2",
                "connector": "Modbus"
            },
            {
                "deviceName": "device3",
                "deviceType": "tempSensor",
                "connector": "REST"
            }
        ]
        for device in devices:
            self.db.add_new_connecting_device(**device)
            
        for i, d in enumerate(devices):
            if i > 0 and i < 2:
                d["deviceType"] = 'default'

            returned_devices.append(tuple(d.values()))
            print(returned_devices)

        connected_devices = self.db.get_connected_devices()
            

        self.assertCountEqual(returned_devices, connected_devices)

    def test_read_from_to_timestamp(self):
        self.db.create_device_table("testingDevice")

        initTimestamp = 1627164000000
        messageCount = 0
        
        for _ in range(6):
            # create messages in the span of a week where each message
            # was taken each day
            message = {
                "deviceName": "testingDevice",
                "ts": initTimestamp,
                "data": "123"
                }
            self.db.write(message)
            initTimestamp += 86400000
            messageCount += 1
        # Check data is for the whole week
        wholeWeek = self.db.readAll("testingDevice")
        self.assertEqual(messageCount, len(wholeWeek))

        # Check only data 2 days since initTimestamp
        twoDays = self.db.read(1627164000000, 1627336799999, "testingDevice")
        print("\ntwoDays: %s" % twoDays)
        self.assertEqual(2 , len(twoDays))
            


    def tearDown(self):
        # COMMENT IF YOU WANT DB TO EXIST AFTER TESTS
        os.remove(self.db.settings.get_data_file_path())
        # pass
        

if __name__ == "__main__":
    unittest.main()