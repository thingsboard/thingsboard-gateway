from thingsboard_gateway.storage.sqlite.database import Database
from queue import Queue
from thingsboard_gateway.storage.sqlite.database_request import DatabaseRequest
from thingsboard_gateway.storage.sqlite.database_action_type import DatabaseActionType
#
#   No need to import DatabaseResponse, responses come to this component to be deconstructed
#
from logging import getLogger

log = getLogger("storage")


class StorageHandler:
    """
    HIGH level api for thingsboard_gateway main loop
    """

    def __init__(self, config):
        log.info("Sqlite Storage initializing...")
        # TODO:
        # Make a init function that checks if database file exists
        # if it exists load connected devices from it
        self.db = Database(config)

        # We need queues to stay atomic when multiple connectors/Threads are
        # trying to write or read from database
        log.info("Initializing read and process queues")
        self.processQueue = Queue(-1)
        self.readQueue = Queue(-1)

        self.db.setReadQueue(self.readQueue)
        self.db.setProcessQueue(self.processQueue)

        # Create table if not exists for connected devices
        self.db.create_connected_devices_table()
        self.connected_devices = self.get_connected_devices()
        log.info("Sqlite storage initialized!")

    def get_connected_devices(self):
        """
        Util func, to only parse and store connected devices names in a list
        """
        _type = DatabaseActionType.READ_CONNECTED_DEVICES
        data = self
        req = DatabaseRequest(_type, data)
        self.processQueue.put(req)

        self.db.process()

        return self.connected_devices

    def readAll(self, deviceName):
        return self.db.readAll(deviceName)

    def readFrom(self, deviceName, ts):
        return self.db.readFrom(deviceName, ts)

    def put(self, message):
        try:

            device_name = message.get("deviceName")

            if device_name is not None and device_name not in self.connected_devices:
                self.db.create_device_table(device_name)

            _type = DatabaseActionType.WRITE_DATA_STORAGE
            request = DatabaseRequest(_type, message)

            log.info("Sending data to storage")
            self.processQueue.put(request)

            # Left for discussion
            log.debug("data %s from device %s " % (str(self.connected_devices[device_name]), device_name))
            self.connected_devices[device_name]["data_saved_index"] += 1

            storageIndex = self.connected_devices[device_name]["data_saved_index"]
            data = (device_name, storageIndex)
            _type = DatabaseActionType.WRITE_STORAGE_INDEX
            log.debug("Index request data: %s" % str(data))
            index_request = DatabaseRequest(_type, data)

            log.debug("Updating device storage index")
            self.processQueue.put(index_request)

            self.db.process()  # This call is necessary
            return True
        except Exception as e:
            log.exception(e)

    def add_device(self, deviceName, connector, deviceType=None):

        self.db.add_new_connecting_device(deviceName, connector, deviceType)
        # Update connected devices list
        self.connected_devices = self.get_connected_devices()

        # Create device table
        self.db.create_device_table(deviceName)

    def del_device(self, device_name):

        self.db.del_connected_device(device_name)

        # Update connected devices list
        self.connected_devices = self.get_connected_devices()

    def closeDB(self):
        self.db.closeDB()
