from thingsboard_gateway.new_storage.database_connector import DatabaseConnector
from thingsboard_gateway.new_storage.database_request import DatabaseRequest
from thingsboard_gateway.new_storage.database_response import DatabaseReponse
from thingsboard_gateway.new_storage.database_action_type import DatabaseActionType
from thingsboard_gateway.new_storage.storage_settings import StorageSettings
from simplejson import dumps
from time import time
from datetime import datetime, timedelta
from hashlib import sha1
from sqlite3 import OperationalError
from logging import getLogger

log = getLogger("database")

class Database:
    """
        What this component does:
        - abstracts creating tables for devices.
        - writes to database
        - reads from database
        - delete data older than specified in config
        ------------- ALL OF THIS IN AN ATOMIC WAY ---------
    """
    def __init__(self, config):
        self.settings = StorageSettings(config)
        # Pass settiongs to connector
        self.db = DatabaseConnector(self.settings)

        self.db.connect()

        self.cur = self.db.get_cursor()

        # process Queue
        self.processQueue = None
        # Response Queue
        self.readQueue = None

        # NOTE: Rename to self.processing
        self.__writing = False
            
    def add_new_connecting_device(self, deviceName=None, connector=None, deviceType=None):

        try:
            if connector is None:
                log.error("Connector was not specified")
                log.exception()
                return

            if isinstance(connector, dict):
                connector_name = connector["connector"]
            elif isinstance(connector, str):
                connector_name = connector

            dataSavedIndex = 1
            dataUploadedIndex = 0
            
            log.debug("Inserting new connecting device to DB")
            self.cur.execute('''INSERT INTO connected_devices(deviceName,deviceType,connector,dataSavedIndex,dataUploadedIndex) VALUES(?,?,?,?,?);''',[deviceName,deviceType,connector_name,dataSavedIndex,dataUploadedIndex])

            self.db.commit()
            
        except Exception as e:
            self.db.rollback()
            log.exception(e)

    def del_connected_device(self, deviceName):
        try:
            self.cur.execute('''
                DELETE FROM connected_devices WHERE deviceName = ? ;''', [deviceName])
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            log.exception(e)

    """
    TODO: update_connected_device 
            - update device in database
            don't know why but its in legacy API
    """
    def update_device_data_index(self,deviceName, dataIndex):
        try:
            log.debug("Updating device %s storage data index to: %d" % (deviceName,dataIndex))
            self.cur.execute('''
                UPDATE connected_devices SET dataSavedIndex = ?  WHERE deviceName = ?''', [dataIndex, deviceName])
            
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            log.exception(e)

    def create_connected_devices_table(self):
        try:
            self.cur.execute('''
                CREATE TABLE IF NOT EXISTS connected_devices (deviceName TEXT, deviceType TEXT, connector TEXT, dataSavedIndex INTEGER, dataUploadedIndex INTEGER);''')

            # dataIndex is a rowid of the actual device table that was read to cluster

            self.db.commit()

        except Exception as e:
            self.db.rollback()
            log.exception(e)

    def create_device_table(self, deviceName):
        """
        Params:
            deviceName: str
        Desc:
            Creates a table for each connected debive.
            Where connectors can store converted messages for backup

            Device name is Hashed so that SQlite doesn't have a problem with
            special simbols like: "-" "." "_" ...
        """

        try:

            h = sha1()
            h.update(bytes(deviceName, 'utf-8'))
            
            device_table = h.hexdigest()[:10].upper()
            device_table = "_" + device_table

            self.cur.execute('''
                CREATE TABLE IF NOT EXISTS ''' + device_table + ''' (dataIndex INTEGER PRIMARY KEY, timestamp INTEGER, message TEXT); ''')

            self.db.commit()

        except Exception as e:
            self.db.rollback()
            log.exception(e)
    
    # DEPRACTED
    def get_all_tables(self):
        """
        Return list of all tables
        """
        try:
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table';")

            return self.cur.fetchall()[0]

        except Exception as e:
            self.db.rollback()
            log.exception(e)
    
    def get_connected_devices(self):
        """
        Returns a list of connected devices in database
        """
        try:
            self.cur.execute("SELECT * FROM connected_devices;")

            return self.cur.fetchall()

        except Exception as e:
            self.db.rollback()
            log.exception(e)

    def delete_old_storage_data(self):
        try:
            today = datetime.today()
            older_than = timedelta(days=self.settings.get_max_days_to_store_data())

            old_after = (today - older_than).timestamp() * 1000

            # get all device tables and for each delete older rows 
            # than config specifies
            device_tables = self.get_connected_devices()
            log.debug(device_tables)
            for device in device_tables:
                self.cur.execute("DELETE FROM " + device + " WHERE timestamp <= " + str(old_after))
                self.db.commit()

        except Exception as e:
            self.db.rollback()
            log.exception(e)

    # TESTED
    # PROCESSING
    def process(self):
        try:
            # Signalization so that we can spam call process()
            if self.__writing == False:
                self.__writing = True
                while(self.processQueue.qsize() > 0):
                    
                    req = self.processQueue.get()

                    log.debug("Processing %s" % req.type)
                    if req.type is DatabaseActionType.WRITE_DATA_STORAGE:

                        message = req.get_data()

                        h = sha1()
                        h.update(bytes(message.get("deviceName"), 'utf-8'))
                        
                        device_table = h.hexdigest()[:10].upper()
                        device_table = "_" + device_table
                        
                        timestamp = message.get("ts", message.get("timestamp", int(time()) * 1000))


                        self.cur.execute('''
                            INSERT INTO ''' + device_table + '''(timestamp, message) VALUES (?, ?);''',[timestamp, dumps(message)])
                        
                        self.db.commit()

                        self.readQueue.put(dumps(message))

                        continue

                    if req.type is DatabaseActionType.WRITE_STORAGE_INDEX:

                        # 0 - deviceName
                        # 1 - storageIndex
                        data = req.get_data()

                        log.debug("%s" % str(data))
                        # log.debug("Updating device %s storage data index to: %d" % (data[0], data[1]))
                        self.cur.execute('''
                            UPDATE connected_devices SET dataSavedIndex = ?  WHERE deviceName = ?''', [data[1], data[0]])
                        
                        self.db.commit()
                        continue
                    
                    if req.type is DatabaseActionType.READ_DEVICE:
                        # Expects 2 arguments:
                        # - DeviceName
                        # - ts (timestamp from which to read to present)

                        data = req.get_data()

                        deviceName = data.get("deviceName")
                        ts = data.get("ts")                       

                        h = sha1()
                        h.update(bytes(deviceName, 'utf-8'))

                        device_table = h.hexdigest()[:10].upper()
                        device_table = "_" + device_table
                        self.cur.execute('''
                        SELECT message FROM ''' + device_table + " WHERE timestamp >= ? ;", [ts])

                        data_pack = self.cur.fetchall()
                        log.debug(str(data_pack))

                        for single_data in data_pack:
                            self.readQueue.put(single_data[0])
                        continue

                    if req.type is DatabaseActionType.READ_CONNECTED_DEVICES:

                        # here req.get_data() returns handle from storage_handler.py to set self.connected_devices
                        data = req.get_data()
                        
                        connected_devices_querry = self.get_connected_devices()

                        devices = {}
                        for device in connected_devices_querry:
                            devices[device[0]] = {"connector": device[2], "device_type": device[1], 'data_saved_index': device[3], 'data_uploaded_index': device[4]}
                            log.debug("Appending device %s to return connected_devices" % device[0]) 
                        log.debug("Returning %s" % str(devices))

                        data.connected_devices = devices
                        continue

                self.__writing = False

        except Exception as e:
            self.db.rollback()
            log.exception(e)

    # TESTED
    def readAll(self, deviceName):
        try:
            h = sha1()
            h.update(bytes(deviceName, 'utf-8'))

            device_table = h.hexdigest()[:10].upper()
            device_table = "_" + device_table
            self.cur.execute('''
            SELECT message FROM ''' + device_table + ";")

            return self.cur.fetchall()

        except Exception as e:
            self.db.rollback()
            log.exception(e)

    def readFrom(self, deviceName, ts):
        try:
            h = sha1()
            h.update(bytes(deviceName, 'utf-8'))

            device_table = h.hexdigest()[:10].upper()
            device_table = "_" + device_table
            self.cur.execute('''
            SELECT message FROM ''' + device_table + " WHERE timestamp >= ? ;", [ts])

            return self.cur.fetchall()

        except Exception as e:
            self.db.rollback()
            log.exception(e)


    def setProcessQueue(self, processQueue):
        self.processQueue = processQueue

    def setReadQueue(self, readQueue):
        self.readQueue = readQueue

    def is_writing(self):
        return self.__writing        

    def closeDB(self):
        self.db.close()