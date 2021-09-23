from sqlite3 import connect
from thingsboard_gateway.storage.sqlite.storage_settings import StorageSettings


from logging import getLogger

log = getLogger("storage")


class DatabaseConnector:
    def __init__(self, settings: StorageSettings):
        self.data_file_path = settings.get_data_file_path()
        self.connection = None

    def connect(self):
        """
        Create database file in path from settings
        """
        try:
            self.connection = connect(self.data_file_path, check_same_thread=False)
        except Exception as e:
            log.exception(e)

    def commit(self):
        """
        Commit changes
        """
        log.debug("Committing changes to DB")
        try:
            self.connection.commit()

        except Exception as e:
            log.exception(e)

    def rollback(self):
        """
        Rollback changes after exception
        """
        log.debug("Rollback transaction")
        try:
            self.connection.rollback()

        except Exception as e:
            log.exception(e)

    def close(self):
        """
        Closes database file
        """
        try:
            self.connection.close()

        except Exception as e:
            log.exception(e)

    def get_cursor(self):
        try:
            return self.connection.cursor()

        except Exception as e:
            log.exception(e)
