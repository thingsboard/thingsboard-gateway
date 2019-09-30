from storage.event_storage import EventStorage
from tb_utility.tb_utility import TBUtility
import yaml
import queue
import logging

#log = logging.getLogger(__name__)


class FileEventStorage(EventStorage):

    def put(self, event):
        pass

    def get_event_pack(self):
        pass

    def event_pack_processing_done(self):
        pass
