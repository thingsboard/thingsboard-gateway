# Generic interface to allow getting data from different BLE sensors
class ExtensionInterface:
    def __init__(self, noti_supported=False):
        self.noti_supported = noti_supported

    def poll(self, bt_device):
        pass

    def start_notify(self, bt_device):
        pass

    def stop_notify(self):
        pass

    def handle_notify(self, handle, data):
        pass

    def notify_started(self):
        return False

    def notify_supported(self):
        return self.noti_supported
