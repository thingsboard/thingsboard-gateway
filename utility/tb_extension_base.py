from threading import Thread


class TBExtension(Thread):
    def __init__(self, extension_id, gateway):
        super(TBExtension, self).__init__()
        self.daemon = True
        self.gateway = gateway
        self.ext_id = extension_id
        self.start()
