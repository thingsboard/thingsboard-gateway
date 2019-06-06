from . import ExtensionInterface

# Extracts data from ESP test device and sends it to TB. Both polling and notifications
# are supported.
class Extension(ExtensionInterface.ExtensionInterface):
    def __init__(self):
        ExtensionInterface.ExtensionInterface.__init__(self, noti_supported=True)
        self.noti_started = False

    def poll(self, bt_device):

        # Example of getting values using direct 'read' mechanism

        esp_service = bt_device.getServiceByUUID("000000ff-0000-1000-8000-00805f9b34fb")
        esp_char = esp_service.getCharacteristics("0000ff00-0000-1000-8000-00805f9b34fb")[0]
        char_value = str(esp_char.read(), 'utf-8')

        return { "esp_char" : char_value.strip('\u0000') }

    def start_notify(self, bt_device):
        # No need in a special preparation before notification starts
        self.noti_started = True

    def stop_notify(self):
        # No need in a special preparation before notification starts
        self.noti_started = False

    def notify_started(self):
        return self.noti_started

    def handle_notify(self, handle, data):
        # Helper routine
        def bcd_to_decimal(num):
            return (10000 * ((num & 0xf0000) >> 16)) + \
                   (1000 * ((num & 0xf000) >> 12)) + \
                   (100 * ((num & 0xf00) >> 8)) + \
                   (10 * ((num & 0xf0) >> 4)) + \
                   (1 * ((num & 0xf) >> 0))

        temperature = bcd_to_decimal(data[0]) / 100 + bcd_to_decimal(data[1] + data[2])
        humidity = bcd_to_decimal(data[4]) / 100 + bcd_to_decimal(data[5] + data[6])

        if data[3] == 0x13:
            temperature *= -1
        if data[7] == 0x13:
            humidity *= -1

        print("Received GATT data from ESP: T={}, H={}".format(temperature, humidity))
        return { "temperature" : temperature, "humidity" : humidity }
