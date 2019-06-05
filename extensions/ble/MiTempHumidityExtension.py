from extensions import ExtensionInterface

from bluepy.btle import DefaultDelegate, Peripheral, Scanner

# Extracts data from MI sensor
class Extension(ExtensionInterface.ExtensionInterface):
    def __init__(self):
        # Full-blown notifications are not supported: MI device will disconnect after a
        # few notifications sent.
        # So it is better to use poll mechanism instead
        ExtensionInterface.ExtensionInterface.__init__(self)

    def poll(self, bt_device):
        class MI_Delegate(DefaultDelegate):
            def __init__(self):
                DefaultDelegate.__init__(self)
                self.telemetry = {}

            def handleNotification(self, handle, data):
                print("Received data:", data)
                self.telemetry = { "temperature" : float(data[2:6]), "humidity" : float(data[9:13]) }

        delegate = MI_Delegate()
        bt_device.withDelegate(delegate)

        # This is a required part of MI sensor protocol.
        # Without it, notification will not be delivered.
        # For some reason the characteristic is not advertised, meaning it is not possible to use
        # UUID here. Instead a write operation is performed by handle.
        bt_device.writeCharacteristic(0x10, b'\x01\x00', True)
        bt_device.waitForNotifications(1)
        return delegate.telemetry
