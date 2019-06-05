from bluepy.btle import DefaultDelegate, Peripheral, Scanner
from tb_modbus_transport_manager import TBModbusTransportManager
from json import load
from importlib import import_module
import os.path
import logging
log = logging.getLogger(__name__)


class TBBluethoothLE:
    class ScanDelegate(DefaultDelegate):
        def __init__(self):
            DefaultDelegate.__init__(self)

        def handleDiscovery(self, dev, isNewDev, isNewData):
            if isNewDev:
                log.debug("Discovered BT device: {}".format(dev.addr))
            elif isNewData:
                log.debug("Received new data from: {}".format(dev.addr))

    def __init__(self, gateway, config_file):
        with open(config_file) as config:
            config = load(config)
            self.gateway = gateway
            self.known_devices = {}
            self.scan_duration = TBModbusTransportManager.get_parameter(config, "scan_duration", 15)
            # CURRENT_SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
            for ble_name, extension_data in config["devices"]:
                extension_module = import_module("extension.ble." + extension_data["extension"])
                extension_class = extension_module.Extension
                self.known_devices[ble_name] = {"extension": extension_class, "scanned": {}}
            #todo add rescan job

    def rescan(self):
        #todo remove all ble jobs for connecting at the beginning
        for dev, dev_data in self.known_devices.items():
            for scanned, scanned_data in dev_data["scanned"].items():
                tb_name = scanned_data["tb_name"]
                # tb_gateway.gw_connect_device(tb_name)
                # tb_gateway.gw_send_attributes(tb_name, {"active": False})
                self.gateway.mqtt_gateway.tb_gateway.gw_disconnect_device(tb_name)
            dev_data["scanned"].clear()
        known_devices_found = False
        while not known_devices_found:
            try:
                scanner = Scanner().withDelegate(self.ScanDelegate())
                devices = scanner.scan(self.scan_duration)
                for device in devices:
                    log.info("Device {} ({}), RSSI={} dB".format(device.addr, device.addrType, device.rssi))
                    for (adtype, desc, value) in device.getScanData():
                        log.debug("  {} = {}".format(desc, value))
                        if desc == "Complete Local Name" and value in self.known_devices:
                            log.debug("Known device found:", value)
                            tb_name = value + "_" + device.addr.replace(':', '').upper()

                            self.known_devices[value]["scanned"][device.addr] = {
                                "inst": self.known_devices[value]["extension"](),
                                "periph": Peripheral(),
                                "tb_name": tb_name
                            }
                            # todo add polling job here
                            # todo add rpc handler processing
                            self.gateway.on_device_connected(tb_name, "Nonehandler", "nonerpchandler")
                            # todo check if added device has "active: true" attribute

                            known_devices_found = True
            except Exception as e:
                log.error(e)

    def get_data_from_device(self, type, type_data):
        for dev_addr, dev_data in type_data["scanned"].items():
            ble_periph = dev_data["periph"]
            try:
                instance = dev_data["inst"]
                tb_dev_name = dev_data["tb_name"]
                telemetry = {}
                log.debug("Connecting to device: {}".format(tb_dev_name))
                ble_periph.connect(dev_addr, "public")
                if instance.notify_supported():
                    if not instance.notify_started():
                        instance.start_notify(ble_periph)

                    class NotiDelegate(DefaultDelegate):
                        def __init__(self):
                            DefaultDelegate.__init__(self)
                            self.dev_instance = instance
                            self.telemetry = {}

                        def handleNotification(self, handle, data):
                            log.debug("Received notifications for handle: {}".format(handle))
                            self.telemetry = self.dev_instance.handle_notify(handle, data)
                    log.debug("Getting notification from: {}".format(tb_dev_name))
                    delegate = NotiDelegate()
                    ble_periph.withDelegate(delegate)
                    if ble_periph.waitForNotifications(1):
                        log.debug("Data received: {}".format(delegate.telemetry))
                    telemetry.update(delegate.telemetry)
                log.debug("Polling data from: {}".format(tb_dev_name))
                poll_telemetry = instance.poll(ble_periph)
                log.debug("Data received: {}".format(poll_telemetry))
                telemetry.update(poll_telemetry)
                if not telemetry:
                    continue
                log.debug(telemetry)
                #todo check if data changed here
                #todo send_to_storage
            except Exception as e:
                print("Exception caught:", e)
            finally:
                print("Disconnecting from device")
                ble_periph.disconnect()
        pass
    # todo add write rpc processing
