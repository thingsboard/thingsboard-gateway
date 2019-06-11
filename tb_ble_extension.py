from datetime import datetime
from bluepy.btle import DefaultDelegate, Peripheral, Scanner
from tb_utility import TBUtility
from json import load
from importlib import import_module
from time import time
import logging
from threading import Thread
from copy import deepcopy
log = logging.getLogger(__name__)


class TBBluetoothLE(Thread):
    class ScanDelegate(DefaultDelegate):
        def __init__(self):
            DefaultDelegate.__init__(self)

        def handleDiscovery(self, dev, isNewDev, isNewData):
            if isNewDev:
                log.debug("Discovered BT device: {}".format(dev.addr))
            elif isNewData:
                log.debug("Received new data from: {}".format(dev.addr))

    def __init__(self, gateway, config_file):
        super(TBBluetoothLE, self).__init__()
        self.daemon = True
        self.start()

        self.is_scanning = False
        with open(config_file) as config:
            config = load(config)
            self.dict_check_ts_changed = {}
            self.polling_jobs = []
            self.gateway = gateway
            self.known_devices = {}
            self.scan_duration = TBUtility.get_parameter(config, "scan_duration", 15)
            rescan_period = TBUtility.get_parameter(config, "rescanPeriod", 120)
            for ble_name, extension_data in config["devices"].items():
                extension_module = import_module("extensions.ble." + extension_data["extension"])
                extension_class = extension_module.Extension
                self.known_devices[ble_name] = {
                    "extension": extension_class,
                    "scanned": {},
                    "poll_period": TBUtility.get_parameter(extension_data,
                                                           "pollPeriod",
                                                           TBUtility.get_parameter(config, "pollPeriod", 100)),
                    "check_data_changed": TBUtility.get_parameter(extension_data,
                                                                  "sendDataOnlyOnChange",
                                                                  TBUtility.get_parameter(config,
                                                                                          "sendDataOnlyOnChange",
                                                                                          False)),
                    "rpc": extension_data.get("rpc")
                }
            self.gateway.scheduler.add_job(self.rescan, 'interval', seconds=rescan_period, next_run_time=datetime.now())

    def rescan(self):
        if self.is_scanning:
            return True
        self.is_scanning = True
        for job in self.polling_jobs:
            self.gateway.scheduler.remove_job(job)
        self.polling_jobs.clear()
        for dev_data in self.known_devices.values():
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
                            log.debug("Known device found: {}".format(value))
                            tb_name = value + "_" + device.addr.replace(':', '')
                            self.known_devices[value]["scanned"][device.addr] = {
                                "inst": self.known_devices[value]["extension"](),
                                "periph": Peripheral(),
                                "tb_name": tb_name
                            }
                            self.gateway.on_device_connected(tb_name,
                                                             self,
                                                             self.known_devices[value]["rpc"])
                            known_devices_found = True
                for known_device in self.known_devices:
                    for scanned_device in self.known_devices[known_device]["scanned"]:
                        pass
                        # job = self.gateway.scheduler.add_job(self._process_request,
                        #                                      'interval',
                        #                                      max_instances=1,
                        #                                      seconds=self.known_devices[known_device]["poll_period"],
                        #                                      next_run_time=datetime.now(),
                        #                                      args=(known_device,
                        #                                            self.known_devices[known_device]["scanned"][
                        #                                                scanned_device],
                        #                                            scanned_device))
                        # self.polling_jobs.append(job)
            except Exception as e:
                log.error(e)
            self.is_scanning = False

    def _process_request(self, device_type, dev_data, dev_addr):
        ble_periph = dev_data["periph"]
        instance = dev_data["inst"]
        tb_dev_name = dev_data["tb_name"]
        try:
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
            log.debug("telemetry {} for device {}".format(tb_dev_name, telemetry))

            def check_ts_changed(telemetry, device_uniq_name):
                if self.dict_check_ts_changed.get(device_uniq_name) == telemetry:
                    log.debug("value {val} related to device id {id} didn't change".format(val=telemetry,
                                                                                           id=device_uniq_name))
                    return
                log.debug("value {val} related to device id {id} changed".format(val=telemetry,
                                                                                 id=device_uniq_name))
                self.dict_check_ts_changed.update({device_uniq_name: telemetry})
                return True

            # if is_rpc_read_call:
            #     return {"ts": int(round(time() * 1000)), "values": telemetry}

            if telemetry and ((not self.known_devices[device_type]["check_data_changed"]) or
                              check_ts_changed(telemetry, dev_addr)):
                # log.critical(telemetry)
                telemetry = {"ts": int(round(time() * 1000)), "values": telemetry}
                # log.critical(telemetry)
                self.gateway.send_data_to_storage(telemetry, "tms", tb_dev_name)

        except Exception as e:
            log.error("Exception caught: {}".format(e))
        finally:
            log.debug("Disconnecting from device")
            ble_periph.disconnect()

#todo add
    def write_to_device(self, adr, *args):
        esp = Peripheral()
        esp.connect(adr)
        srvs = esp.getServices()
        resp = esp.writeCharacteristic(args[0], args[1], args[2])
        # return resp
        # todo check resp
        esp.disconnect()

    def get_data_from_device_once(self, name):
        copy_devices = deepcopy(self.known_devices)
        for device_type in copy_devices:
            for adr, scanned_device in copy_devices[device_type]["scanned"].items():
                if scanned_device["tb_name"] == name:
                    self.gateway.scheduler.add_job(self._process_request,
                                                   max_instances=1,
                                                   next_run_time=datetime.now(),
                                                   args=(device_type, scanned_device, adr))
                    return
