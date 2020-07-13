#      Copyright 2020. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

from threading import Thread
from time import sleep, time
from random import choice
from string import ascii_lowercase
from socket import gethostbyname

from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    import puresnmp
except ImportError:
    TBUtility.install_package("puresnmp")
    import puresnmp

from puresnmp.exc import Timeout as SNMPTimeoutException


class SNMPConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.daemon = True
        self.__gateway = gateway
        self._connected = False
        self.__stopped = False
        self._connector_type = connector_type
        self.__config = config
        self.__devices = self.__config["devices"]
        self.setName(config.get("name", 'SNMP Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self._default_converters = {
            "uplink": "SNMPUplinkConverter",
            "downlink": "SNMPDownlinkConverter"
        }
        self.__methods = ["get", "multiget", "getnext", "multigetnext", "walk", "multiwalk", "set", "multiset", "bulkget", "bulkwalk", "table", "bulktable"]
        self.__datatypes = ('attributes', 'telemetry')

    def open(self):
        self.__stopped = False
        self.__fill_converters()
        self.start()

    def run(self):
        self._connected = True
        try:
            while not self.__stopped:
                current_time = time()*1000
                for device in self.__devices:
                    try:
                        if device.get("previous_poll_time", 0) + device.get("pollPeriod", 10000) < current_time:
                            self.__process_data(device)
                            device["previous_poll_time"] = current_time
                    except Exception as e:
                        log.exception(e)
                if self.__stopped:
                    break
                else:
                    sleep(.01)
        except Exception as e:
            log.exception(e)

    def close(self):
        self.__stopped = True
        self._connected = False

    def get_name(self):
        return self.name

    def is_connected(self):
        return self._connected

    def on_attributes_update(self, content):
        log.debug(content)

    def server_side_rpc_handler(self, content):
        log.debug(content)

    def collect_statistic_and_send(self, connector_name, data):
        self.statistics["MessagesReceived"] = self.statistics["MessagesReceived"] + 1
        self.__gateway.send_to_storage(connector_name, data)
        self.statistics["MessagesSent"] = self.statistics["MessagesSent"] + 1

    def __process_data(self, device):
        common_parameters = {
            "ip": gethostbyname(device["ip"]),
            "port": device.get("port", 161),
            "timeout": device.get("timeout", 6),
            "community": device["community"],
        }
        converted_data = {}
        for datatype in self.__datatypes:
            for datatype_config in device[datatype]:
                try:
                    response = None
                    method = datatype_config.get("method")
                    if method is None:
                        log.error("Method not found in configuration: %r", datatype_config)
                        continue
                    else:
                        method = method.lower()
                    if method not in self.__methods:
                        log.error("Unknown method: %s, configuration is: %r", method, datatype_config)
                    response = self.__process_methods(method, common_parameters, datatype_config)
                    converted_data.update(**device["uplink_converter"].convert((datatype, datatype_config), response))
                except SNMPTimeoutException:
                    log.error("Timeout exception on connection to device \"%s\" with ip: \"%s\"", device["deviceName"], device["ip"])
                    return
                except Exception as e:
                    log.exception(e)
                if isinstance(converted_data, dict) and (converted_data.get("attributes") or converted_data.get("telemetry")):
                    self.collect_statistic_and_send(self.get_name(), converted_data)


    def __process_methods(self, method, common_parameters, datatype_config):
        response = None

        if method == "get":
            oid = datatype_config["oid"]
            response = puresnmp.get(**common_parameters,
                                    oid=oid)
        if method == "multiget":
            oids = datatype_config["oid"]
            oids = oids if isinstance(oids, list) else list(oids)
            response = puresnmp.multiget(**common_parameters,
                                         oids=oids)
        if method == "getnext":
            oid = datatype_config["oid"]
            master_response = puresnmp.getnext(**common_parameters,
                                               oid=oid)
            response = {master_response.oid: master_response.value}
        if method == "multigetnext":
            oids = datatype_config["oid"]
            oids = oids if isinstance(oids, list) else list(oids)
            master_response = puresnmp.multigetnext(**common_parameters,
                                                    oids=oids)
            response = {binded_var.oid: binded_var.value for binded_var in master_response}
        if method == "walk":
            oid = datatype_config["oid"]
            response = {binded_var.oid: binded_var.value for binded_var in list(puresnmp.walk(**common_parameters,
                                                                                              oid=oid))}
        if method == "multiwalk":
            oids = datatype_config["oid"]
            oids = oids if isinstance(oids, list) else list(oids)
            response = {binded_var.oid: binded_var.value for binded_var in list(puresnmp.multiwalk(**common_parameters,
                                                                                                   oids=oids))}
        if method == "set":
            oid = datatype_config["oid"]
            value = datatype_config["value"]
            response = puresnmp.set(**common_parameters,
                                    oid=oid,
                                    value=value)
        if method == "multiset":
            mappings = datatype_config["mappings"]
            response = puresnmp.multiset(**common_parameters,
                                         mappings=mappings)
        if method == "bulkget":
            scalar_oids = datatype_config.get("scalarOid", [])
            scalar_oids = scalar_oids if isinstance(scalar_oids, list) else list(scalar_oids)
            repeating_oids = datatype_config.get("repeatingOid", [])
            repeating_oids = repeating_oids if isinstance(repeating_oids, list) else list(repeating_oids)
            max_list_size = datatype_config.get("maxListSize", 1)
            response = puresnmp.bulkget(**common_parameters,
                                        scalar_oids=scalar_oids,
                                        repeating_oids=repeating_oids,
                                        max_list_size=max_list_size)._asdict()
        if method == "bulkwalk":
            oids = datatype_config["oid"]
            oids = oids if isinstance(oids, list) else list(oids)
            bulk_size = datatype_config.get("bulkSize", 10)
            response = {binded_var.oid: binded_var.value for binded_var in list(puresnmp.bulkwalk(**common_parameters,
                                                                                bulk_size=bulk_size,
                                                                                oids=oids))}
        if method == "table":
            oid = datatype_config["oid"]
            del common_parameters["timeout"]
            num_base_nodes = datatype_config.get("numBaseNodes", 0)
            response = puresnmp.table(**common_parameters,
                                      oid=oid,
                                      num_base_nodes=num_base_nodes)
        if method == "bulktable":
            oid = datatype_config["oid"]
            num_base_nodes = datatype_config.get("numBaseNodes", 0)
            bulk_size = datatype_config.get("bulkSize", 10)
            response = puresnmp.bulktable(**common_parameters,
                                          oid=oid,
                                          num_base_nodes=num_base_nodes,
                                          bulk_size=bulk_size)

        return response

    def __fill_converters(self):
        try:
            for device in self.__devices:
                device["uplink_converter"] = TBUtility.check_and_import("snmp", device.get('converter', self._default_converters["uplink"]))(device)
                device["downlink_converter"] = TBUtility.check_and_import("snmp", device.get('converter', self._default_converters["downlink"]))(device)
        except Exception as e:
            log.exception(e)
