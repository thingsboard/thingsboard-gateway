#     Copyright 2021. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from hashlib import sha1
from os import path
from pathlib import Path
from time import sleep
from random import choice
from string import ascii_lowercase
from threading import Thread
from simplejson import dumps, load

from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    import pyodbc
except ImportError:
    print("ODBC library not found - installing...")
    TBUtility.install_package("pyodbc")
    import pyodbc

from thingsboard_gateway.connectors.odbc.odbc_uplink_converter import OdbcUplinkConverter

from thingsboard_gateway.connectors.connector import Connector, log


class OdbcConnector(Connector, Thread):
    DEFAULT_SEND_IF_CHANGED = False
    DEFAULT_RECONNECT_STATE = True
    DEFAULT_SAVE_ITERATOR = False
    DEFAULT_RECONNECT_PERIOD = 60
    DEFAULT_POLL_PERIOD = 60
    DEFAULT_ENABLE_UNKNOWN_RPC = False
    DEFAULT_OVERRIDE_RPC_PARAMS = False
    DEFAULT_PROCESS_RPC_RESULT = False

    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.daemon = True
        self.setName(config.get("name", 'ODBC Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5))))

        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__gateway = gateway
        self.__connector_type = connector_type
        self.__config = config
        self.__stopped = False

        self.__config_dir = self.__gateway.get_config_path() + "odbc" + path.sep

        self.__connection = None
        self.__cursor = None
        self.__rpc_cursor = None
        self.__iterator = None
        self.__iterator_file_name = ""

        self.__devices = {}

        self.__column_names = []
        self.__attribute_columns = []
        self.__timeseries_columns = []

        self.__converter = OdbcUplinkConverter() if not self.__config.get("converter", "") else \
            TBModuleLoader.import_module(self.__connector_type, self.__config["converter"])

        self.__configure_pyodbc()
        self.__parse_rpc_config()

    def open(self):
        log.debug("[%s] Starting...", self.get_name())
        self.__stopped = False
        self.start()

    def close(self):
        if not self.__stopped:
            self.__stopped = True
            log.debug("[%s] Stopping", self.get_name())

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connection is not None

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        done = False
        try:
            if not self.is_connected():
                log.warning("[%s] Cannot process RPC request: not connected to database", self.get_name())
                raise Exception("no connection")

            is_rpc_unknown = False
            rpc_config = self.__config["serverSideRpc"]["methods"].get(content["data"]["method"])
            if rpc_config is None:
                if not self.__config["serverSideRpc"]["enableUnknownRpc"]:
                    log.warning("[%s] Ignore unknown RPC request '%s' (id=%s)",
                                self.get_name(), content["data"]["method"], content["data"]["id"])
                    raise Exception("unknown RPC request")
                else:
                    is_rpc_unknown = True
                    rpc_config = content["data"].get("params", {})
                    sql_params = rpc_config.get("args", [])
                    query = rpc_config.get("query", "")
            else:
                if self.__config["serverSideRpc"]["overrideRpcConfig"]:
                    rpc_config = {**rpc_config, **content["data"].get("params", {})}

                # The params attribute is obsolete but leave for backward configuration compatibility
                sql_params = rpc_config.get("args") or rpc_config.get("params", [])
                query = rpc_config.get("query", "")

            log.debug("[%s] Processing %s '%s' RPC request (id=%s) for '%s' device: params=%s, query=%s",
                      self.get_name(), "unknown" if is_rpc_unknown else "", content["data"]["method"],
                      content["data"]["id"], content["device"], sql_params, query)

            if self.__rpc_cursor is None:
                self.__rpc_cursor = self.__connection.cursor()

            if query:
                if sql_params:
                    self.__rpc_cursor.execute(query, sql_params)
                else:
                    self.__rpc_cursor.execute(query)
            else:
                if sql_params:
                    self.__rpc_cursor.execute("{{CALL {} ({})}}".format(content["data"]["method"],
                                                                        ("?," * len(sql_params))[0:-1]),
                                              sql_params)
                else:
                    self.__rpc_cursor.execute("{{CALL {}}}".format(content["data"]["method"]))

            done = True
            log.debug("[%s] Processed '%s' RPC request (id=%s) for '%s' device",
                      self.get_name(), content["data"]["method"], content["data"]["id"], content["device"])
        except pyodbc.Warning as w:
            log.warning("[%s] Warning while processing '%s' RPC request (id=%s) for '%s' device: %s",
                        self.get_name(), content["data"]["method"], content["data"]["id"], content["device"], str(w))
        except Exception as e:
            log.error("[%s] Failed to process '%s' RPC request (id=%s) for '%s' device: %s",
                      self.get_name(), content["data"]["method"], content["data"]["id"], content["device"], str(e))
        finally:
            if done and rpc_config.get("result", self.DEFAULT_PROCESS_RPC_RESULT):
                response = self.row_to_dict(self.__rpc_cursor.fetchone())
                self.__gateway.send_rpc_reply(content["device"], content["data"]["id"], response)
            else:
                self.__gateway.send_rpc_reply(content["device"], content["data"]["id"], {"success": done})

    def run(self):
        while not self.__stopped:
            # Initialization phase
            if not self.is_connected():
                while not self.__stopped and \
                        not self.__init_connection() and \
                        self.__config["connection"].get("reconnect", self.DEFAULT_RECONNECT_STATE):
                    reconnect_period = self.__config["connection"].get("reconnectPeriod", self.DEFAULT_RECONNECT_PERIOD)
                    log.info("[%s] Will reconnect to database in %d second(s)", self.get_name(), reconnect_period)
                    sleep(reconnect_period)

                if not self.is_connected():
                    log.error("[%s] Cannot connect to database so exit from main loop", self.get_name())
                    break

                if not self.__init_iterator():
                    log.error("[%s] Cannot init database iterator so exit from main loop", self.get_name())
                    break

            # Polling phase
            try:
                self.__poll()
                if not self.__stopped:
                    polling_period = self.__config["polling"].get("period", self.DEFAULT_POLL_PERIOD)
                    log.debug("[%s] Next polling iteration will be in %d second(s)", self.get_name(), polling_period)
                    sleep(polling_period)
            except pyodbc.Warning as w:
                log.warning("[%s] Warning while polling database: %s", self.get_name(), str(w))
            except pyodbc.Error as e:
                log.error("[%s] Error while polling database: %s", self.get_name(), str(e))
                self.__close()

        self.__close()
        self.__stopped = False
        log.info("[%s] Stopped", self.get_name())

    def __close(self):
        if self.is_connected():
            try:
                self.__cursor.close()
                if self.__rpc_cursor is not None:
                    self.__rpc_cursor.close()
                self.__connection.close()
            finally:
                log.info("[%s] Connection to database closed", self.get_name())
                self.__connection = None
                self.__cursor = None
                self.__rpc_cursor = None

    def __poll(self):
        rows = self.__cursor.execute(self.__config["polling"]["query"], self.__iterator["value"])

        if not self.__column_names:
            for column in self.__cursor.description:
                self.__column_names.append(column[0])
            log.info("[%s] Fetch column names: %s", self.get_name(), self.__column_names)

        # For some reason pyodbc.Cursor.rowcount may be 0 (sqlite) so use our own row counter
        row_count = 0
        for row in rows:
            # log.debug("[%s] Fetch row: %s", self.get_name(), row)
            row_count += 1
            self.__process_row(row)

        self.__iterator["total"] += row_count
        log.info("[%s] Polling iteration finished. Processed rows: current %d, total %d",
                 self.get_name(), row_count, self.__iterator["total"])

        if self.__config["polling"]["iterator"]["persistent"] and row_count > 0:
            self.__save_iterator_config()

    def __process_row(self, row):
        try:
            data = self.row_to_dict(row)

            to_send = {"attributes": {} if "attributes" not in self.__config["mapping"] else
            self.__converter.convert(self.__config["mapping"]["attributes"], data),
                       "telemetry": {} if "timeseries" not in self.__config["mapping"] else
                       self.__converter.convert(self.__config["mapping"]["timeseries"], data)}

            device_name = eval(self.__config["mapping"]["device"]["name"], globals(), data)
            if device_name not in self.__devices:
                self.__devices[device_name] = {"attributes": {}, "telemetry": {}}
                self.__gateway.add_device(device_name, {"connector": self})

            self.__iterator["value"] = getattr(row, self.__iterator["name"])
            self.__check_and_send(device_name,
                                  self.__config["mapping"]["device"].get("type", self.__connector_type),
                                  to_send)
        except Exception as e:
            log.warning("[%s] Failed to process database row: %s", self.get_name(), str(e))

    @staticmethod
    def row_to_dict(row):
        data = {}
        for column_description in row.cursor_description:
            data[column_description[0]] = getattr(row, column_description[0])
        return data

    def __check_and_send(self, device_name, device_type, new_data):
        self.statistics['MessagesReceived'] += 1
        to_send = {"attributes": [], "telemetry": []}
        send_on_change = self.__config["mapping"].get("sendDataOnlyOnChange", self.DEFAULT_SEND_IF_CHANGED)

        for tb_key in to_send.keys():
            for key, new_value in new_data[tb_key].items():
                if not send_on_change or self.__devices[device_name][tb_key].get(key, None) != new_value:
                    self.__devices[device_name][tb_key][key] = new_value
                    to_send[tb_key].append({key: new_value})

        if to_send["attributes"] or to_send["telemetry"]:
            to_send["deviceName"] = device_name
            to_send["deviceType"] = device_type

            log.debug("[%s] Pushing to TB server '%s' device data: %s", self.get_name(), device_name, to_send)

            self.__gateway.send_to_storage(self.get_name(), to_send)
            self.statistics['MessagesSent'] += 1
        else:
            log.debug("[%s] '%s' device data has not been changed", self.get_name(), device_name)

    def __init_connection(self):
        try:
            log.debug("[%s] Opening connection to database", self.get_name())
            connection_config = self.__config["connection"]
            self.__connection = pyodbc.connect(connection_config["str"], **connection_config.get("attributes", {}))
            if connection_config.get("encoding", ""):
                log.info("[%s] Setting encoding to %s", self.get_name(), connection_config["encoding"])
                self.__connection.setencoding(connection_config["encoding"])

            decoding_config = connection_config.get("decoding")
            if decoding_config is not None:
                if isinstance(decoding_config, dict):
                    if decoding_config.get("char", ""):
                        log.info("[%s] Setting SQL_CHAR decoding to %s", self.get_name(), decoding_config["char"])
                        self.__connection.setdecoding(pyodbc.SQL_CHAR, decoding_config["char"])
                    if decoding_config.get("wchar", ""):
                        log.info("[%s] Setting SQL_WCHAR decoding to %s", self.get_name(), decoding_config["wchar"])
                        self.__connection.setdecoding(pyodbc.SQL_WCHAR, decoding_config["wchar"])
                    if decoding_config.get("metadata", ""):
                        log.info("[%s] Setting SQL_WMETADATA decoding to %s",
                                 self.get_name(), decoding_config["metadata"])
                        self.__connection.setdecoding(pyodbc.SQL_WMETADATA, decoding_config["metadata"])
                else:
                    log.warning("[%s] Unknown decoding configuration %s. Read data may be misdecoded", self.get_name(),
                                decoding_config)

            self.__cursor = self.__connection.cursor()
            log.info("[%s] Connection to database opened, attributes %s",
                     self.get_name(), connection_config.get("attributes", {}))
        except pyodbc.Error as e:
            log.error("[%s] Failed to connect to database: %s", self.get_name(), str(e))
            self.__close()

        return self.is_connected()

    def __resolve_iterator_file(self):
        file_name = ""
        try:
            # The algorithm of resolving iterator file name is described in
            # https://thingsboard.io/docs/iot-gateway/config/odbc/#subsection-iterator
            # Edit that description whether algorithm is changed.
            file_name += self.__connection.getinfo(pyodbc.SQL_DRIVER_NAME)
            file_name += self.__connection.getinfo(pyodbc.SQL_SERVER_NAME)
            file_name += self.__connection.getinfo(pyodbc.SQL_DATABASE_NAME)
            file_name += self.get_name()
            file_name += self.__config["polling"]["iterator"]["column"]

            self.__iterator_file_name = sha1(file_name.encode()).hexdigest() + ".json"
            log.debug("[%s] Iterator file name resolved to %s", self.get_name(), self.__iterator_file_name)
        except Exception as e:
            log.warning("[%s] Failed to resolve iterator file name: %s", self.get_name(), str(e))
        return bool(self.__iterator_file_name)

    def __init_iterator(self):
        save_iterator = self.DEFAULT_SAVE_ITERATOR
        if "persistent" not in self.__config["polling"]["iterator"]:
            self.__config["polling"]["iterator"]["persistent"] = save_iterator
        else:
            save_iterator = self.__config["polling"]["iterator"]["persistent"]

        log.info("[%s] Iterator saving %s", self.get_name(), "enabled" if save_iterator else "disabled")

        if save_iterator and self.__load_iterator_config():
            log.info("[%s] Init iterator from file '%s': column=%s, start_value=%s",
                     self.get_name(), self.__iterator_file_name,
                     self.__iterator["name"], self.__iterator["value"])
            return True

        self.__iterator = {"name": self.__config["polling"]["iterator"]["column"],
                           "total": 0}

        if "value" in self.__config["polling"]["iterator"]:
            self.__iterator["value"] = self.__config["polling"]["iterator"]["value"]
            log.info("[%s] Init iterator from configuration: column=%s, start_value=%s",
                     self.get_name(), self.__iterator["name"], self.__iterator["value"])
        elif "query" in self.__config["polling"]["iterator"]:
            try:
                self.__iterator["value"] = \
                    self.__cursor.execute(self.__config["polling"]["iterator"]["query"]).fetchone()[0]
                log.info("[%s] Init iterator from database: column=%s, start_value=%s",
                         self.get_name(), self.__iterator["name"], self.__iterator["value"])
            except pyodbc.Warning as w:
                log.warning("[%s] Warning on init iterator from database: %s", self.get_name(), str(w))
            except pyodbc.Error as e:
                log.error("[%s] Failed to init iterator from database: %s", self.get_name(), str(e))
        else:
            log.error("[%s] Failed to init iterator: value/query param is absent", self.get_name())

        return "value" in self.__iterator

    def __save_iterator_config(self):
        try:
            Path(self.__config_dir).mkdir(exist_ok=True)
            with Path(self.__config_dir + self.__iterator_file_name).open("w") as iterator_file:
                iterator_file.write(dumps(self.__iterator, indent=2, sort_keys=True))
            log.debug("[%s] Saved iterator configuration to %s", self.get_name(), self.__iterator_file_name)
        except Exception as e:
            log.error("[%s] Failed to save iterator configuration to %s: %s",
                      self.get_name(), self.__iterator_file_name, str(e))

    def __load_iterator_config(self):
        if not self.__iterator_file_name:
            if not self.__resolve_iterator_file():
                log.error("[%s] Unable to load iterator configuration from file: file name is not resolved",
                          self.get_name())
                return False

        try:
            iterator_file_path = Path(self.__config_dir + self.__iterator_file_name)
            if not iterator_file_path.exists():
                return False

            with iterator_file_path.open("r") as iterator_file:
                self.__iterator = load(iterator_file)
            log.debug("[%s] Loaded iterator configuration from %s", self.get_name(), self.__iterator_file_name)
        except Exception as e:
            log.error("[%s] Failed to load iterator configuration from %s: %s",
                      self.get_name(), self.__iterator_file_name, str(e))

        return bool(self.__iterator)

    def __configure_pyodbc(self):
        pyodbc_config = self.__config.get("pyodbc", {})
        if not pyodbc_config:
            return

        for name, value in pyodbc_config.items():
            pyodbc.__dict__[name] = value

        log.info("[%s] Set pyodbc attributes: %s", self.get_name(), pyodbc_config)

    def __parse_rpc_config(self):
        if "serverSideRpc" not in self.__config:
            self.__config["serverSideRpc"] = {}
        if "enableUnknownRpc" not in self.__config["serverSideRpc"]:
            self.__config["serverSideRpc"]["enableUnknownRpc"] = self.DEFAULT_ENABLE_UNKNOWN_RPC

        log.info("[%s] Processing unknown RPC %s", self.get_name(),
                 "enabled" if self.__config["serverSideRpc"]["enableUnknownRpc"] else "disabled")

        if "overrideRpcConfig" not in self.__config["serverSideRpc"]:
            self.__config["serverSideRpc"]["overrideRpcConfig"] = self.DEFAULT_OVERRIDE_RPC_PARAMS

        log.info("[%s] Overriding RPC config %s", self.get_name(),
                 "enabled" if self.__config["serverSideRpc"]["overrideRpcConfig"] else "disabled")

        if "serverSideRpc" not in self.__config or not self.__config["serverSideRpc"].get("methods", []):
            self.__config["serverSideRpc"] = {"methods": {}}
            return

        reformatted_config = {}
        for rpc_config in self.__config["serverSideRpc"]["methods"]:
            if isinstance(rpc_config, str):
                reformatted_config[rpc_config] = {}
            elif isinstance(rpc_config, dict):
                reformatted_config[rpc_config["name"]] = rpc_config
            else:
                log.warning("[%s] Wrong RPC config format. Expected str or dict, get %s", self.get_name(),
                            type(rpc_config))

        self.__config["serverSideRpc"]["methods"] = reformatted_config
