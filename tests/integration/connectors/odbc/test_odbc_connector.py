# #     Copyright 2020. ThingsBoard
# #
# #     Licensed under the Apache License, Version 2.0 (the "License");
# #     you may not use this file except in compliance with the License.
# #     You may obtain a copy of the License at
# #
# #         http://www.apache.org/licenses/LICENSE-2.0
# #
# #     Unless required by applicable law or agreed to in writing, software
# #     distributed under the License is distributed on an "AS IS" BASIS,
# #     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# #     See the License for the specific language governing permissions and
# #     limitations under the License.
#
# import logging
# import unittest
# from os import path
# from pathlib import Path
# from time import sleep
# from unittest.mock import Mock, call
#
# try :
#     import pyodbc
# except (ImportError, ModuleNotFoundError):
#     from thingsboard_gateway.tb_utility.tb_utility import TBUtility
#     from sys import platform
#     if platform == "darwin":
#         from subprocess import check_call, CalledProcessError
#         import site
#         from importlib import reload
#
#         check_call(["brew", "install", "unixodbc"])
#         check_call(["pip", "install", "--no-binary", ":all:", "pyodbc"])
#         reload(site)
#     else:
#         TBUtility.install_package("pyodbc")
#     import pyodbc
# from simplejson import load
# from thingsboard_gateway.tb_utility.tb_utility import TBUtility
#
# from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService
# from thingsboard_gateway.connectors.odbc.odbc_connector import OdbcConnector
#
# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S')
#
# ODBC_DRIVER_WITH_STORED_PROCEDURE = "postgres"
# ODBC_DRIVER_WITHOUT_STORED_PROCEDURE = "sqlite"
# IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED = False
# IS_ODBC_DRIVER_INSTALLED = False
#
# for driver_name in pyodbc.drivers():
#     IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED = IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED or \
#                                                      ODBC_DRIVER_WITH_STORED_PROCEDURE in driver_name.lower()
#     IS_ODBC_DRIVER_INSTALLED = IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED or \
#                                ODBC_DRIVER_WITHOUT_STORED_PROCEDURE in driver_name.lower()
#
# if IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED:
#     try:
#         import testing.postgresql
#     except ImportError:
#         print("ODBC library not found - installing...")
#         TBUtility.install_package("testing.postgresql")
#         import testing.postgresql
#
#
# @unittest.skipIf(not IS_ODBC_DRIVER_INSTALLED,
#                  "To run ODBC tests install " + ODBC_DRIVER_WITH_STORED_PROCEDURE + " or " +
#                  ODBC_DRIVER_WITHOUT_STORED_PROCEDURE + " ODBC driver")
# class OdbcConnectorTests(unittest.TestCase):
#     CONFIG_PATH = path.join(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))),
#                             "data" + path.sep + "odbc" + path.sep)
#     POSTGRES_PORT = 12345
#
#     def setUp(self):
#         self.gateway = Mock(spec=TBGatewayService)
#         self.gateway.get_config_path.return_value = self.CONFIG_PATH
#
#         self.connector = None
#         self.config = None
#         self.db_connection = None
#         self.db_cursor = None
#         self.db_connection_str = self._get_connection_string()
#
#         if IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED:
#             self.postgresql = testing.postgresql.Postgresql(port=self.POSTGRES_PORT)
#             # To prevent database overlapping start each test server on different port
#             self.POSTGRES_PORT += 1
#
#     def tearDown(self):
#         self.connector.close()
#
#         if self.config["polling"]["iterator"].get("persistent", OdbcConnector.DEFAULT_SAVE_ITERATOR):
#             Path(self.CONFIG_PATH + "odbc" + path.sep + self.connector._OdbcConnector__iterator_file_name).unlink()
#             Path(self.CONFIG_PATH + "odbc").rmdir()
#
#         if self.db_connection:
#             self.db_cursor.close()
#             self.db_connection.close()
#
#         if IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED:
#             self.postgresql.stop()
#
#     def _create_connector(self,
#                           config_file_name,
#                           send_on_change=OdbcConnector.DEFAULT_SEND_IF_CHANGED,
#                           unknown_rpc=OdbcConnector.DEFAULT_ENABLE_UNKNOWN_RPC,
#                           override_rpc_params=OdbcConnector.DEFAULT_OVERRIDE_RPC_PARAMS,
#                           connection_str="",
#                           reconnect=OdbcConnector.DEFAULT_RECONNECT_STATE):
#         with open(self.CONFIG_PATH + config_file_name, 'r', encoding="UTF-8") as file:
#             self.config = load(file)
#
#             if "sendDataOnlyOnChange" not in self.config["mapping"]:
#                 self.config["mapping"]["sendDataOnlyOnChange"] = send_on_change
#
#             if "serverSideRpc" not in self.config:
#                 self.config["serverSideRpc"] = {}
#
#             if "enableUnknownRpc" not in self.config["serverSideRpc"]:
#                 self.config["serverSideRpc"]["enableUnknownRpc"] = unknown_rpc
#
#             if "overrideRpcConfig" not in self.config["serverSideRpc"]:
#                 self.config["serverSideRpc"]["overrideRpcConfig"] = override_rpc_params
#
#             self.config["connection"]["reconnect"] = reconnect
#
#             if connection_str:
#                 self.config["connection"]["str"] = connection_str
#             else:
#                 self.config["connection"]["str"] = self.db_connection_str
#                 self._init_test_db()
#
#             self.connector = OdbcConnector(self.gateway, self.config, "odbc")
#             self.connector.open()
#             sleep(1)  # some time to init
#
#     def _get_connection_string(self):
#         return "Driver={PostgreSQL};" \
#                "Server=localhost;" \
#                "Port=" + str(self.POSTGRES_PORT) + ";" \
#                                                    "Database=test;" \
#                                                    "Uid=postgres;" \
#                                                    "Pwd=postgres;" if IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED else \
#             "Driver={SQLITE3};" \
#             "Database=" + self.CONFIG_PATH + "sqlite3.db;"
#
#     def _init_test_db(self):
#         self.db_connection = pyodbc.connect(self.config["connection"]["str"])
#         self.db_cursor = self.db_connection.cursor()
#
#         if IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED:
#             with open(self.CONFIG_PATH + 'postgres.sql', 'r') as f:
#                 self.db_cursor.execute(f.read())
#                 self.db_cursor.commit()
#
#     def test_override_default_pyodbc_attributes(self):
#         self._create_connector("odbc_timeseries.json")
#         self.assertEqual(pyodbc.pooling, self.config["pyodbc"]["pooling"])
#         self.assertEqual(pyodbc.native_uuid, self.config["pyodbc"]["native_uuid"])
#
#     def test_override_default_connection_attributes(self):
#         self._create_connector("odbc_timeseries.json")
#         self.assertEqual(self.connector._OdbcConnector__connection.autocommit,
#                          self.config["connection"]["attributes"]["autocommit"])
#         self.assertEqual(self.connector._OdbcConnector__connection.timeout,
#                          self.config["connection"]["attributes"]["timeout"])
#
#     def test_timeseries(self):
#         self._create_connector("odbc_timeseries.json")
#         record_count, device_id = self.db_cursor.execute("SELECT COUNT(*), device_id "
#                                                          "FROM timeseries "
#                                                          "GROUP BY device_id").fetchone()
#
#         self.gateway.add_device.assert_called_once_with(eval(self.config["mapping"]["device"]["name"]),
#                                                         {"connector": self.connector})
#         self.assertEqual(self.gateway.send_to_storage.call_count, record_count)
#
#     def test_timeseries_send_on_change(self):
#         self._create_connector("odbc_timeseries.json", True)
#         rows = self.db_cursor.execute("SELECT DISTINCT long_v "
#                                       "FROM timeseries "
#                                       "ORDER BY long_v ASC").fetchall()
#
#         self.assertEqual(self.gateway.send_to_storage.call_count, len(rows))
#
#         calls = []
#         device_id = 1  # for eval
#         for row in rows:
#             calls.append(call(self.connector.get_name(),
#                               {"deviceName": eval(self.config["mapping"]["device"]["name"]),
#                                "deviceType": self.config["mapping"]["device"]["type"],
#                                "attributes": [],
#                                "telemetry": [{"value": row.long_v}]}))
#
#         self.gateway.send_to_storage.assert_has_calls(calls)
#
#     def test_attributes(self):
#         self._create_connector("odbc_attributes.json")
#         rows = self.db_cursor.execute("SELECT key, device_id, bool_v, str_v, long_v, dbl_v "
#                                       "FROM attributes "
#                                       "ORDER BY ts ASC").fetchall()
#
#         self.assertEqual(self.gateway.send_to_storage.call_count, len(rows))
#
#         calls = []
#         device_id = 1  # for eval
#         for row in rows:
#             data = {
#                 "bool_v": row[2],
#                 "str_v": row[3],
#                 "long_v": row[4],
#                 "dbl_v": row[5]}
#             device_id = row.device_id  # for eval
#             calls.append(call(self.connector.get_name(),
#                               {"deviceName": eval(self.config["mapping"]["device"]["name"]),
#                                "deviceType": self.connector._OdbcConnector__connector_type,
#                                "attributes": [
#                                    {row[0]: eval(self.config["mapping"]["attributes"][0]["value"],
#                                                  globals(),
#                                                  data)}],
#                                "telemetry": []}))
#
#         self.gateway.send_to_storage.assert_has_calls(calls)
#
#     @unittest.skipIf(not IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED,
#                      "To run RPC ODBC tests install " + ODBC_DRIVER_WITH_STORED_PROCEDURE +
#                      " database and add it to PATH")
#     def test_rpc_without_param(self):
#         self._create_connector("odbc_rpc.json")
#         value_before = self.db_cursor.execute("SELECT get_integer_value(?)", "inc_value").fetchval()
#
#         self.connector.server_side_rpc_handler({"device": "someDevice",
#                                                 "data": {
#                                                     "id": 1,
#                                                     "method": "decrement_value"
#                                                 }})
#         sleep(1)
#
#         # Cursor from test connection doesn't see changes made through connector's cursor,
#         # so use latter to get up to date data
#         value_after = self.connector._OdbcConnector__rpc_cursor.execute("SELECT get_integer_value(?)",
#                                                                         "inc_value").fetchval()
#         self.assertEqual(value_before - 1, value_after)
#
#     @unittest.skipIf(not IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED,
#                      "To run RPC ODBC tests install " + ODBC_DRIVER_WITH_STORED_PROCEDURE +
#                      " database and add it to PATH")
#     def test_rpc_without_param_with_config_query(self):
#         self._create_connector("odbc_rpc.json")
#         value_before = self.db_cursor.execute("SELECT get_integer_value(?)", "inc_value").fetchval()
#
#         self.connector.server_side_rpc_handler({"device": "someDevice",
#                                                 "data": {
#                                                     "id": 1,
#                                                     "method": "increment_value"
#                                                 }})
#         sleep(1)
#
#         # Cursor from test connection doesn't see changes made through connector's cursor,
#         # so use latter to get up to date data
#         value_after = self.connector._OdbcConnector__rpc_cursor.execute("SELECT get_integer_value(?)",
#                                                                         "inc_value").fetchval()
#         self.assertEqual(value_before + 1, value_after)
#
#     @unittest.skipIf(not IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED,
#                      "To run RPC ODBC tests install " + ODBC_DRIVER_WITH_STORED_PROCEDURE +
#                      " database and add it to PATH")
#     def test_rpc_with_param(self):
#         self._create_connector("odbc_rpc.json")
#         values_before = self.config["serverSideRpc"]["methods"]["reset_values"]["params"]
#
#         self.connector.server_side_rpc_handler({"device": "someDevice",
#                                                 "data": {
#                                                     "id": 1,
#                                                     "method": "reset_values"
#                                                 }})
#         sleep(1)
#
#         # Cursor from test connection doesn't see changes made through connector's cursor,
#         # so use latter to get up to date data
#         str_value_after = self.connector._OdbcConnector__rpc_cursor.execute("SELECT get_string_value(?)",
#                                                                             "str_value").fetchval()
#         int_value_after = self.connector._OdbcConnector__rpc_cursor.execute("SELECT get_integer_value(?)",
#                                                                             "int_value").fetchval()
#         self.assertEqual(values_before[0], str_value_after)
#         self.assertEqual(values_before[1], int_value_after)
#
#     @unittest.skipIf(not IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED,
#                      "To run RPC ODBC tests install " + ODBC_DRIVER_WITH_STORED_PROCEDURE +
#                      " database and add it to PATH")
#     def test_rpc_with_param_with_config_query(self):
#         self._create_connector("odbc_rpc.json")
#         values_before = self.config["serverSideRpc"]["methods"]["update_values"]["params"]
#
#         self.connector.server_side_rpc_handler({"device": "someDevice",
#                                                 "data": {
#                                                     "id": 1,
#                                                     "method": "update_values"
#                                                 }})
#         sleep(1)
#
#         # Cursor from test connection doesn't see changes made through connector's cursor,
#         # so use latter to get up to date data
#         str_value_after = self.connector._OdbcConnector__rpc_cursor.execute("SELECT get_string_value(?)",
#                                                                             "str_value").fetchval()
#         int_value_after = self.connector._OdbcConnector__rpc_cursor.execute("SELECT get_integer_value(?)",
#                                                                             "int_value").fetchval()
#         self.assertEqual(values_before[0], str_value_after)
#         self.assertEqual(values_before[1], int_value_after)
#
#     @unittest.skipIf(not IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED,
#                      "To run RPC ODBC tests install " + ODBC_DRIVER_WITH_STORED_PROCEDURE +
#                      " database and add it to PATH")
#     def test_rpc_with_param_override(self):
#         self._create_connector("odbc_rpc.json", override_rpc_params=True)
#
#         data = {"device": "someDevice",
#                 "data": {
#                     "id": 1,
#                     "method": "reset_values",
#                     "params": {
#                         "args": ["override_value", 12345],
#                         "query": "SELECT reset_values(?,?)"
#                     }
#                 }}
#         self.connector.server_side_rpc_handler(data)
#         sleep(1)
#
#         # Cursor from test connection doesn't see changes made through connector's cursor,
#         # so use latter to get up to date data
#         str_value_after = self.connector._OdbcConnector__rpc_cursor.execute("SELECT get_string_value(?)",
#                                                                             "str_value").fetchval()
#         int_value_after = self.connector._OdbcConnector__rpc_cursor.execute("SELECT get_integer_value(?)",
#                                                                             "int_value").fetchval()
#         self.assertEqual(data["data"]["params"]["args"][0], str_value_after)
#         self.assertEqual(data["data"]["params"]["args"][1], int_value_after)
#
#         self.gateway.send_rpc_reply.assert_has_calls([call(data["device"],
#                                                            data["data"]["id"],
#                                                            {"success": True})])
#
#     @unittest.skipIf(not IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED,
#                      "To run RPC ODBC tests install " + ODBC_DRIVER_WITH_STORED_PROCEDURE +
#                      " database and add it to PATH")
#     def test_unknown_rpc_disabled(self):
#         self._create_connector("odbc_rpc.json")
#
#         data = {"device": "someDevice",
#                 "data": {
#                     "id": 1,
#                     "method": "unknown_function"
#                 }}
#         self.connector.server_side_rpc_handler(data)
#         sleep(1)
#
#         self.gateway.send_rpc_reply.assert_has_calls([call(data["device"],
#                                                            data["data"]["id"],
#                                                            {"success": False})])
#
#     @unittest.skipIf(not IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED,
#                      "To run RPC ODBC tests install " + ODBC_DRIVER_WITH_STORED_PROCEDURE +
#                      " database and add it to PATH")
#     def test_unknown_rpc_enabled_without_query_and_result(self):
#         self._create_connector("odbc_rpc.json", unknown_rpc=True)
#         inc_value_before = self.db_cursor.execute("SELECT get_integer_value(?)", "inc_value").fetchval()
#
#         data = {"device": "someDevice",
#                 "data": {
#                     "id": 1,
#                     "method": "secret_function",
#                     "params": {
#                         "args": ["secret_string"],
#                         "result": True
#                     }
#                 }}
#         self.connector.server_side_rpc_handler(data)
#         sleep(1)
#
#         # Cursor from test connection doesn't see changes made through connector's cursor,
#         # so use latter to get up to date data
#         str_value_after = self.connector._OdbcConnector__rpc_cursor.execute("SELECT get_string_value(?)",
#                                                                             "str_value").fetchval()
#         inc_value_after = self.connector._OdbcConnector__rpc_cursor.execute("SELECT get_integer_value(?)",
#                                                                             "inc_value").fetchval()
#         self.assertEqual(data["data"]["params"]["args"][0], str_value_after)
#         self.assertEqual(inc_value_before + 1, inc_value_after)
#
#         self.gateway.send_rpc_reply.assert_has_calls([call(data["device"],
#                                                            data["data"]["id"],
#                                                            {data["data"]["method"]: 1})])
#
#     @unittest.skipIf(not IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED,
#                      "To run RPC ODBC tests install " + ODBC_DRIVER_WITH_STORED_PROCEDURE +
#                      " database and add it to PATH")
#     def test_unknown_rpc_enabled_with_query_and_result(self):
#         self._create_connector("odbc_rpc.json", unknown_rpc=True)
#         inc_value_before = self.db_cursor.execute("SELECT get_integer_value(?)", "inc_value").fetchval()
#
#         data = {"device": "someDevice",
#                 "data": {
#                     "id": 1,
#                     "method": "secret_function",
#                     "params": {
#                         "args": ["secret_string"],
#                         "query": "SELECT secret_function(?) as return_value",
#                         "result": True
#                     }
#                 }}
#         self.connector.server_side_rpc_handler(data)
#         sleep(1)
#
#         # Cursor from test connection doesn't see changes made through connector's cursor,
#         # so use latter to get up to date data
#         str_value_after = self.connector._OdbcConnector__rpc_cursor.execute("SELECT get_string_value(?)",
#                                                                             "str_value").fetchval()
#         inc_value_after = self.connector._OdbcConnector__rpc_cursor.execute("SELECT get_integer_value(?)",
#                                                                             "inc_value").fetchval()
#         self.assertEqual(data["data"]["params"]["args"][0], str_value_after)
#         self.assertEqual(inc_value_before + 1, inc_value_after)
#
#         self.gateway.send_rpc_reply.assert_has_calls([call(data["device"],
#                                                            data["data"]["id"],
#                                                            {"return_value": 1})])
#
#     @unittest.skipIf(not IS_ODBC_DRIVER_WITH_STORED_PROCEDURE_INSTALLED,
#                      "To run RPC ODBC tests install " + ODBC_DRIVER_WITH_STORED_PROCEDURE +
#                      " database and add it to PATH")
#     def test_rpc_multiple_field_response(self):
#         self._create_connector("odbc_rpc.json", override_rpc_params=True)
#         row = self.db_cursor.execute("{CALL get_values}").fetchone()
#         expected_values = {}
#         for d in self.db_cursor.description:
#             expected_values[d[0]] = getattr(row, d[0])
#
#         data = {"device": "someDevice",
#                 "data": {
#                     "id": 1,
#                     "method": "get_values",
#                     "params": {
#                         "result": True
#                     }
#                 }}
#         self.connector.server_side_rpc_handler(data)
#         sleep(1)
#
#         self.gateway.send_rpc_reply.assert_has_calls([call(data["device"],
#                                                            data["data"]["id"],
#                                                            expected_values)])
#
#     def test_rpc_no_connection(self):
#         self._create_connector("odbc_rpc.json", connection_str="fake_connection_string")
#         self.assertFalse(self.connector.is_connected())
#
#         data = {"device": "someDevice",
#                 "data": {
#                     "id": 1,
#                     "method": "get_values",
#                     "params": {
#                         "result": True
#                     }
#                 }}
#         self.connector.server_side_rpc_handler(data)
#         sleep(1)
#
#         self.gateway.send_rpc_reply.assert_has_calls([call(data["device"],
#                                                            data["data"]["id"],
#                                                            {"success": False})])
#
#     def test_reconnect_disabled(self):
#         self._create_connector("odbc_rpc.json", connection_str="fake_connection_string")
#
#         self.assertFalse(self.connector.is_connected())
#         sleep(self.config["connection"]["reconnectPeriod"] * 2 + 1)
#         self.assertFalse(self.connector.is_connected())
#
#     @unittest.skipIf(not IS_ODBC_DRIVER_INSTALLED,
#                      "To run ODBC tests install " + ODBC_DRIVER_WITH_STORED_PROCEDURE + "or" +
#                      ODBC_DRIVER_WITHOUT_STORED_PROCEDURE + " ODBC driver")
#     def test_reconnect_enabled(self):
#         self._create_connector("odbc_rpc.json", connection_str="fake_connection_string")
#
#         self.assertFalse(self.connector.is_connected())
#
#         self.config["connection"]["str"] = self.db_connection_str
#         self._init_test_db()
#         sleep(self.config["connection"]["reconnectPeriod"] + 1)
#
#         self.assertTrue(self.connector.is_connected())
#
#     def test_restore_connection(self):
#         self._create_connector("odbc_iterator.json")
#         postgres_port = self.postgresql.dsn()["port"]
#
#         device_id = 1  # For eval
#         data = {"deviceName": eval(self.config["mapping"]["device"]["name"]),
#                 "deviceType": self.connector._OdbcConnector__connector_type,
#                 "attributes": [],
#                 "telemetry": [{"value": 0}]}
#         self.gateway.send_to_storage.assert_has_calls([call(self.connector.get_name(), data)])
#
#         self.postgresql.stop()
#
#         sleep(self.config["polling"]["period"])
#         self.assertFalse(self.connector.is_connected())
#         sleep(self.config["connection"]["reconnectPeriod"])
#
#         self.postgresql = testing.postgresql.Postgresql(port=postgres_port)
#         self._init_test_db()
#
#         sleep(self.config["connection"]["reconnectPeriod"])
#
#         self.assertTrue(self.connector.is_connected())
#         data["telemetry"] = [{"value": 5}]
#         self.gateway.send_to_storage.assert_has_calls([call(self.connector.get_name(), data)])
#
#     def test_iterator_persistence(self):
#         self._create_connector("odbc_iterator.json")
#         iterator_file_name = self.connector._OdbcConnector__iterator_file_name
#
#         device_id = 1  # For eval
#         data = {"deviceName": eval(self.config["mapping"]["device"]["name"]),
#                 "deviceType": self.connector._OdbcConnector__connector_type,
#                 "attributes": [],
#                 "telemetry": [{"value": 0}]}
#
#         self.gateway.send_to_storage.assert_has_calls([call(self.connector.get_name(), data)])
#         self.connector.close()
#         sleep(1)
#
#         self.assertTrue(Path(self.CONFIG_PATH + "odbc" + path.sep + iterator_file_name).exists())
#
#         self.connector = OdbcConnector(self.gateway, self.config, "odbc")
#         self.connector.open()
#         sleep(1)
#
#         data["telemetry"] = [{"value": 5}]
#         self.gateway.send_to_storage.assert_has_calls([call(self.connector.get_name(), data)])
#
#
# if __name__ == '__main__':
#     unittest.main()
