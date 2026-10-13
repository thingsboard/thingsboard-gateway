#     Copyright 2026. ThingsBoard
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

from queue import Queue
from random import choice
from re import fullmatch
from string import ascii_lowercase
from threading import Thread
from time import sleep, time
from os import environ

from simplejson import loads
from thingsboard_gateway.connectors.request.constants import (
    RESERVED_GET_RPC_SCHEMA,
    RESERVED_SET_RPC_SCHEMA,
    RESERVED_GET_RPC_PATTERN,
    RESERVED_SET_RPC_PATTERN
)
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.rpc_request import RPCType
from thingsboard_gateway.gateway.entities.rpc_response import RPCResponse
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.connectors.request.entities.rpc_execution_result import RpcExecutionResult
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.tb_utility.tb_logger import init_logger

try:
    from requests import Timeout, request
except ImportError:
    print("Requests library not found - installing...")
    TBUtility.install_package("requests")
    from requests import Timeout, request
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException, JSONDecodeError

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.connectors.request.json_request_uplink_converter import JsonRequestUplinkConverter
from thingsboard_gateway.connectors.request.json_request_downlink_converter import JsonRequestDownlinkConverter


class RequestConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.__rpc_requests = []
        self.__config = config
        self.__id = self.__config.get('id')
        self._connector_type = connector_type
        self.__gateway = gateway
        self.name = self.__config.get("name", "".join(choice(ascii_lowercase) for _ in range(5)))
        self._log = init_logger(self.__gateway, self.name, self.__config.get('logLevel', 'INFO'),
                                enable_remote_logging=self.__config.get('enableRemoteLogging', False),
                                is_connector_logger=True)
        self._converter_log = init_logger(self.__gateway, self.name + '_converter',
                                          self.__config.get('logLevel', 'INFO'),
                                          enable_remote_logging=self.__config.get('enableRemoteLogging', False),
                                          is_converter_logger=True, attr_name=self.name)

        security_cfg = self.__config.get("security", {})
        security_type = security_cfg.get("type")
        self.__security = None
        if security_type == "basic":
            self.__security = self._get_basic_auth(security_cfg)

        self.__host = None
        self.__service_headers = {}
        if "http://" in self.__config["host"].lower() or "https://" in self.__config["host"].lower():
            self.__host = self.__config["host"]
        else:
            self.__host = "http://" + self.__config["host"]
        self.__ssl_verify = self.__config.get("SSLVerify", False)
        self.daemon = True
        self.__connected = False
        self.__stopped = False
        self.__requests_in_progress = []
        self.__convert_queue = Queue(1000000)
        self.__attribute_updates = []
        self.__fill_attribute_updates()
        self.__fill_rpc_requests()
        self.__fill_requests()

    def _get_basic_auth(self, security_config):
        username = environ.get("REQUEST_BASIC_USERNAME")
        password = environ.get("REQUEST_BASIC_PASSWORD")

        if username is None or password is None:
            username = security_config.get("username")
            password = security_config.get("password")

            if username is None or password is None:
                self._log.warning("Basic authentication requires username and password to be set")
                self._log.warning("Will use anonymous authentication for requests")
                return None

        return HTTPBasicAuth(username, password)

    def run(self):
        while not self.__stopped:
            request_sent = False
            if self.__requests_in_progress:
                for req in self.__requests_in_progress:
                    if time() >= req["next_time"]:
                        thread = Thread(target=self.__send_request, args=(req, self.__convert_queue, self._log),
                                        daemon=True,
                                        name="Request to endpoint \'%s\' Thread" % (req["config"].get("url")))
                        thread.start()
                        request_sent = True
            if not request_sent:
                sleep(.2)
            self.__process_data()

    def on_attributes_update(self, content):
        try:
            for attribute_request in self.__attribute_updates:
                if fullmatch(attribute_request["deviceNameFilter"], content["device"]) and fullmatch(
                        attribute_request["attributeFilter"], list(content["data"].keys())[0]):
                    converted_data = attribute_request["converter"].convert(attribute_request, content)
                    response_queue = Queue(1)
                    request_dict = {"config": {**attribute_request,
                                               **converted_data},
                                    "request": request,
                                    "withResponse": True}
                    attribute_update_request_thread = Thread(target=self.__send_request,
                                                             args=(request_dict, response_queue, self._log),
                                                             daemon=True,
                                                             name="Attribute request to %s" % (converted_data["url"]))
                    attribute_update_request_thread.start()
                    attribute_update_request_thread.join()
                    if not response_queue.empty():
                        response = response_queue.get_nowait()
                        self._log.debug(response)
                    del response_queue
        except Exception as e:
            self._log.exception(e)

    def server_side_rpc_handler(self, rpc_request) -> RPCResponse:
        self._log.debug('Received RPC request: %s', rpc_request)

        try:
            if rpc_request.rpc_type == RPCType.DEVICE:
                return self._process_rpc_to_device(rpc_request)
            elif rpc_request.rpc_type == RPCType.RESERVED:
                return self._process_reserved_rpc(rpc_request)
            elif rpc_request.rpc_type == RPCType.CONNECTOR:
                return self._process_rpc_to_connector(rpc_request)
            else:
                return self.__rpc_error_response(rpc_request, f'Invalid RPC type request: {rpc_request}')
        except Exception as e:
            return self.__rpc_error_response(rpc_request, f'Error processing RPC request {rpc_request}: {e}')

    def __rpc_error_response(self, rpc_request, error_msg) -> RPCResponse:
        self._log.error(error_msg)
        response = RPCResponse(rpc_request.id, device=rpc_request.device_name)
        response.set_error_msg(error_msg)
        return response

    def _process_rpc_to_connector(self, rpc_request) -> RPCResponse:
        rpc_config = self.__build_rpc_config(rpc_request.params)
        return self._process_rpc(rpc_request, rpc_config)

    def _process_reserved_rpc(self, rpc_request) -> RPCResponse:
        if not rpc_request.params:
            return self.__rpc_error_response(
                rpc_request, f"No 'params' found in reserved RPC request '{rpc_request.method_name}'")

        is_set = rpc_request.method_name.lower() == 'set'
        pattern = RESERVED_SET_RPC_PATTERN if is_set else RESERVED_GET_RPC_PATTERN
        match = fullmatch(pattern, rpc_request.params)
        if match is None:
            expected_schema = RESERVED_SET_RPC_SCHEMA if is_set else RESERVED_GET_RPC_SCHEMA
            return self.__rpc_error_response(
                rpc_request, f"The requested RPC does not match with the schema: {expected_schema}")

        rpc_config = self.__build_rpc_config(self.__build_reserved_rpc_params(match.groupdict()))
        return self._process_rpc(rpc_request, rpc_config)

    def _process_rpc_to_device(self, rpc_request) -> RPCResponse:
        rpc_config = self.__find_rpc_config(rpc_request)
        if rpc_config is None:
            return self.__rpc_error_response(
                rpc_request, f'Neither of configured device rpc methods match with {rpc_request.method_name}')

        return self._process_rpc(rpc_request, rpc_config)

    def _process_rpc(self, rpc_request, rpc_config) -> RPCResponse:
        downlink_converter = rpc_config.get('converter') or JsonRequestDownlinkConverter(rpc_config,
                                                                                         self._converter_log)
        converted_data = downlink_converter.convert(rpc_config, self.__build_converter_data(rpc_request))
        if converted_data is None:
            return self.__rpc_error_response(rpc_request, f'Failed to convert RPC request {rpc_request}')

        result = self.__execute_rpc(rpc_config, converted_data)
        if not result.success:
            return self.__rpc_error_response(rpc_request, result.error)

        response = RPCResponse(rpc_request.id, device=rpc_request.device_name)
        response.set_message(result.response_body)
        return response

    def __find_rpc_config(self, rpc_request):
        for rpc_config in self.__rpc_requests:
            if (fullmatch(rpc_config["deviceNameFilter"], rpc_request.device_name)
                    and fullmatch(rpc_config["methodFilter"], rpc_request.method_name)):
                return rpc_config
        return None

    @staticmethod
    def __build_converter_data(rpc_request):
        return {'device': rpc_request.device_name or '',
                'data': {'id': rpc_request.id, 'method': rpc_request.method_name, 'params': rpc_request.params}}

    def __execute_rpc(self, rpc_request: dict, converted_data: dict) -> RpcExecutionResult:
        response_queue = Queue(1)
        request_dict = {"config": {**rpc_request,
                                   **converted_data},
                        "request": request,
                        "withResponse": True,
                        }
        send_request_thread = Thread(target=self.__send_request,
                                     args=(request_dict, response_queue, self._log),
                                     daemon=True,
                                     name="RPC request to %s Thread" % (converted_data.get("url")))
        try:
            send_request_thread.start()
            send_request_thread.join(timeout=rpc_request.get("responseTimeout", 5))

        except Exception as e:
            self._log.error("Error in executing RPC request: %s", e)
            del response_queue
            return RpcExecutionResult(success=False, error=f"Thread execution error: {e}")

        if send_request_thread.is_alive() and response_queue.empty():
            del response_queue
            return RpcExecutionResult(success=False, error="Rpc timeout")

        if response_queue.empty():
            del response_queue
            return RpcExecutionResult(success=False, error="No response received from RPC execution")

        response = response_queue.get_nowait()
        if not response.ok:
            return RpcExecutionResult(success=False, error=f"{response.reason} (code: {response.status_code})",
                                      status_code=response.status_code)

        response_expression = rpc_request.get('responseValueExpression')
        try:
            response_body = response.json()
            self._log.debug("RPC response body: %s", response_body)

        except Exception as e:
            self._log.debug("RPC response is not in JSON format: %s", e)
            response_body = response.text
            self._log.debug("RPC response body as text: %s", response_body)
            return RpcExecutionResult(success=True, response_body=response_body, status_code=response.status_code)

        if response_expression:
            try:
                values = TBUtility.get_values(response_expression, response_body, expression_instead_none=True)
                value_tags = TBUtility.get_values(response_expression, response_body, get_tag=True)
                if not values or not value_tags:
                    return RpcExecutionResult(
                        success=False,
                        error=f"No values found for responseValueExpression: {response_expression} in RPC response. Please check your configuration.")
                full_value = response_expression
                for (value, value_tag) in zip(values, value_tags):
                    is_template = "${" in response_expression and "}" in response_expression
                    full_value = full_value.replace('${' + str(value_tag) + '}', str(value)) if is_template else str(
                        value)
                return RpcExecutionResult(success=True, response_body=full_value, status_code=response.status_code)

            except Exception as e:
                del response_queue
                return RpcExecutionResult(success=False, error=f"Response parsing error {e}")

        return RpcExecutionResult(success=True, response_body=response_body, status_code=response.status_code)

    @staticmethod
    def __build_reserved_rpc_params(groups):
        params = {key: value for key, value in groups.items() if value is not None}
        params['requestValueExpression'] = params.pop('value', '')
        return params

    @staticmethod
    def __build_rpc_config(params):
        headers = params.get("httpHeaders", {"Content-Type": "application/json"})
        rpc_config = {
            "requestUrlExpression": params.get("requestUrlExpression"),
            "responseTimeout": params.get("responseTimeout", 1),
            "httpMethod": params.get("httpMethod", "GET"),
            "requestValueExpression": params.get("requestValueExpression", "${params}"),
            "responseValueExpression": params.get("responseValueExpression"),
            "timeout": params.get("timeout", 0.5),
            "tries": params.get("tries", 3),
            "httpHeaders": loads(headers) if isinstance(headers, str) else headers
        }

        if not rpc_config["requestUrlExpression"]:
            raise ValueError("requestUrlExpression parameter is required. Please check your RPC params.")

        return rpc_config

    def __fill_requests(self):
        self._log.debug(self.__config["mapping"])
        for endpoint in self.__config["mapping"]:
            try:
                self._log.debug(endpoint)
                converter = None
                if endpoint["converter"]["type"] == "custom":
                    module = TBModuleLoader.import_module(self._connector_type, endpoint["converter"]["extension"])
                    if module:
                        self._log.debug('Custom converter for url %s - found!', endpoint["url"])
                        converter = module(endpoint, self._converter_log)
                    else:
                        self._log.error(
                            "\n\nCannot find extension module for %s url.\nPlease check your configuration.\n",
                            endpoint["url"])
                else:
                    converter = JsonRequestUplinkConverter(endpoint, self._converter_log)
                self.__requests_in_progress.append({"config": endpoint,
                                                    "converter": converter,
                                                    "next_time": time(),
                                                    "request": request})
            except Exception as e:
                self._log.exception(e)

    def __fill_attribute_updates(self):
        for attribute_request in self.__config.get("attributeUpdates", []):
            if attribute_request.get("converter") is not None:
                converter = TBModuleLoader.import_module("request", attribute_request["converter"])(attribute_request,
                                                                                                    self._converter_log)
            else:
                converter = JsonRequestDownlinkConverter(attribute_request, self._converter_log)
            attribute_request_dict = {**attribute_request, "converter": converter}
            self.__attribute_updates.append(attribute_request_dict)

    def __fill_rpc_requests(self):
        for rpc_request in self.__config.get("serverSideRpc", []):
            if rpc_request.get("converter") is not None:
                converter = TBModuleLoader.import_module("request", rpc_request["converter"])(rpc_request, self._converter_log)
            else:
                converter = JsonRequestDownlinkConverter(rpc_request, self._converter_log)
            rpc_request_dict = {**rpc_request, "converter": converter}
            self.__rpc_requests.append(rpc_request_dict)

    def __send_request(self, request, converter_queue, logger):
        url = ""
        try:
            request["next_time"] = time() + request["config"].get("scanPeriod", 10)
            if request.get("converter") is None and isinstance(request["config"].get("converter"), dict):
                logger.error("Converter for request to '%s' endpoint is not defined. Request will be skipped.", request["config"].get("url"))
                return
            request_url_from_config = request["config"]["url"]
            request_url_from_config = (
                str("/" + request_url_from_config)
                if not request_url_from_config.startswith("/")
                   and not request_url_from_config.startswith("http")
                else request_url_from_config
            )
            logger.debug("Obtained request url from config - %s ", request_url_from_config)
            url, response = self.__execute_request(request, request_url_from_config, logger)

            if request.get('withResponse'):
                converter_queue.put(response)
                return
            if response and response.ok:
                if not converter_queue.full():
                    config_converter_data = [url, request["converter"]]
                    try:
                        json_response = response.json()

                        # Unpack data if dataUnpackExpression is defined in config
                        # This allows to unpack JSON responses that have final data at a sub key on any level
                        # {
                        #   "device": [...]
                        # }
                        data_unpack_expression = request["config"].get("dataUnpackExpression")
                        if data_unpack_expression:
                            json_response = TBUtility.get_value(data_unpack_expression, json_response, value_type="json")

                        config_converter_data.append(json_response)
                    except UnicodeDecodeError:
                        config_converter_data.append(response.content)
                    except JSONDecodeError:
                        config_converter_data.append(response.content)

                    if len(config_converter_data) == 3:
                        # Process sub requests if defined in config
                        if request["config"].get("subRequests"):
                            self.__process_sub_requests(request, url, config_converter_data[2], logger)
                        self.__convert_data(config_converter_data)
            else:
                logger.error("Request to URL: %s finished with code: %i", url, response.status_code)
        except Timeout:
            logger.error("Timeout error on request %s.", url)
        except RequestException as e:
            logger.error("Cannot connect to %s. Connection error.", url)
            logger.debug(e)
        except ConnectionError:
            logger.error("Cannot connect to %s. Connection error.", url)
        except Exception as e:
            logger.exception(e)

    def __execute_request(self, request, request_url, logger):
        url = self.__host + request_url if not request_url.lower().startswith("http") else request_url

        request_timeout = request["config"].get("timeout", 1)
        params = {
            "method": request["config"].get("httpMethod", "GET"),
            "url": url,
            "timeout": request_timeout,
            "allow_redirects": request["config"].get("allowRedirects", False),
            "verify": self.__ssl_verify,
            "auth": self.__security,
            "data": request["config"].get("data", {})
        }
        logger.debug("Full url request has been formed - %s", url)

        if request["config"].get("httpHeaders") is not None:
            params["headers"] = request["config"]["httpHeaders"]

        logger.debug("Request to %s will be sent", url)
        if isinstance(params["data"], str):
            params["data"] = params["data"].encode("utf-8")
        self._log.debug("Sending request to URL: %s with params %s", url, params)
        response = request["request"](**params)

        return url, response

    def __convert_data(self, data):
        try:
            url, converter, data = data
            data_to_send = []

            StatisticsService.count_connector_message(self.name, stat_parameter_name='connectorMsgsReceived')
            StatisticsService.count_connector_bytes(self.name, data, stat_parameter_name='connectorBytesReceived')

            if isinstance(data, list):
                for data_item in data:
                    self.__add_ts(data_item)
                    converted_data = converter.convert(url, data_item)
                    data_to_send.append(converted_data)
            else:
                self.__add_ts(data)
                data_to_send.append(converter.convert(url, data))

            for to_send in data_to_send:
                self.__convert_queue.put(to_send)

        except Exception as e:
            self._log.exception(e)

    def __add_ts(self, data):
        if isinstance(data, list):
            for item in data:
                self.__add_ts(item)
        elif isinstance(data, dict):
            if data.get("ts") is None:
                data["ts"] = int(time() * 1000)

    def __process_data(self):
        try:
            if not self.__convert_queue.empty():
                data: ConvertedData = self.__convert_queue.get()
                if data and (data.attributes_datapoints_count > 0 or data.telemetry_datapoints_count > 0):
                    self.__gateway.send_to_storage(self.get_name(), self.get_id(), data)

        except Exception as e:
            self._log.exception(e)

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.name

    def get_type(self):
        return self._connector_type

    def is_connected(self):
        return self.__connected

    def is_stopped(self):
        return self.__stopped

    def open(self):
        self.__stopped = False
        self.start()

    def close(self):
        self.__stopped = True
        self._log.info("%r has been stopped.", self.name)
        self._log.stop()

    def get_config(self):
        return self.__config

    def __process_sub_requests(self, request, url, data, logger):
        datatypes = {"attributes": "attributes",
                     "telemetry": "telemetry"}
        data = data if isinstance(data, list) else [data]

        for data_item in data:
            for datatype in datatypes:
                for datatype_object_config in request["config"]["converter"].get(datatype, []):
                    # Check if a sub request for key is needed
                    key = datatype_object_config.get("key")
                    if key in request["config"].get("subRequests", {}):
                        request_url_from_config = TBUtility.replace_params_tags(request["config"]["subRequests"][key]["url"],
                                                                                {"data": data_item})

                        if not request_url_from_config.lower().startswith("http"):
                            if not request_url_from_config.startswith("/"):
                                request_url_from_config = "/" + request_url_from_config
                            request_url_from_config = url + request_url_from_config
                        logger.debug("Sub request needed for key %s with url %s", key, request_url_from_config)

                        response = self.__send_sub_request(request, request_url_from_config, logger)
                        logger.debug("Sub request response: %s", response)

                        # Only if a response is available, process it
                        if response:
                            result = response
                            # Make processing function available if defined and call it
                            processing_function = request["config"]["subRequests"][key].get("processingFunction")
                            if processing_function:
                                logger.trace("Processing sub request response with function:\n%s", processing_function)
                                local_scope = {}
                                exec(processing_function, {}, local_scope)
                                result = local_scope["process_data"](response, key)
                            # Update data with result of sub request
                            data_item.update(result)
                            logger.debug("Data after sub request processing: %s", data_item)

    def __send_sub_request(self, request, sub_request_url, logger):
        url = ""
        try:
            url, response = self.__execute_request(request, sub_request_url, logger)
            if response and response.ok:
                try:
                    return response.json()
                except UnicodeDecodeError:
                    return response.content
                except JSONDecodeError:
                    return response.content
            else:
                logger.error("Request to URL: %s finished with code: %i", url, response.status_code)
        except Timeout:
            logger.error("Timeout error on request %s.", url)
        except RequestException as e:
            logger.error("Cannot connect to %s. Connection error.", url)
            logger.debug(e)
        except ConnectionError:
            logger.error("Cannot connect to %s. Connection error.", url)
        except Exception as e:
            logger.exception(e)
