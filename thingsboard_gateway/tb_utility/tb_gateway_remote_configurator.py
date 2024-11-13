#     Copyright 2024. ThingsBoard
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
import inspect
import os.path
from copy import deepcopy
from logging import getLogger, setLoggerClass
from logging.config import dictConfig
from queue import Queue
from threading import Thread
from time import sleep, time, monotonic

from regex import fullmatch
from simplejson import dumps, load

from tb_gateway_mqtt import TBGatewayMqttClient

from thingsboard_gateway.gateway.constants import CONFIG_VERSION_PARAMETER, REPORT_STRATEGY_PARAMETER, \
    DEFAULT_REPORT_STRATEGY_CONFIG
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.tb_client import TBClient
from thingsboard_gateway.tb_utility.tb_logger import TbLogger
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

setLoggerClass(TbLogger)
LOG = getLogger("service")


class RemoteConfigurator:
    ALLOWED_BACKUPS_AMOUNT = 10
    RECEIVED_UPDATE_QUEUE = Queue()

    DEFAULT_STATISTICS = {
        'enable': True,
        'statsSendPeriodInSeconds': 3600
    }

    def __init__(self, gateway, config):
        self._request_queue = Queue()
        self.in_process = False
        self._gateway = gateway
        self._config = config
        self._load_connectors_configuration()
        self._logs_configuration = self._load_logs_configuration()
        self._active_connectors = []
        self._handlers = {
            'storage_configuration': self._handle_storage_configuration_update,
            'grpc_configuration': self._handle_grpc_configuration_update,
            'logs_configuration': self._handle_logs_configuration_update,
            'active_connectors': self._handle_active_connectors_update,
            'RemoteLoggingLevel': self._handle_remote_logging_level_update,
            r'(?=\D*\d?).*': self._handle_connector_configuration_update,
        }
        self._modifiable_static_attrs = {
            'logs_configuration': 'logs.json'
        }
        self._max_backup_files_number = 10

        # creating backups (general and connectors)
        self.create_configuration_file_backup(self._get_general_config_in_local_format(), "tb_gateway.json")
        self._create_connectors_backup()

        self._requests_processing_thread = Thread(name='Remote Request Processing', target=self._process_config_request,
                                                  daemon=True)
        self._requests_processing_thread.start()

        LOG.info('Remote Configurator started')

    @property
    def general_configuration(self):
        return self._config.get('thingsboard', {})

    @general_configuration.setter
    def general_configuration(self, config):
        self._config['thingsboard'].update(config)

    @property
    def storage_configuration(self):
        return self._config.get('storage', {})

    @storage_configuration.setter
    def storage_configuration(self, config):
        self._config['storage'].update(config)

    @property
    def grpc_configuration(self):
        return self._config.get('grpc', {})

    @grpc_configuration.setter
    def grpc_configuration(self, config):
        self._config.get('grpc', {}).update(config)

    @property
    def connectors_configuration(self):
        connectors = self._config.get('connectors', [])
        for connector in connectors:
            connector.pop('config_updated', None)
            connector.pop('config_file_path', None)
        return connectors

    def _get_active_connectors(self):
        return [connector['name'] for connector in self.connectors_configuration]

    def _get_general_config_in_local_format(self):
        """
        Method returns general configuration in format that should be used only for local files
        !!!Don't use it for sending data to TB (use `_get_general_config_in_remote_format` instead)!!!
        """
        connectors_config = []
        for connector in self.connectors_configuration:
            config = {
                'type': connector['type'],
                'name': connector['name'],
                'configuration': connector['configuration']
            }
            if connector.get('class') is not None:
                config['class'] = connector.get('class')
            connectors_config.append(config)

        adopted_general_config = {}
        adopted_general_config.update(self.general_configuration)

        security_section = adopted_general_config.get('security')
        security_type = security_section.get('type', 'accessToken')
        if security_type == 'accessToken':
            security_section.pop('clientId', None)
            security_section.pop('username', None)
            security_section.pop('password', None)
        elif security_type == 'usernamePassword':
            security_section.pop('accessToken', None)

        return {
            'thingsboard': adopted_general_config,
            'storage': self.storage_configuration,
            'grpc': self.grpc_configuration,
            'connectors': connectors_config
        }

    def _get_general_config_in_remote_format(self):
        """
        Method returns general configuration in format that should be used only for sending configuration to the server.
        !!!Don't use it for saving data to conf files (use `_get_general_config_in_local_format`)!!!
        """

        stat_conf_path = self.general_configuration['statistics'].get('configuration')
        commands = []
        if stat_conf_path:
            with open(self._gateway.get_config_path() + stat_conf_path, 'r') as file:
                commands = load(file)
        config = self.general_configuration
        config.update(
            {
                'statistics': {
                    'enable': self.general_configuration['statistics']['enable'],
                    'statsSendPeriodInSeconds': self.general_configuration['statistics']['statsSendPeriodInSeconds'],
                    'configuration': self.general_configuration['statistics'].get('configuration'),
                    'commands': commands
                }
            }
        )
        return config

    def send_current_configuration(self):
        """
        Calling manually only on RemoteConfigurator init (TbGatewayService.__init_remote_configuration method)
        When Gateway started, sending all configs for states synchronizing
        """

        LOG.debug('Sending all configurations (init)')
        ts = int(time() * 1000)
        init_config_message = {
            'general_configuration': {**self._get_general_config_in_remote_format(), 'ts': ts},
            'storage_configuration': {**self.storage_configuration, 'ts': ts},
            'grpc_configuration': {**self.grpc_configuration, 'ts': ts},
            'logs_configuration': {**self._logs_configuration, 'ts': ts},
            'active_connectors': self._get_active_connectors(),
            'Version': self._gateway.version.get('current_version', '0.0')
        }
        self._gateway.send_attributes(init_config_message)

        # sending remote created connectors
        for connector in self.connectors_configuration:
            self.send_connector_current_configuration(connector)

    def send_connector_current_configuration(self, connector_configuration: dict):
        config_to_send = {**connector_configuration,
                          'logLevel': connector_configuration.get('configurationJson', {}).get('logLevel', 'INFO'),
                          'ts': int(time() * 1000)
                          }
        if config_to_send.get('config') is not None:
            config_to_send.pop('config', None)
        if config_to_send.get("configurationJson", {}) is not None:
            config_to_send.get("configurationJson", {}).pop('logLevel', None)
        if config_to_send.get('configurationJson', {}).get(REPORT_STRATEGY_PARAMETER) is not None:
            config_to_send[REPORT_STRATEGY_PARAMETER] = config_to_send['configurationJson'].pop(REPORT_STRATEGY_PARAMETER)
        if config_to_send.get('configurationJson', {}).get(CONFIG_VERSION_PARAMETER) is not None:
            config_to_send[CONFIG_VERSION_PARAMETER] = config_to_send['configurationJson'].pop(CONFIG_VERSION_PARAMETER)
        self._gateway.send_attributes({connector_configuration['name']: config_to_send}, quality_of_service=1)

    def _load_connectors_configuration(self):
        for (_, connector_list) in self._gateway.connectors_configs.items():
            for connector in connector_list:
                for general_connector_config in self.connectors_configuration:
                    if general_connector_config['name'] == connector['name']:
                        config = connector.get('config')[general_connector_config['configuration']]
                        general_connector_config.update(connector)
                        general_connector_config['configurationJson'] = config

    def _load_logs_configuration(self):
        """
        Calling only on RemoteConfigurator init (__init__ method)
        """

        try:
            with open(self._gateway.get_config_path() + 'logs.json', 'r') as logs:
                return load(logs)
        except Exception:
            LOG.warning("Cannot open logs configuration file. Using default logs configuration.")
            return {}

    def _process_config_request(self):
        while not self._gateway.stopped:
            if not RemoteConfigurator.RECEIVED_UPDATE_QUEUE.empty():
                self.in_process = True
                config = RemoteConfigurator.RECEIVED_UPDATE_QUEUE.get()
                LOG.info('Configuration update request received.')
                LOG.debug('Got config update request: %s', config)

                try:
                    if 'general_configuration' in config.keys():
                        self._handle_general_configuration_update(config['general_configuration'])
                        config.pop('general_configuration', None)

                    for attr_name in config.keys():
                        if 'deleted' in attr_name:
                            continue

                        request_config = config[attr_name]
                        if not self._is_modified(attr_name, request_config) and self.__is_running(request_config):
                            continue

                        request_processed = False
                        for (name, func) in self._handlers.items():
                            if fullmatch(name, attr_name):
                                func(request_config)
                                request_processed = True
                                break
                        if not request_processed:
                            LOG.error("Cannot process request for %s", attr_name)
                except (KeyError, AttributeError) as e:
                    LOG.error('Unknown attribute update name (Available: %s), %r', list(self._handlers.keys()),e)

                self.in_process = False
            else:
                sleep(.2)

    # HANDLERS ---------------------------------------------------------------------------------------------------------
    def _handle_general_configuration_update(self, config):
        """
        General configuration update handling in 5 steps:
        1. Checking if connection changed (host, port, QoS, security or provisioning sections);
        2. Checking if statistics collecting changed;
        3. Checking if device filtering changed;
        4. Checking if Remote Shell on/off state changed;
        5. Updating other params (regardless of whether they have changed):
            a. maxPayloadSizeBytes;
            b. minPackSendDelayMS;
            c. minPackSizeToSend;
            d. checkConnectorsConfigurationInSeconds;
            f. handleDeviceRenaming.

        If config from steps 1-4 changed:
        True -> applying new config with related objects creating (old objects will remove).
        """

        LOG.debug('Processing general configuration update')

        LOG.debug('--- Checking connection configuration changes...')
        security_match = self.__is_security_match(self.general_configuration.get('security'),
                                                     config.get('security', {}))

        if (config['host'] != self.general_configuration['host']
                or config['port'] != self.general_configuration['port']
                or not security_match
                or config.get('provisioning', {}) != self.general_configuration.get('provisioning', {})
                or config['qos'] != self.general_configuration['qos']):
            LOG.debug('---- Connection configuration changed. Processing...')
            success = self._apply_connection_config(config)
            if self._gateway.stopped:
                return
            if not success:
                config.update(self.general_configuration)
        else:
            LOG.debug('--- Connection configuration not changed.')

        LOG.debug('--- Checking statistics configuration changes...')
        changed = self._check_statistics_configuration_changes(config.get('statistics', self.DEFAULT_STATISTICS))
        if changed:
            LOG.debug('---- Statistics configuration changed. Processing...')
            success = self._apply_statistics_config(config.get('statistics', self.DEFAULT_STATISTICS))
            if not success:
                config['statistics'].update(self.general_configuration['statistics'])
        else:
            LOG.debug('--- Statistics configuration not changed.')

        LOG.debug('--- Checking device filtering configuration changes...')
        if config.get('deviceFiltering') != self.general_configuration.get('deviceFiltering'):
            LOG.debug('---- Device filtering configuration changed. Processing...')
            success = self._apply_device_filtering_config(config)
            if not success:
                config['deviceFiltering'].update(self.general_configuration['deviceFiltering'])
        else:
            LOG.debug('--- Device filtering configuration not changed.')

        LOG.debug('--- Checking Remote Shell configuration changes...')
        if config.get('remoteShell') != self.general_configuration.get('remoteShell'):
            LOG.debug('---- Remote Shell configuration changed. Processing...')
            success = self._apply_remote_shell_config(config)
            if not success:
                config['remoteShell'].update(self.general_configuration['remoteShell'])
        else:
            LOG.debug('--- Remote Shell configuration not changed.')

        LOG.debug('--- Checking other configuration parameters changes...')

        LOG.debug('--- Checking Report Strategy configuration changes...')
        if config.get(REPORT_STRATEGY_PARAMETER) != self.general_configuration.get(REPORT_STRATEGY_PARAMETER):
            LOG.debug('---- Report Strategy configuration changed. Processing...')
            success = self._apply_report_strategy_config(config)
            if not success:
                config[REPORT_STRATEGY_PARAMETER].update(self.general_configuration[REPORT_STRATEGY_PARAMETER])
        else:
            LOG.debug('--- Report Strategy configuration not changed.')

        LOG.debug('--- Checking other configuration parameters changes...')
        self._apply_other_params_config(config)

        LOG.debug('--- Saving new general configuration...')
        self.general_configuration = config
        self._gateway.send_attributes({'general_configuration': self.general_configuration})
        self._cleanup()
        with open(self._gateway.get_config_path() + "tb_gateway.json", "w",  encoding="UTF-8") as file:
            file.writelines(dumps(self._get_general_config_in_local_format(), indent='  '))

    def _handle_storage_configuration_update(self, config):
        LOG.debug('Processing storage configuration update...')

        old_event_storage = self._gateway._event_storage
        try:
            storage_class = self._gateway.event_storage_types[config["type"]]
            self._gateway._event_storage.stop()
            storage_logger = self._gateway.remote_handler.get_logger('storage')
            self._gateway._event_storage = storage_class(config, storage_logger)
        except Exception as e:
            LOG.error('Something went wrong with applying the new storage configuration. Reverting...')
            LOG.exception(e)
            self._gateway._event_storage = old_event_storage
        else:
            self.storage_configuration = config
            with open(self._gateway.get_config_path() + "tb_gateway.json", "w", encoding="UTF-8") as file:
                file.writelines(dumps(self._get_general_config_in_local_format(), indent='  '))
            self._gateway.send_attributes({'storage_configuration': self.storage_configuration})

            LOG.debug('Processed storage configuration update successfully')

    def _handle_grpc_configuration_update(self, config):
        LOG.debug('Processing GRPC configuration update...')
        if config.get('enabled', False):
            try:
                self._gateway.init_grpc_service(config)
                for connector_name in self._gateway.available_connectors_by_name:
                    self._gateway.available_connectors_by_name[connector_name].close()
                self._gateway.load_connectors(self._get_general_config_in_local_format())
                self._gateway.connect_with_connectors()
            except Exception as e:
                LOG.error('Something went wrong with applying the new GRPC configuration. Reverting...')
                LOG.exception(e)
                self._gateway.init_grpc_service(self.grpc_configuration)
                for connector_name in self._gateway.available_connectors_by_name:
                    self._gateway.available_connectors_by_name[connector_name].close()
                self._gateway.load_connectors(self._get_general_config_in_local_format())
                self._gateway.connect_with_connectors()
            else:
                if 'keepaliveTimeMs' in self.grpc_configuration:
                    self.grpc_configuration['keepAliveTimeMs'] = self.grpc_configuration.pop('keepaliveTimeMs')
                if 'keepaliveTimeoutMs' in self.grpc_configuration:
                    self.grpc_configuration['keepAliveTimeoutMs'] = self.grpc_configuration.pop('keepaliveTimeoutMs')
                if 'keepalivePermitWithoutCalls' in self.grpc_configuration:
                    self.grpc_configuration['keepAlivePermitWithoutCalls'] = self.grpc_configuration.pop(
                        'keepalivePermitWithoutCalls')
                self.grpc_configuration = config
                with open(self._gateway.get_config_path() + "tb_gateway.json", "w", encoding="UTF-8") as file:
                    file.writelines(dumps(self._get_general_config_in_local_format(), indent='  '))
                self._gateway.send_attributes({'grpc_configuration': self.grpc_configuration})

                LOG.debug('Processed GRPC configuration update successfully')

    def _handle_logs_configuration_update(self, config):
        global LOG
        LOG.debug('Processing logs configuration update...')
        try:
            LOG = getLogger('service')
            logs_conf_file_path = self._gateway.get_config_path() + 'logs.json'
            target_handlers = {}
            original_in_config = deepcopy(config)

            for handler in config['handlers']:
                filename = config['handlers'][handler].get('filename', None)

                if "consoleHandler" == handler or (filename is not None and os.path.exists(filename)):
                    target_handlers[handler] = config['handlers'][handler]
                elif filename is not None:
                    LOG.debug('Handler %s not found. Trying to create...', handler)
                    try:
                        with open(filename, 'w'):
                            pass

                        target_handlers[handler] = config['handlers'][handler]
                    except Exception as e:
                        LOG.error('Cannot create handler %s with %s error. Skipping...', handler, e)
                        self._delete_handler(config, handler)

                    LOG.debug('Handler %s created.', handler)
                else:
                    LOG.warning('Config is invalid. Filename is empty in handler %s', handler)
                    self._delete_handler(config, handler)

            config['handlers'] = target_handlers

            for logger in config['loggers']:
                if config['loggers'][logger].get('level', "").lower() == 'none':
                    config['loggers'][logger]['level'] = 'NOTSET'

            dictConfig(config)

            with open(logs_conf_file_path, 'w') as logs:
                logs.write(dumps(original_in_config, indent='  '))

            self._gateway.update_loggers()

            LOG.debug("Logs configuration has been updated.")
            self._gateway.send_attributes({'logs_configuration': config})
        except Exception as e:
            LOG.error("Remote logging configuration is wrong, cannot apply it!")
            LOG.exception(e)

    def _handle_active_connectors_update(self, config):
        LOG.debug('Processing active connectors configuration update...')

        for connector_name in config:
            self._gateway._check_shared_attributes(shared_keys=[connector_name])

        has_changed = False
        for_deletion = []
        for_deletion_ids = []
        for active_connector_name in self._gateway.available_connectors_by_name:
            if active_connector_name not in config:
                try:
                    self._gateway.available_connectors_by_name[active_connector_name].close()
                    connector_id = self._gateway.available_connectors_by_name[active_connector_name].get_id()
                    self._gateway.available_connectors_by_id.pop(connector_id)
                    for_deletion.append(active_connector_name)
                    for_deletion_ids.append(connector_id)
                    self._gateway._report_strategy_service.delete_all_records_for_connector_by_connector_id_and_connector_name(connector_id, active_connector_name)
                    has_changed = True
                except Exception as e:
                    LOG.exception("Exception on removing connector occurred:", exc_info=e)

        if has_changed:
            for name in for_deletion:
                self._gateway.available_connectors_by_name.pop(name, None)
            for connector_id in for_deletion_ids:
                self._gateway.available_connectors_by_id.pop(connector_id, None)

            self._delete_connectors_from_config(config)
            with open(self._gateway.get_config_path() + 'tb_gateway.json', 'w') as file:
                file.writelines(dumps(self._get_general_config_in_local_format(), indent='  '))
            self._active_connectors = config

        self._gateway.send_attributes({'active_connectors': config})

    def _handle_connector_configuration_update(self, config):
        if not isinstance(config, dict):
            return
        """
        Expected the following data structure:
        {
            "name": "Mqtt Broker Connector",
            "id": "bee7caf1-fd37-4026-8782-d480915d4d1a"
            "type": "mqtt",
            "configuration": "mqtt.json",
            "enableRemoteLogging": false,
            "logLevel": "INFO",
            "key?type=>grpc": "auto",
            "class?type=>custom": "",
            "configurationJson": {
                ...
            }
        }
        """

        LOG.debug('Processing connectors configuration update...')

        try:
            connector_name = config['name']
            connector_type = config['type']
            if config.get('configuration') is None:
                config_file_name = connector_name.replace(' ', '_').lower() + '.json'
                config['configuration'] = config_file_name
            else:
                config_file_name = config['configuration']

            identifier_parameter = 'id' if config.get('configurationJson', {}).get('id') else 'name'
            found_connectors = []
            for connector in self.connectors_configuration:
                connector_identifier = connector.get(identifier_parameter)
                if connector_identifier is None:
                    LOG.warning('Connector %s has no identifier parameter %s, it will be skipped.',
                                connector, identifier_parameter)
                    continue
                if connector[identifier_parameter] == config.get('configurationJson', {}).get(identifier_parameter) or \
                        connector[identifier_parameter] == config.get(identifier_parameter):
                    found_connectors.append(connector)

            if (config.get('configurationJson', {})
                    and config.get('configurationJson', {}).get('id') is None
                    and len(found_connectors) > 0
                    and found_connectors[0].get('configurationJson') is not None
                    and found_connectors[0].get('configurationJson', {}).get('id') is not None):
                connector_id = TBUtility.get_or_create_connector_id(found_connectors[0].get("configurationJson"))
            else:
                connector_id = TBUtility.get_or_create_connector_id(config.get('configurationJson'))

            if not found_connectors:
                connector_configuration = {'name': connector_name,
                                           'type': connector_type,
                                           'id': connector_id,
                                           'enableRemoteLogging': config.get('enableRemoteLogging', False),
                                           'configuration': config_file_name,
                                           CONFIG_VERSION_PARAMETER: config.get(CONFIG_VERSION_PARAMETER)}

                if config.get(REPORT_STRATEGY_PARAMETER):
                    connector_configuration[REPORT_STRATEGY_PARAMETER] = config[REPORT_STRATEGY_PARAMETER]

                if config.get('key'):
                    connector_configuration['key'] = config['key']

                if config.get('class'):
                    connector_configuration['class'] = config['class']

                locally_formatted_connector_config = {'logLevel': config['logLevel'],
                                                        'name': connector_name,
                                                        'enableRemoteLogging': config.get('enableRemoteLogging', False),
                                                        'id': connector_id}
                if config.get(REPORT_STRATEGY_PARAMETER) is not None:
                    locally_formatted_connector_config[REPORT_STRATEGY_PARAMETER] = config[REPORT_STRATEGY_PARAMETER]

                if config.get(CONFIG_VERSION_PARAMETER) is not None:
                    locally_formatted_connector_config[CONFIG_VERSION_PARAMETER] = config[CONFIG_VERSION_PARAMETER]

                with open(self._gateway.get_config_path() + config_file_name, 'w') as file:
                    config['configurationJson'].update(locally_formatted_connector_config)
                    self.create_configuration_file_backup(config, config_file_name)
                    file.writelines(dumps(config['configurationJson'], indent='  '))

                self.connectors_configuration.append(connector_configuration)
                with open(self._gateway.get_config_path() + 'tb_gateway.json', 'w') as file:
                    file.writelines(dumps(self._get_general_config_in_local_format(), indent='  '))

                self._gateway.load_connectors(self._get_general_config_in_local_format())
                self._gateway.connect_with_connectors()
            else:
                found_connector = found_connectors[0]
                changed = False

                config_file_path = self._gateway.get_config_path() + config_file_name
                if os.path.exists(config_file_path):
                    with open(config_file_path, 'r') as file:
                        connector_config_data = load(file)
                        config_hash = hash(str(connector_config_data))

                    if config_hash != hash(str(config['configurationJson'])):
                        self.create_configuration_file_backup(connector_config_data, config_file_name)
                        changed = True

                connector_configuration = None
                if (found_connector.get('id') != connector_id
                        or found_connector.get('name') != connector_name
                        or found_connector.get('type') != connector_type
                        or found_connector.get('class') != config.get('class')
                        or found_connector.get('key') != config.get('key')
                        or found_connector.get('configurationJson', {}).get('logLevel') != config.get('logLevel')
                        or found_connector.get('enableRemoteLogging', False) != config.get('enableRemoteLogging',
                                                                                           False)):
                    changed = True
                    connector_configuration = {'name': connector_name,
                                               'type': connector_type,
                                               'id': connector_id,
                                               'enableRemoteLogging': config.get('enableRemoteLogging', False),
                                               'configuration': config_file_name,
                                               CONFIG_VERSION_PARAMETER: config.get(CONFIG_VERSION_PARAMETER),
                                               }
                    if config.get(REPORT_STRATEGY_PARAMETER):
                        connector_configuration[REPORT_STRATEGY_PARAMETER] = config[REPORT_STRATEGY_PARAMETER]

                    if config.get('key'):
                        connector_configuration['key'] = config['key']

                    if config.get('class'):
                        connector_configuration['class'] = config['class']

                    found_connector.update(connector_configuration)

                if changed:
                    with open(self._gateway.get_config_path() + config_file_name, 'w') as file:
                        config_json_update = {'logLevel': config['logLevel'],
                                      'name': connector_name,
                                      'enableRemoteLogging': config.get('enableRemoteLogging',
                                                                        False),
                                      'id': connector_id,
                                      CONFIG_VERSION_PARAMETER:
                                          config.get(CONFIG_VERSION_PARAMETER)}
                        if config.get(REPORT_STRATEGY_PARAMETER):
                            config_json_update[REPORT_STRATEGY_PARAMETER] = config[REPORT_STRATEGY_PARAMETER]
                        config['configurationJson'].update(config_json_update)
                        file.writelines(dumps(config['configurationJson'], indent='  '))

                    if connector_configuration is None:
                        connector_configuration = found_connector

                    self._gateway._report_strategy_service.delete_all_records_for_connector_by_connector_id_and_connector_name(connector_id, connector_name)

                    if connector_configuration.get('id') in self._gateway.available_connectors_by_id:
                        try:
                            close_start = monotonic()
                            while not self._gateway.available_connectors_by_id[connector_configuration['id']].is_stopped():
                                self._gateway.available_connectors_by_id[connector_configuration['id']].close()
                                if monotonic() - close_start > 5:
                                    LOG.error('Connector %s not stopped in 5 seconds', connector_configuration['id'])
                                    break
                        except Exception as e:
                            LOG.exception("Exception on closing connector occurred:", exc_info=e)
                    elif connector_configuration.get('name') in self._gateway.available_connectors_by_name:
                        try:
                            close_start = monotonic()
                            while not self._gateway.available_connectors_by_name[connector_configuration['name']].is_stopped():
                                self._gateway.available_connectors_by_name[connector_configuration['name']].close()
                                if monotonic() - close_start > 5:
                                    LOG.error('Connector %s not stopped in 5 seconds', connector_configuration['name'])
                                    break
                        except Exception as e:
                            LOG.exception("Exception on closing connector occurred:", exc_info=e)
                    else:
                        LOG.warning('Connector with id %s not found in available connectors', connector_configuration.get('id'))
                    if connector_configuration.get('id') in self._gateway.available_connectors_by_id:
                        self._gateway.available_connectors_by_id.pop(connector_configuration['id'])
                        connector_configuration['id'] = connector_id
                    elif connector_configuration.get('name') in self._gateway.available_connectors_by_name:
                        self._gateway.available_connectors_by_name.pop(connector_configuration['name'])
                        connector_configuration['id'] = connector_id
                    else:
                        LOG.warning('Connector with id %s not found in available connectors', connector_configuration.get('id'))

                    self._gateway.load_connectors(self._get_general_config_in_local_format())
                    self._gateway.connect_with_connectors()

            # can be removed in the future versions:
            config['sendDataOnlyOnChange'] = config['configurationJson'].get('sendDataOnlyOnChange', False)

            if connector_type != 'modbus':
                config['configurationJson'].pop('reportStrategy', None)

            for (device_name, device_config) in list(self._gateway.get_devices().items()):
                if (connector_id == device_config.get('connector').get_id()
                        and self._gateway.available_connectors_by_id.get(connector_id) is not None):
                    self._gateway.update_device(device_name,
                                                "connector",
                                                self._gateway.available_connectors_by_id[connector_id])

            self._gateway.send_attributes({connector_name: config})
            with open(self._gateway.get_config_path() + 'tb_gateway.json', 'w') as file:
                file.writelines(dumps(self._get_general_config_in_local_format(), indent='  '))
        except Exception as e:
            LOG.exception("Exception on connector configuration update occurred:", exc_info=e)

    def _handle_remote_logging_level_update(self, config):
        self._gateway.send_attributes({'RemoteLoggingLevel': config})

    # HANDLERS SUPPORT METHODS -----------------------------------------------------------------------------------------
    def _apply_connection_config(self, config) -> bool:
        old_tb_client_config_path = self._gateway.tb_client.get_config_folder_path()
        old_tb_client_config = self._gateway.tb_client.config
        connection_logger = getLogger('tb_connection')
        try:
            self._gateway.tb_client.disconnect()
            self._gateway.tb_client.stop()

            while not self._gateway.stopped and self._gateway.tb_client.client.is_connected():
                sleep(1)

            apply_start = time()

            connection_state = False
            use_new_config = True

            config['rateLimits'] = old_tb_client_config.get('rateLimits', 'DEFAULT_TELEMETRY_RATE_LIMIT')
            config['dpRateLimits'] = old_tb_client_config.get('dpRateLimits', 'DEFAULT_TELEMETRY_DP_RATE_LIMIT')
            if 'messages_rate_limit' in inspect.signature(TBGatewayMqttClient.__init__).parameters:
                config['messagesRateLimits'] = old_tb_client_config.get('messagesRateLimits', 'DEFAULT_MESSAGES_RATE_LIMIT')
                config['deviceMessagesRateLimits'] = old_tb_client_config.get('deviceMessagesRateLimits', 'DEFAULT_MESSAGES_RATE_LIMIT')
                config['deviceRateLimits'] = old_tb_client_config.get('deviceRateLimits', 'DEFAULT_TELEMETRY_RATE_LIMIT')
                config['deviceDpRateLimits'] = old_tb_client_config.get('deviceDpRateLimits', 'DEFAULT_TELEMETRY_DP_RATE_LIMIT')

            previous_rate_limits = self._gateway.tb_client.get_rate_limits()

            while not self._gateway.stopped and not connection_state:
                self._gateway.__subscribed_to_rpc_topics = False
                new_tb_client = TBClient(config if use_new_config else old_tb_client_config, old_tb_client_config_path, connection_logger)
                new_tb_client.update_client_rate_limits_with_previous(previous_rate_limits)
                new_tb_client.connect()
                while not self._gateway.stopped and time() - apply_start <= 30 and not connection_state:
                    connection_state = new_tb_client.is_connected()
                    sleep(.1)

                if connection_state:
                    self._gateway.tb_client = new_tb_client
                    self._gateway.__subscribed_to_rpc_topics = False
                    self._gateway.subscribe_to_required_topics()

                    return True
                else:
                    new_tb_client.disconnect()
                    new_tb_client.stop()
                    while not self._gateway.stopped and new_tb_client.client.is_connected():
                        sleep(1)
                    apply_start = time() * 1000
                    use_new_config = not use_new_config
                if self._gateway.stopped:
                    return False
        except Exception as e:
            LOG.exception(e)
            self._revert_connection()
            return False

    def _revert_connection(self):
        try:
            LOG.warning("Remote general configuration will be restored.")
            self._gateway.tb_client.disconnect()
            self._gateway.tb_client.stop()
            connection_logger = getLogger('tb_connection')
            self._gateway.tb_client = TBClient(self.general_configuration, self._gateway.get_config_path(), connection_logger)
            self._gateway.tb_client.connect()
            self._gateway.subscribe_to_required_topics()
            LOG.debug("%s connection has been restored", str(self._gateway.tb_client.client))
        except Exception as e:
            LOG.exception("Exception on reverting configuration occurred:")
            LOG.exception(e)

    def _apply_statistics_config(self, config) -> bool:
        try:
            commands = config.get('commands', [])
            if commands:
                statistics_conf_file_name = self.general_configuration['statistics'].get('configuration',
                                                                                         'statistics.json')
                if statistics_conf_file_name is None:
                    statistics_conf_file_name = 'statistics.json'

                with open(self._gateway.get_config_path() + statistics_conf_file_name, 'w') as file:
                    file.writelines(dumps(commands, indent='  '))
                config['configuration'] = statistics_conf_file_name

            self._gateway.init_statistics_service(config)
            self.general_configuration['statistics'] = config
            return True
        except Exception as e:
            LOG.error('Something went wrong with applying the new statistics configuration. Reverting...')
            LOG.exception(e)
            self._gateway.init_statistics_service(
                self.general_configuration.get('statistics', {'enable': True, 'statsSendPeriodInSeconds': 3600}))
            return False

    def _apply_device_filtering_config(self, config):
        try:
            self._gateway.init_device_filtering(config.get('deviceFiltering', {'enable': False}))
            return True
        except Exception as e:
            LOG.error('Something went wrong with applying the new device filtering configuration. Reverting...')
            LOG.exception(e)
            self._gateway.init_device_filtering(
                self.general_configuration.get('deviceFiltering', {'enable': False}))
            return False

    def _apply_remote_shell_config(self, config):
        try:
            self._gateway.init_remote_shell(config.get('remoteShell'))
            return True
        except Exception as e:
            LOG.error('Something went wrong with applying the new Remote Shell configuration. Reverting...')
            LOG.exception(e)
            self._gateway.init_remote_shell(self.general_configuration.get('remoteShell'))
            return False

    def _apply_report_strategy_config(self, config):
        old_main_report_strategy = self._gateway._report_strategy_service.main_report_strategy
        try:
            new_main_report_strategy = ReportStrategyConfig(config.get(REPORT_STRATEGY_PARAMETER, DEFAULT_REPORT_STRATEGY_CONFIG))
            self._gateway._report_strategy_service.main_report_strategy = new_main_report_strategy
            self._gateway._report_strategy_service.clear_cache()
            return True
        except Exception as e:
            self._gateway._report_strategy_service.main_report_strategy = old_main_report_strategy
            LOG.error('Something went wrong with applying the new Report Strategy configuration. Reverting...')
            LOG.exception(e)
            return False

    def _apply_other_params_config(self, config):
        self._gateway.config['thingsboard'].update(config)

    def _delete_connectors_from_config(self, connector_list):
        self._config['connectors'] = list(
            filter(lambda connector: connector['name'] in connector_list, self.connectors_configuration))

    def _check_statistics_configuration_changes(self, config):
        general_statistics_config = self.general_configuration.get('statistics', self.DEFAULT_STATISTICS)
        if config['enable'] != general_statistics_config['enable'] or config['statsSendPeriodInSeconds'] != \
                general_statistics_config['statsSendPeriodInSeconds']:
            return True

        commands = []
        if general_statistics_config.get('configuration'):
            with open(self._gateway.get_config_path() + general_statistics_config['configuration'], 'r') as file:
                commands = load(file)

        if config.get('commands', []) != commands:
            return True

        return False

    def _cleanup(self):
        self.general_configuration['statistics'].pop('commands')

    def _is_modified(self, attr_name, config):
        try:
            file_path = config.get('configuration') or self._modifiable_static_attrs.get(attr_name)
        except AttributeError:
            file_path = None

        # if there is no file path that means that it is RemoteLoggingLevel or active_connectors attribute update
        # in this case, we have to update the configuration without TS compare
        if file_path is None:
            return True

        try:
            file_path = self._gateway.get_config_path() + file_path
            if config.get('ts', 0) <= int(os.path.getmtime(file_path) * 1000):
                return False
        except OSError:
            LOG.warning('File %s not exist', file_path)

        return True

    def _is_running(self, attr_name, config):
        if (config.get(attr_name, {}).get('configurationJson', {}).get('id') in self._gateway.available_connectors_by_id
                or attr_name in self._gateway.available_connectors_by_name):
            return True
        return False

    def create_configuration_file_backup(self, config_data, config_file_name):
        backup_folder_path = self._gateway.get_config_path() + "backup"
        if not os.path.exists(backup_folder_path):
            os.mkdir(backup_folder_path)

        backup_file_name = config_file_name.split('.')[0] + ".backup." + str(int(time())) + ".json"
        backup_file_path = backup_folder_path + os.path.sep + backup_file_name
        with open(backup_file_path, "w") as backup_file:
            LOG.debug(f"Backup file created for configuration file {config_file_name} in {backup_file_path}")
            backup_file.writelines(dumps(config_data, indent='  ', skipkeys=True))
        self.check_and_remove_old_backups_for_configuration_file(backup_folder_path, config_file_name)

    def _create_connectors_backup(self):
        for connector in self.connectors_configuration:
            if connector.get('configurationJson'):
                self.create_configuration_file_backup(connector['configurationJson'], connector['configuration'])
            else:
                LOG.debug(f"Configuration for {connector['name']} connector is not found, backup wasn't created")

    @staticmethod
    def check_and_remove_old_backups_for_configuration_file(backup_folder_path, config_file_name):
        backup_files = [f for f in os.listdir(backup_folder_path) if f.startswith(config_file_name.split('.')[0]) and f.endswith('.json')]
        if len(backup_files) > RemoteConfigurator.ALLOWED_BACKUPS_AMOUNT:
            backup_files.sort()
            for i in range(len(backup_files) - RemoteConfigurator.ALLOWED_BACKUPS_AMOUNT):
                os.remove(backup_folder_path + os.path.sep + backup_files[i])

    @staticmethod
    def _delete_handler(config, handler):
        for logger in config['loggers']:
            if handler in config['loggers'][logger]['handlers']:
                config['loggers'][logger]['handlers'].remove(handler)

    def __is_running(self, request_config):
        return (request_config.get('configurationJson', {}).get('id') in self._gateway.available_connectors_by_id or
                request_config.get('name') in self._gateway.available_connectors_by_name)

    @staticmethod
    def __is_security_match(old_security, new_security):
        if new_security.get('type'):
            is_match = RemoteConfigurator.__is_security_match_by_type(old_security, new_security)
        else:
            is_match = RemoteConfigurator.__is_security_match_by_fields(old_security, new_security)

        return is_match

    @staticmethod
    def __is_security_match_by_type(old_security, new_security):
        new_security_type = new_security.get('type')

        if new_security_type == 'accessToken':
            return RemoteConfigurator.__is_access_token_match(old_security.get('accessToken'),
                                                              new_security.get('accessToken'))
        elif new_security_type == 'usernamePassword':
            return RemoteConfigurator.__is_username_password_match(old_security, new_security)
        elif new_security_type == 'tlsAccessToken':
            return RemoteConfigurator.__is_ca_cert_match(old_security, new_security)
        else:
            return False

    @staticmethod
    def __is_security_match_by_fields(old_security, new_security):
        if new_security.get('accessToken'):
            return RemoteConfigurator.__is_access_token_match(old_security.get('accessToken'),
                                                              new_security.get('accessToken'))
        elif new_security.get('username'):
            return RemoteConfigurator.__is_username_password_match(old_security, new_security)
        elif new_security.get('caCert'):
            return RemoteConfigurator.__is_ca_cert_match(old_security, new_security)
        else:
            return False

    @staticmethod
    def __is_access_token_match(old_access_token, new_access_token):
        return old_access_token == new_access_token

    @staticmethod
    def __is_username_password_match(old_security, new_security):
        return new_security.get('username') == old_security.get('username') and \
               new_security.get('password') == old_security.get('password') and \
               new_security.get('clientId') == old_security.get('clientId')

    @staticmethod
    def __is_ca_cert_match(old_security, new_security):
        return new_security.get('accessToken') == old_security.get('accessToken') and \
            new_security.get('caCert') == old_security.get('caCert')
