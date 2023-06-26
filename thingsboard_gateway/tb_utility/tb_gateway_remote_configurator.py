from logging import getLogger
from time import sleep, time
from logging.config import dictConfig

from regex import fullmatch
from simplejson import dumps, load

from thingsboard_gateway.gateway.tb_client import TBClient
from thingsboard_gateway.tb_utility.tb_logger import TBLoggerHandler

LOG = getLogger("service")


class RemoteConfigurator:
    DEFAULT_STATISTICS = {
        'enable': True,
        'statsSendPeriodInSeconds': 3600
    }

    def __init__(self, gateway, config):
        self._gateway = gateway
        self._config = config
        self._load_connectors_configuration()
        self._logs_configuration = self._load_logs_configuration()
        self.in_process = False
        self._active_connectors = []
        self._handlers = {
            'general_configuration': self._handle_general_configuration_update,
            'storage_configuration': self._handle_storage_configuration_update,
            'grpc_configuration': self._handle_grpc_configuration_update,
            'logs_configuration': self._handle_logs_configuration_update,
            'active_connectors': self._handle_active_connectors_update,
            'RemoteLoggingLevel': self._handle_remote_logging_level_update,
            r'(?=\D*\d?).*': self._handle_connector_configuration_update,
        }
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
        return self._config.get('connectors', [])

    def _get_active_connectors(self):
        return [connector['name'] for connector in self.connectors_configuration]

    def _get_tb_gateway_general_config_for_save(self):
        """
        Method used only for saving data to conf files
        !!!Don't use it for sending data to TB (use `_get_tb_gateway_general_config_for_remote` instead)!!!
        """

        connectors_config = [
            {'type': connector['type'], 'name': connector['name'], 'configuration': connector['configuration']} for
            connector in self.connectors_configuration]

        return {
            'thingsboard': self.general_configuration,
            'storage': self.storage_configuration,
            'grpc': self.grpc_configuration,
            'connectors': connectors_config
        }

    def _get_tb_gateway_general_config_for_remote(self):
        """
        Method used only for saving data to TB
        !!!Don't use it for saving data to conf files (use `_get_tb_gateway_general_config_for_save`)!!!
        """

        stat_conf_path = self.general_configuration['statistics'].get('configuration')
        commands = []
        if stat_conf_path:
            with open(self._gateway.get_config_path() + stat_conf_path, 'r') as file:
                commands = load(file)
        return {**self.general_configuration}.update(
            {
                'statistics': {
                    'enable': self.general_configuration['statistics']['enable'],
                    'statsSendPeriodInSeconds': self.general_configuration['statistics']['statsSendPeriodInSeconds'],
                    'configuration': self.general_configuration['statistics'].get('configuration'),
                    'commands': commands
                }
            }
        )

    def send_current_configuration(self):
        """
        Calling manually only on RemoteConfigurator init (TbGatewayService.__init_remote_configuration method)
        When Gateway started, sending all configs for states synchronizing
        """

        LOG.debug('Sending all configurations (init)')
        self._gateway.tb_client.client.send_attributes(
            {'general_configuration': self._get_tb_gateway_general_config_for_remote()})
        self._gateway.tb_client.client.send_attributes({'storage_configuration': self.storage_configuration})
        self._gateway.tb_client.client.send_attributes({'grpc_configuration': self.grpc_configuration})
        self._gateway.tb_client.client.send_attributes({'logs_configuration': self._logs_configuration})
        self._gateway.tb_client.client.send_attributes({'active_connectors': self._get_active_connectors()})
        self._gateway.tb_client.client.send_attributes({'Version': self._gateway.version.get('current_version', 0.0)})
        for connector in self.connectors_configuration:
            self._gateway.tb_client.client.send_attributes(
                {connector['name']: connector})

    def _load_connectors_configuration(self):
        for (_, connector_list) in self._gateway.connectors_configs.items():
            for connector in connector_list:
                for general_connector_config in self.connectors_configuration:
                    if general_connector_config['name'] == connector['name']:
                        config = connector.pop('config')[general_connector_config['configuration']]
                        general_connector_config.update(connector)
                        general_connector_config['configurationJson'] = config

    def _load_logs_configuration(self):
        """
        Calling only on RemoteConfigurator init (__init__ method)
        """

        try:
            with open(self._gateway.get_config_path() + 'logs.json', 'r') as logs:
                return load(logs)
        except Exception as e:
            LOG.exception(e)

    def process_config_request(self, config):
        if not self.in_process:
            LOG.debug('Got config update request: %s', config)

            self.in_process = True

            try:
                for attr_name in config.keys():
                    if 'deleted' in attr_name:
                        continue

                    for (name, func) in self._handlers.items():
                        if fullmatch(name, attr_name):
                            func(config[attr_name])
                            break
            except (KeyError, AttributeError):
                LOG.error('Unknown attribute update name (Available: %s)', ', '.join(self._handlers.keys()))
            finally:
                self.in_process = False
        else:
            LOG.error("Remote configuration is already in processing")

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

        LOG.info('Processing general configuration update')

        LOG.info('--- Checking connection configuration changes...')
        if config['host'] != self.general_configuration['host'] or config['port'] != self.general_configuration[
            'port'] or config['security'] != self.general_configuration['security'] or config.get('provisioning',
                                                                                                  {}) != self.general_configuration.get(
                'provisioning', {}) or config['qos'] != self.general_configuration['qos']:
            LOG.info('---- Connection configuration changed. Processing...')
            success = self._apply_connection_config(config)
            if not success:
                config.update(self.general_configuration)
        else:
            LOG.info('--- Connection configuration not changed.')

        LOG.info('--- Checking statistics configuration changes...')
        changed = self._check_statistics_configuration_changes(config.get('statistics', self.DEFAULT_STATISTICS))
        if changed:
            LOG.info('---- Statistics configuration changed. Processing...')
            success = self._apply_statistics_config(config.get('statistics', self.DEFAULT_STATISTICS))
            if not success:
                config['statistics'].update(self.general_configuration['statistics'])
        else:
            LOG.info('--- Statistics configuration not changed.')

        LOG.info('--- Checking device filtering configuration changes...')
        if config.get('deviceFiltering') != self.general_configuration.get('deviceFiltering'):
            LOG.info('---- Device filtering configuration changed. Processing...')
            success = self._apply_device_filtering_config(config)
            if not success:
                config['deviceFiltering'].update(self.general_configuration['deviceFiltering'])
        else:
            LOG.info('--- Device filtering configuration not changed.')

        LOG.info('--- Checking Remote Shell configuration changes...')
        if config.get('remoteShell') != self.general_configuration.get('remoteShell'):
            LOG.info('---- Remote Shell configuration changed. Processing...')
            success = self._apply_remote_shell_config(config)
            if not success:
                config['remoteShell'].update(self.general_configuration['remoteShell'])
        else:
            LOG.info('--- Remote Shell configuration not changed.')

        LOG.info('--- Checking other configuration parameters changes...')
        self._apply_other_params_config(config)

        LOG.info('--- Saving new general configuration...')
        self.general_configuration = config
        self._gateway.tb_client.client.send_attributes({'general_configuration': self.general_configuration})
        self._cleanup()
        with open(self._gateway.get_config_path() + "tb_gateway.json", "w",
                  encoding="UTF-8") as file:
            file.writelines(dumps(self._get_tb_gateway_general_config_for_save()))

    def _handle_storage_configuration_update(self, config):
        LOG.debug('Processing storage configuration update...')

        old_event_storage = self._gateway._event_storage
        try:
            storage_class = self._gateway.event_storage_types[config["type"]]
            self._gateway._event_storage = storage_class(config)
        except Exception as e:
            LOG.error('Something went wrong with applying the new storage configuration. Reverting...')
            LOG.exception(e)
            self._gateway._event_storage = old_event_storage
        else:
            # TODO: maybe it can be replaced by setter
            self.storage_configuration = config
            with open(self._gateway.get_config_path() + "tb_gateway.json", "w", encoding="UTF-8") as file:
                file.writelines(dumps(self._get_tb_gateway_general_config_for_save()))
            self._gateway.tb_client.client.send_attributes({'storage_configuration': self.storage_configuration})

            LOG.info('Processed storage configuration update successfully')

    def _handle_grpc_configuration_update(self, config):
        LOG.debug('Processing GRPC configuration update...')
        if config != self.grpc_configuration:
            try:
                self._gateway.init_grpc_service(config)
                for connector_name in self._gateway.available_connectors:
                    self._gateway.available_connectors[connector_name].close()
                self._gateway.load_connectors(self._get_tb_gateway_general_config_for_save())
                self._gateway.connect_with_connectors()
            except Exception as e:
                LOG.error('Something went wrong with applying the new GRPC configuration. Reverting...')
                LOG.exception(e)
                self._gateway.init_grpc_service(self.grpc_configuration)
                for connector_name in self._gateway.available_connectors:
                    self._gateway.available_connectors[connector_name].close()
                self._gateway.load_connectors(self._get_tb_gateway_general_config_for_save())
                self._gateway.connect_with_connectors()
            else:
                # TODO: maybe it can be replaced by setter
                self.grpc_configuration = config
                with open(self._gateway.get_config_path() + "tb_gateway.json", "w", encoding="UTF-8") as file:
                    file.writelines(dumps(self._get_tb_gateway_general_config_for_save()))
                self._gateway.tb_client.client.send_attributes({'grpc_configuration': self.grpc_configuration})

                LOG.info('Processed GRPC configuration update successfully')

    def _handle_logs_configuration_update(self, config):
        global LOG
        LOG.debug('Processing logs configuration update...')
        try:
            LOG = getLogger('service')
            logs_conf_file_path = self._gateway.get_config_path() + 'logs.json'

            dictConfig(config)
            LOG = getLogger('service')
            self._gateway.remote_handler = TBLoggerHandler(self._gateway)
            self._gateway.remote_handler.activate(self._gateway.main_handler.level)
            self._gateway.main_handler.setTarget(self._gateway.remote_handler)
            LOG.addHandler(self._gateway.remote_handler)

            with open(logs_conf_file_path, 'w') as logs:
                logs.write(dumps(config))

            LOG.debug("Logs configuration has been updated.")
        except Exception as e:
            LOG.error("Remote logging configuration is wrong!")
            LOG.exception(e)

    def _handle_active_connectors_update(self, config):
        LOG.debug('Processing active connectors configuration update...')

        for active_connector_name in self._gateway.available_connectors:
            if active_connector_name not in config:
                try:
                    self._gateway.available_connectors[active_connector_name].close()
                    self._delete_connector_from_config(active_connector_name)
                    with open(self._gateway.get_config_path() + 'tb_gateway.json', 'w') as file:
                        file.writelines(dumps(self._get_tb_gateway_general_config_for_save()))
                except Exception as e:
                    LOG.exception(e)

        self._active_connectors = config
        self._gateway.tb_client.client.send_attributes({'active_connectors': config})

    def _handle_connector_configuration_update(self, config):
        """
        Expected the following data structure:
        {
            "name": "Mqtt Broker Connector",
            "type": "mqtt",
            "configuration": "mqtt.json",
            "log_level": "INFO",
            "key?type=>grpc": "auto",
            "class?type=>custom": "",
            "configurationJson": {
                ...
            }
        }
        """

        LOG.debug('Processing logs configuration update...')

        try:
            # if path.isfile(self._gateway.get_config_path() + config.get('configuration')) and config[
            #         'name'] in self._active_connectors:
            #     available_connector = self._gateway.available_connectors.get(config['name'])
            #     if available_connector:
            #         available_connector.close()
            #
            #     for connector in self.connectors_configuration:
            #         if connector['name'] == config['name'] and connector['type'] != config['type']:
            #             connector['type'] = config['type']
            #             with open(self._gateway.get_config_path() + 'tb_gateway.json', 'w') as file:
            #                 file.writelines(dumps(self._get_tb_gateway_general_config_for_save()))
            #
            #     with open(self._gateway.get_config_path() + config['configuration'], 'w') as file:
            #         file.writelines(dumps(config['configurationJson']))
            #
            # else:
            configuration = config['configuration']
            with open(self._gateway.get_config_path() + configuration, 'w') as file:
                config['configurationJson'].update({'logLevel': config['log_level'], 'name': config['name']})
                file.writelines(dumps(config['configurationJson']))

            found_connectors = list(filter(lambda item: item['name'] == config['name'], self.connectors_configuration))
            if not found_connectors:
                connector_configuration = {'name': config['name'], 'type': config['type'],
                                           'configuration': configuration}
                if config.get('key'):
                    connector_configuration['key'] = config['key']

                if config.get('class'):
                    connector_configuration['class'] = config['class']

                self.connectors_configuration.append(connector_configuration)
                with open(self._gateway.get_config_path() + 'tb_gateway.json', 'w') as file:
                    file.writelines(dumps(self._get_tb_gateway_general_config_for_save()))
            else:
                found_connector = found_connectors[0]
                connector_configuration = {'name': config['name'], 'type': config['type'],
                                           'configuration': configuration}

                if config.get('key'):
                    connector_configuration['key'] = config['key']

                if config.get('class'):
                    connector_configuration['class'] = config['class']

                found_connector.update(connector_configuration)
                self._gateway.available_connectors[connector_configuration['name']].close()

            self._gateway.load_connectors(self._get_tb_gateway_general_config_for_save())
            self._gateway.connect_with_connectors()

            self._gateway.tb_client.client.send_attributes({config['name']: config})
        except Exception as e:
            LOG.exception(e)

    def _handle_remote_logging_level_update(self, config):
        self._gateway.tb_client.client.send_attributes({'RemoteLoggingLevel': config})

    # HANDLERS SUPPORT METHODS -----------------------------------------------------------------------------------------
    def _apply_connection_config(self, config) -> bool:
        apply_start = time() * 1000
        old_tb_client = self._gateway.tb_client
        try:
            old_tb_client.disconnect()
            old_tb_client.stop()

            self._gateway.tb_client = TBClient(config, old_tb_client.get_config_folder_path())
            self._gateway.tb_client.connect()
            connection_state = False
            while time() * 1000 - apply_start < 10000 and not connection_state:
                connection_state = self._gateway.tb_client.is_connected()
                sleep(.1)

            if connection_state:
                self._gateway.subscribe_to_required_topics()
                return True
            else:
                self._revert_connection()
                LOG.info("The gateway cannot connect to the ThingsBoard server with a new configuration.")
                return False
        except Exception as e:
            LOG.exception(e)
            self._revert_connection()
            return False

    def _revert_connection(self):
        try:
            LOG.info("Remote general configuration will be restored.")
            self._gateway.tb_client.disconnect()
            self._gateway.tb_client.stop()
            self._gateway.tb_client = TBClient(self.general_configuration, self._gateway.get_config_path())
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

                with open(self._gateway.get_config_path() + statistics_conf_file_name, 'w') as file:
                    file.writelines(dumps(commands))
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

    def _apply_other_params_config(self, config):
        self._gateway.config['thingsboard'].update(config)

    def _delete_connector_from_config(self, connector_name):
        self._config['connectors'] = list(
            filter(lambda connector: connector['name'] != connector_name, self.connectors_configuration))

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
