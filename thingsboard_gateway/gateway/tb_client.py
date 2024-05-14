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

import random
import string
import threading
import inspect
from os.path import exists
from ssl import CERT_REQUIRED, PROTOCOL_TLSv1_2
from time import sleep, time

from simplejson import dumps, load

from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    from tb_gateway_mqtt import TBGatewayMqttClient, TBDeviceMqttClient
except ImportError:
    print("tb-mqtt-client library not found - installing...")
    TBUtility.install_package('tb-mqtt-client')
    from tb_gateway_mqtt import TBGatewayMqttClient, TBDeviceMqttClient

import tb_device_mqtt
tb_device_mqtt.DEFAULT_TIMEOUT = 3

class TBClient(threading.Thread):
    def __init__(self, config, config_folder_path, logger):
        self.__logger = logger
        super().__init__()
        self.name = 'Connection thread.'
        self.daemon = True
        self.__config_folder_path = config_folder_path
        self.__config = config
        self.__host = config["host"]
        self.__port = config.get("port", 1883)
        self.__default_quality_of_service = config.get("qos", 1)
        self.__min_reconnect_delay = 1
        self.__ca_cert = None
        self.__private_key = None
        self.__cert = None
        self.__client_id = ""
        self.__username = None
        self.__password = None
        self.__is_connected = False
        self.__stopped = False
        self.__paused = False
        self._last_cert_check_time = 0
        self.__service_subscription_callbacks = []

        # check if provided creds or provisioning strategy
        if config.get('security'):
            self._create_mqtt_client(config['security'])
        elif config.get('provisioning'):
            if exists(self.__config_folder_path + 'credentials.json'):
                with open(self.__config_folder_path + 'credentials.json', 'r') as file:
                    credentials = load(file)
                creds = self._get_provisioned_creds(credentials)
            else:
                credentials = config['provisioning']
                logger.info('Starting provisioning gateway...')

                credentials_type = credentials.pop('type', 'ACCESS_TOKEN')
                if credentials_type.upper() == 'ACCESS_TOKEN':
                    credentials['access_token'] = ''.join(random.choice(string.ascii_lowercase) for _ in range(15))
                elif credentials_type.upper() == 'MQTT_BASIC':
                    credentials['client_id'] = ''.join(random.choice(string.ascii_lowercase) for _ in range(15))
                    credentials['username'] = ''.join(random.choice(string.ascii_lowercase) for _ in range(15))
                    credentials['password'] = ''.join(random.choice(string.ascii_lowercase) for _ in range(15))
                elif credentials_type.upper() == 'X509_CERTIFICATE':
                    self._ca_cert_name = credentials.pop('caCert')
                    new_cert_path = self.__config_folder_path + 'cert.pem'
                    new_private_key_path = self.__config_folder_path + 'key.pem'
                    gen_hash = TBUtility.generate_certificate(new_cert_path, new_private_key_path).decode('utf-8')
                    credentials['hash'] = gen_hash
                else:
                    raise RuntimeError(
                        'Unknown provisioning type (Available options: AUTO, ACCESS_TOKEN, MQTT_BASIC, X509_CERTIFICATE)')

                gateway_name = 'Gateway ' + ''.join(random.choice(string.ascii_lowercase) for _ in range(5))
                prov_gateway_key = credentials.pop('provisionDeviceKey')
                prov_gateway_secret = credentials.pop('provisionDeviceSecret')
                creds = TBDeviceMqttClient.provision(host=self.__host,
                                                     port=1883,
                                                     device_name=gateway_name,
                                                     provision_device_key=prov_gateway_key,
                                                     provision_device_secret=prov_gateway_secret,
                                                     **credentials)

                with open(self.__config_folder_path + 'credentials.json', 'w') as file:
                    creds['caCert'] = self._ca_cert_name
                    file.writelines(dumps(creds))
                logger.info('Gateway provisioned')

                creds = self._get_provisioned_creds(creds)

            self._create_mqtt_client(creds)
        else:
            raise RuntimeError('Security section not provided')

        # pylint: disable=protected-access
        # Adding callbacks
        self.client._client._on_connect = self._on_connect
        self.client._client._on_disconnect = self._on_disconnect
        # self.client._client._on_log = self._on_log
        self.start()

    # def _on_log(self, *args):
    #     if "exception" in args[-1]:
    #         log.exception(args)
    #     else:
    #         log.debug(args)

    def _create_mqtt_client(self, credentials):
        self.__tls = bool(credentials.get('tls', False) or credentials.get('caCert', False))
        if credentials.get("accessToken") is not None:
            self.__username = str(credentials["accessToken"])
        if credentials.get("username") is not None:
            self.__username = str(credentials["username"])
        if credentials.get("password") is not None:
            self.__password = str(credentials["password"])
        if credentials.get("clientId") is not None:
            self.__client_id = str(credentials["clientId"])

        if self.__config.get('rateLimits'):
            self.client = TBGatewayMqttClient(self.__host, self.__port, self.__username, self.__password, self,
                                              quality_of_service=self.__default_quality_of_service,
                                              client_id=self.__client_id, rate_limit=self.__config['rateLimits'])
        else:
            self.client = TBGatewayMqttClient(self.__host, self.__port, self.__username, self.__password, self,
                                              quality_of_service=self.__default_quality_of_service,
                                              client_id=self.__client_id)

        if self.__tls:
            self.__ca_cert = self.__config_folder_path + credentials.get("caCert") if credentials.get(
                "caCert") is not None else None
            self.__private_key = self.__config_folder_path + credentials.get("privateKey") if credentials.get(
                "privateKey") is not None else None
            self.__cert = self.__config_folder_path + credentials.get("cert") if credentials.get(
                "cert") is not None else None
            self.__check_cert_period = credentials.get('checkCertPeriod', 86400)
            self.__certificate_days_left = credentials.get('certificateDaysLeft', 3)

            # check certificates for end date
            self._check_cert_thread = threading.Thread(name='Check Certificates Thread',
                                                       target=self._check_certificates, daemon=True)
            self._check_cert_thread.start()

            self.client._client.tls_set(ca_certs=self.__ca_cert,
                                        certfile=self.__cert,
                                        keyfile=self.__private_key,
                                        tls_version=PROTOCOL_TLSv1_2,
                                        cert_reqs=CERT_REQUIRED,
                                        ciphers=None)
            if credentials.get("insecure", False):
                self.client._client.tls_insecure_set(True)

    @staticmethod
    def _get_provisioned_creds(credentials):
        creds = {}
        if credentials.get('credentialsType') == 'ACCESS_TOKEN':
            creds['accessToken'] = credentials['credentialsValue']
        elif credentials.get('credentialsType') == 'MQTT_BASIC':
            creds['clientId'] = credentials['credentialsValue']['clientId']
            creds['username'] = credentials['credentialsValue']['userName']
            creds['password'] = credentials['credentialsValue']['password']
        elif credentials.get('credentialsType') == 'X509_CERTIFICATE':
            creds['tls'] = True
            creds['caCert'] = credentials['caCert']
            creds['privateKey'] = 'key.pem'
            creds['cert'] = 'cert.pem'

        return creds

    def _check_certificates(self):
        while not self.__stopped and not self.__paused:
            if time() - self._last_cert_check_time >= self.__check_cert_period:
                if self.__cert:
                    self.__logger.info('Will generate new certificate')
                    new_cert = TBUtility.check_certificate(self.__cert, key=self.__private_key,
                                                           days_left=self.__certificate_days_left)

                    if new_cert:
                        self.client.send_attributes({'newCertificate': new_cert})

                if self.__ca_cert:
                    is_outdated = TBUtility.check_certificate(self.__ca_cert, generate_new=False,
                                                              days_left=self.__certificate_days_left)

                    if is_outdated:
                        self.client.send_attributes({'CACertificate': 'CA certificate will outdated soon'})

                self._last_cert_check_time = time()

            sleep(10)

    def pause(self):
        self.__paused = True

    def unpause(self):
        self.__paused = False

    def is_connected(self):
        return self.__is_connected

    def _on_connect(self, client, userdata, flags, result_code, *extra_params):
        self.__logger.debug('TB client %s connected to ThingsBoard', str(client))
        if result_code == 0:
            self.__is_connected = True
        # pylint: disable=protected-access
        self.client._on_connect(client, userdata, flags, result_code, *extra_params)
        for callback in self.__service_subscription_callbacks:
            callback()

    def _on_disconnect(self, client, userdata, result_code, properties=None):
        # pylint: disable=protected-access
        if self.client._client != client:
            self.__logger.info("TB client %s has been disconnected. Current client for connection is: %s", str(client), str(self.client._client))
            client.disconnect()
            client.loop_stop()
        else:
            self.__is_connected = False
            if len(inspect.signature(self.client._on_disconnect).parameters) == 4:
                self.client._on_disconnect(client, userdata, result_code, properties)
            else:
                self.client._on_disconnect(client, userdata, result_code)

    def stop(self):
        # self.disconnect()
        self.client.stop()
        self.__stopped = True

    def disconnect(self):
        self.__paused = True
        self.unsubscribe('*')
        self.client.disconnect()

    def unsubscribe(self, subsription_id):
        self.client.gw_unsubscribe(subsription_id)
        self.client.unsubscribe_from_attribute(subsription_id)

    def connect(self, min_reconnect_delay=10):
        self.__paused = False
        self.__stopped = False
        self.__min_reconnect_delay = min_reconnect_delay

        keep_alive = self.__config.get("keep_alive", 120)
        try:
            while not self.client.is_connected() and not self.__stopped:
                if not self.__paused:
                    if self.__stopped:
                        break
                    self.__logger.debug("connecting to ThingsBoard")
                    try:
                        self.client.connect(keepalive=keep_alive,
                                            min_reconnect_delay=self.__min_reconnect_delay)
                    except ConnectionRefusedError:
                        self.__logger.error("Connection refused. Check ThingsBoard is running.")
                    except Exception as e:
                        self.__logger.exception(e)
                sleep(1)
        except Exception as e:
            self.__logger.exception(e)
            sleep(10)

    def run(self):
        while not self.__stopped:
            try:
                if not self.__stopped:
                    sleep(.2)
                else:
                    break
            except KeyboardInterrupt:
                self.__stopped = True
            except Exception as e:
                self.__logger.exception(e)

    def get_config_folder_path(self):
        return self.__config_folder_path

    def register_service_subscription_callback(self, subscribe_to_required_topics):
        self.__service_subscription_callbacks.append(subscribe_to_required_topics)

    @property
    def config(self):
        return self.__config

    def is_stopped(self):
        return self.__stopped
