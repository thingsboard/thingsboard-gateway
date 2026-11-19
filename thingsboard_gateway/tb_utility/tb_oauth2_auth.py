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

from threading import Lock
from time import monotonic

from requests import post
from requests.auth import AuthBase, HTTPBasicAuth
from requests.exceptions import RequestException

from thingsboard_gateway.tb_utility.tb_utility import TBUtility

TOKEN_REFRESH_LIFETIME_RATIO = 0.75
TOKEN_REQUEST_TIMEOUT = 10
TOKEN_RETRY_DELAY = 10
CLIENT_AUTHENTICATION_METHODS = ('header', 'body')


class OAuth2TokenError(RequestException):
    pass


class OAuth2ClientCredentialsAuth(AuthBase):
    def __init__(self, token_url, client_id, client_secret, logger, scope=None, client_authentication='header',
                 verify=True):
        self.__token_url = token_url
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__log = logger
        self.__scope = scope
        self.__client_authentication = client_authentication
        self.__verify = verify
        self.__lock = Lock()
        self.__access_token = None
        self.__expires_at = 0
        self.__refresh_at = 0
        self.__retry_at = 0

    @classmethod
    def from_config(cls, security_config, logger, verify=True):
        client_authentication = security_config.get('clientAuthentication', 'header')
        if client_authentication not in CLIENT_AUTHENTICATION_METHODS:
            raise ValueError(f'"security.clientAuthentication" must be one of '
                             f'{", ".join(CLIENT_AUTHENTICATION_METHODS)}, got "{client_authentication}"')

        token_url = security_config.get('tokenUrl')
        client_id = _resolve_env_placeholders(security_config.get('clientId'), 'security.clientId')
        client_secret = _resolve_env_placeholders(security_config.get('clientSecret'), 'security.clientSecret')
        missing_parameters = [name for name, value in (('tokenUrl', token_url),
                                                       ('clientId', client_id),
                                                       ('clientSecret', client_secret)) if not value]
        if missing_parameters:
            parameters = ', '.join(f'"security.{name}"' for name in missing_parameters)
            raise ValueError(f'Missing required parameter(s) {parameters} for oauth2 security')

        return cls(token_url, str(client_id), str(client_secret), logger,
                   scope=security_config.get('scope'),
                   client_authentication=client_authentication,
                   verify=verify)

    def __call__(self, request):
        request.headers['Authorization'] = 'Bearer ' + self.__get_token()
        request.register_hook('response', self.__handle_401)
        return request

    def __handle_401(self, response, **kwargs):
        if response.status_code != 401:
            return response

        rejected_token = response.request.headers.get('Authorization', '').removeprefix('Bearer ')
        self.__log.debug('Request to %s was rejected with code 401, retrying with a new OAuth2 access token',
                         response.request.url)
        token = self.__get_token(rejected_token)

        response.content
        response.close()
        retry_request = response.request.copy()
        retry_request.headers['Authorization'] = 'Bearer ' + token
        retry_response = response.connection.send(retry_request, **kwargs)
        retry_response.history.append(response)
        retry_response.request = retry_request
        return retry_response

    def __get_token(self, rejected_token=None):
        with self.__lock:
            now = monotonic()
            token_is_valid = (self.__access_token is not None
                              and self.__access_token != rejected_token
                              and (self.__expires_at is None or now < self.__expires_at))
            refresh_is_due = self.__refresh_at is not None and now >= self.__refresh_at
            if token_is_valid and (not refresh_is_due or now < self.__retry_at):
                return self.__access_token

            if now < self.__retry_at:
                raise OAuth2TokenError(f'No valid OAuth2 access token, next attempt to obtain it from '
                                       f'{self.__token_url} in {self.__retry_at - now:.0f} s')

            try:
                self.__request_token(now)
            except OAuth2TokenError as e:
                self.__retry_at = now + TOKEN_RETRY_DELAY
                self.__log.error('%s', e)
                if token_is_valid:
                    self.__log.warning('Current OAuth2 access token is still valid and will be used until it expires')
                    return self.__access_token
                raise

            return self.__access_token

    def __request_token(self, now):
        data = {'grant_type': 'client_credentials'}
        if self.__scope:
            data['scope'] = self.__scope

        auth = None
        if self.__client_authentication == 'body':
            data['client_id'] = self.__client_id
            data['client_secret'] = self.__client_secret
        else:
            auth = HTTPBasicAuth(self.__client_id, self.__client_secret)

        try:
            response = post(self.__token_url, data=data, auth=auth, headers={'Accept': 'application/json'},
                            timeout=TOKEN_REQUEST_TIMEOUT, verify=self.__verify)
        except RequestException as e:
            raise OAuth2TokenError(f'Failed to obtain OAuth2 access token from {self.__token_url}: {e}') from e

        if not response.ok:
            raise OAuth2TokenError(f'Failed to obtain OAuth2 access token from {self.__token_url}: '
                                   f'{response.status_code} {response.reason} {response.text[:200]}')

        try:
            token_response = response.json()
            access_token = token_response['access_token']
        except (ValueError, TypeError, KeyError) as e:
            raise OAuth2TokenError(f'Response from {self.__token_url} does not contain "access_token"') from e

        expires_in = self.__parse_expires_in(token_response.get('expires_in'))
        self.__access_token = str(access_token)
        self.__retry_at = 0
        if expires_in is None:
            self.__expires_at = None
            self.__refresh_at = None
            self.__log.info('Obtained OAuth2 access token from %s without expiration time, '
                            'it will be renewed only after code 401', self.__token_url)
        else:
            self.__expires_at = now + expires_in
            self.__refresh_at = now + expires_in * TOKEN_REFRESH_LIFETIME_RATIO
            self.__log.info('Obtained OAuth2 access token from %s, expires in %s s', self.__token_url, expires_in)

    @staticmethod
    def __parse_expires_in(value):
        try:
            expires_in = float(value)
        except (TypeError, ValueError):
            return None

        return expires_in if expires_in > 0 else None


def _resolve_env_placeholders(value, parameter_name):
    if not isinstance(value, str):
        return value

    resolved_value, missing_variables = TBUtility.substitute_env_placeholders(value)
    if missing_variables:
        variables = ', '.join(f'"{variable}"' for variable in missing_variables)
        raise ValueError(f'"{parameter_name}" uses environment variable(s) {variables} that are not set')

    return resolved_value
