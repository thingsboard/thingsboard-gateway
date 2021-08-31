from __future__ import print_function, unicode_literals

from os import path

from PyInquirer import style_from_dict, Token, prompt
from pyfiglet import Figlet
from termcolor import colored
from prompt_toolkit.validation import ValidationError, Validator
from re import compile, IGNORECASE
from os.path import exists, splitext
from yaml import safe_load, dump

if exists('thingsboard_gateway/config/tb_gateway.yaml'):
    CONFIG_PATH = 'thingsboard_gateway/config/tb_gateway.yaml'
elif exists(path.dirname(path.abspath(__file__)) + '/config/tb_gateway.yaml'.replace('/', path.sep)):
    CONFIG_PATH = path.dirname(path.abspath(__file__)) + '/config/tb_gateway.yaml'.replace('/', path.sep)
elif exists("/etc/thingsboard-gateway/config/tb_gateway.yaml".replace('/', path.sep)):
    CONFIG_PATH = "/etc/thingsboard-gateway/config/tb_gateway.yaml".replace('/', path.sep)

style = style_from_dict({
    Token.Separator: '#cc5454',
    Token.QuestionMark: '#673ab7 bold',
    Token.Selected: '#673ab7',
    Token.Pointer: '#673ab7 bold',
    Token.Instruction: '',  # default
    Token.Answer: '#673ab7 bold',
    Token.Question: '',
})


class NotNullValidator(Validator):
    def validate(self, document):
        if not document.text or document.text == '':
            raise ValidationError(message='Value can be empty!', cursor_position=len(document.text))


class NumberValidator(Validator):
    def validate(self, document):
        try:
            int(document.text)
        except ValueError:
            raise ValidationError(message='Must be a number type!', cursor_position=len(document.text))


class PortValidator(Validator):
    def validate(self, document):
        try:
            port = int(document.text)
        except ValueError:
            raise ValidationError(message='Must be a number type!', cursor_position=len(document.text))

        if not 1 <= port <= 65535:
            raise ValidationError(message='Port is invalid!', cursor_position=len(document.text))


class HostValidator(NotNullValidator):
    def validate(self, document):
        super(HostValidator, self).validate(document)

        hostname = document.text
        if len(hostname) > 255:
            return ValidationError(message='Host is invalid!', cursor_position=len(document.text))

        if hostname[-1] == ".":
            hostname = hostname[:-1]

        allowed = compile(r"(?!-)[A-Z\d-]{1,63}(?<!-)$", IGNORECASE)
        if not all(allowed.match(x) for x in hostname.split(".")):
            raise ValidationError(message='Host is invalid!', cursor_position=len(document.text))


class PathValidator(Validator):
    def validate(self, document):
        if not exists(document.text):
            raise ValidationError(message='File doesn\'t exist!', cursor_position=len(document.text))

        if splitext(document.text)[1] != '.pem':
            raise ValidationError(message='File must be .pem extension!', cursor_position=len(document.text))


class FileExtensionValidator(NotNullValidator):
    def validate(self, document):
        super(FileExtensionValidator, self).validate(document)

        if document.text.split('.')[-1] != 'json':
            raise ValidationError(message='File must be .json!', cursor_position=len(document.text))


def generate_config_file(data: {str: str}) -> None:
    with open(CONFIG_PATH, 'w') as file:
        dump(data, file, default_flow_style=False, sort_keys=False)


def read_config_file() -> {str: str}:
    with open(CONFIG_PATH, 'r') as file:
        config_dict = safe_load(file)

    return config_dict


def configure():
    try:
        default_config = read_config_file()

        base_questions = [
            {
                'type': 'input',
                'name': 'host',
                'message': 'ThingsBoard host:',
                'default': default_config['thingsboard']['host'],
                'validate': HostValidator
            },
            {
                'type': 'input',
                'name': 'port',
                'message': 'ThingsBoard port:',
                'default': str(default_config['thingsboard']['port']),
                'validate': PortValidator,
                'filter': lambda val: int(val)
            },
            {
                'type': 'confirm',
                'name': 'remoteShell',
                'message': 'Do you want to have access from remote shell? (No)',
                'default': False
            },
            {
                'type': 'confirm',
                'name': 'remoteConfiguration',
                'message': 'Do you want to enable remote configuration feature? (No)',
                'default': False
            },
            {
                'type': 'input',
                'name': 'statsSendPeriodInSeconds',
                'message': 'Statistics will be send every (sec.):',
                'default': str(default_config['thingsboard']['statsSendPeriodInSeconds']),
                'validate': NumberValidator,
                'filter': lambda val: int(val)
            },
            {
                'type': 'input',
                'name': 'checkConnectorsConfigurationInSeconds',
                'message': 'Connectors config files will check every (sec.):',
                'default': str(default_config['thingsboard']['checkConnectorsConfigurationInSeconds']),
                'validate': NumberValidator,
                'filter': lambda val: int(val)
            },
            {
                'type': 'list',
                'name': 'security',
                'message': 'What security type do you need?',
                'choices': [
                    'Access Token (Basic Security)',
                    'TLS + Access Token (Advanced Security)',
                    'TLS + Private Key (Advanced Security)'
                ]
            }
        ]

        f = Figlet(font='small')
        print(colored(f.renderText('ThingsBoard'), color= 'green', attrs=['bold']), sep='')
        print(colored(f.renderText('IoT Gateway'), color='blue', attrs=['bold']))
        print(colored('Welcome to ThingsBoard IoT Gateway configuration Wizard', 'cyan'))
        print(colored('Let\'s configure you Gateway by answering on questions below ⬇\n'))

        base_answers = prompt(base_questions, style=style)

        access_token_config = [
            {
                'type': 'input',
                'name': 'accessToken',
                'message': 'Your token:',
                'validate': NotNullValidator
            }
        ]
        tls = [
            {
                'type': 'input',
                'name': 'caCert',
                'message': 'Path to your CA file (.pem):',
                'validate': PathValidator
            }
        ]
        tls_access_token_config = access_token_config + tls
        tls_private_key_config = [
                                     {
                                         'type': 'input',
                                         'name': 'privateKey',
                                         'message': 'Path to you private key file (.pem):',
                                         'validate': PathValidator
                                     }
                                 ] + tls + [
                                     {
                                         'type': 'input',
                                         'name': 'cert',
                                         'message': 'Path to your certificate file (.pem):',
                                         'validate': PathValidator
                                     }
                                 ]

        if base_answers['security'] == 'Access Token (Basic Security)':
            security_questions = access_token_config
        elif base_answers['security'] == 'TLS + Access Token (Advanced Security)':
            security_questions = tls_access_token_config
        else:
            security_questions = tls_private_key_config

        security_answers = prompt(security_questions, style=style)

        qos_and_storage_type_question = [
            {
                'type': 'input',
                'name': 'qos',
                'message': 'QoS:',
                'validate': NumberValidator,
                'default': str(default_config['thingsboard']['qos']),
                'filter': lambda val: int(val)
            },
            {
                'type': 'list',
                'name': 'storage',
                'message': 'Choose storage type:',
                'choices': [
                    'Memory',
                    'File storage'
                ],
                'filter': lambda val: val.lower() if val == 'Memory' else 'file'
            }
        ]

        qos_and_storage_type_answers = prompt(qos_and_storage_type_question, style=style)

        if qos_and_storage_type_answers['storage'] == 'memory':
            storage_questions = [
                {
                    'type': 'input',
                    'name': 'read_records_count',
                    'message': 'Count of messages to get from storage and send to ThingsBoard:',
                    'default': str(default_config['storage'].get('read_records_count', '')),
                    'validate': NumberValidator,
                    'filter': lambda val: int(val)
                },
                {
                    'type': 'input',
                    'name': 'max_records_count',
                    'message': 'Maximum count of data in storage before send to ThingsBoard:',
                    'default': str(default_config['storage'].get('max_records_count', '')),
                    'validate': NumberValidator,
                    'filter': lambda val: int(val)
                }
            ]
        else:
            storage_questions = [
                {
                    'type': 'input',
                    'name': 'data_folder_path',
                    'message': 'Path to folder, that will contains data (Relative or Absolute):',
                    'default': str(default_config['storage'].get('data_folder_path', '')),
                    'validate': NotNullValidator
                },
                {
                    'type': 'input',
                    'name': 'max_file_count',
                    'message': 'Maximum count of file that will be saved:',
                    'default': str(default_config['storage'].get('max_file_count', '')),
                    'validate': NumberValidator,
                    'filter': lambda val: int(val)
                },
                {
                    'type': 'input',
                    'name': 'max_read_records_count',
                    'message': 'Count of messages to get from storage and send to ThingsBoard:',
                    'default': str(default_config['storage'].get('max_read_records_count', '')),
                    'validate': NumberValidator,
                    'filter': lambda val: int(val)
                },
                {
                    'type': 'input',
                    'name': 'max_records_per_file',
                    'message': 'Maximum count of records that will be stored in one file:',
                    'default': str(default_config['storage'].get('max_records_per_file', '')),
                    'validate': NumberValidator,
                    'filter': lambda val: int(val)
                }
            ]

        storage_answers = prompt(storage_questions, style=style)

        connectors_questions = [
            {
                'type': 'checkbox',
                'name': 'connectors',
                'message': 'Choose connectors you want to use:',
                'choices': [
                    {
                        'name': 'MQTT',
                    },
                    {
                        'name': 'FTP',
                    },
                    {
                        'name': 'Modbus',
                    },
                    {
                        'name': 'CAN',
                    },
                    {
                        'name': 'Bacnet',
                    },
                    {
                        'name': 'BLE',
                    },
                    {
                        'name': 'OPC-UA',
                    },
                    {
                        'name': 'ODBC',
                    },
                    {
                        'name': 'Request',
                    },
                    {
                        'name': 'REST',
                    },
                    {
                        'name': 'SNMP'
                    }
                ],
                'validate': lambda answer: 'You must choose at least one connector.' if len(answer) == 0 else True
            }
        ]

        connectors_answers = prompt(connectors_questions, style=style)

        connectors_list = []

        for connector in connectors_answers['connectors']:
            print(colored(f'Configuration {connector} connector:', 'blue'))
            connector_questions = [
                {
                    'type': 'input',
                    'name': 'name',
                    'message': 'Name of connector:',
                    'validate': NotNullValidator
                },
                {
                    'type': 'input',
                    'name': 'configuration',
                    'message': 'Config file of connector:',
                    'validate': FileExtensionValidator
                }
            ]
            connector_answers = prompt(connector_questions, style=style)
            connectors_list.append({'type': connector.lower(), **connector_answers})

        generate_config_file(
            {
                'thingsboard': {**base_answers, 'security': security_answers,
                                'qos': qos_and_storage_type_answers['qos']},
                'storage': {'type': qos_and_storage_type_answers['storage'], **storage_answers},
                'connectors': connectors_list
            })
    except Exception:
        print(colored('Something went wrong! Please try again.', color='red'))
        raise SystemExit

if __name__ == '__main__':
    configure()
