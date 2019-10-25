from setuptools import setup

setup(
    packages=['thingsboard_gateway', 'thingsboard_gateway.gateway', 'thingsboard_gateway.storage',
              'thingsboard_gateway.tb_client', 'thingsboard_gateway.connectors', 'thingsboard_gateway.connectors.ble',
              'thingsboard_gateway.connectors.mqtt', 'thingsboard_gateway.connectors.opcua',
              'thingsboard_gateway.connectors.modbus', 'thingsboard_gateway.tb_utility'],
    install_requires=[
        'cffi',
        'jsonpath-rw',
        'jsonpath-rw-ext',
        'jsonschema==3.1.1',
        'lxml',
        'opcua',
        'paho-mqtt',
        'pymodbus',
        'pyserial',
        'pytz',
        'PyYAML',
        'six'
    ],
    download_url='https://github.com/thingsboard/thingsboard-gateway/archive/2.0.0rc.tar.gz',
    entry_points={
        'console_scripts': [
            'thingsboard-gateway = thingsboard_gateway.tb_gateway:daemon'
        ]
    })
