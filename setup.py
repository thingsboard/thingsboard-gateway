from setuptools import setup

setup(
    name='thingsboard_gateway',
    version='1.0.0',
    packages=['thingsboard_gateway', 'thingsboard_gateway.gateway', 'thingsboard_gateway.storage',
              'thingsboard_gateway.tb_client', 'thingsboard_gateway.connectors', 'thingsboard_gateway.connectors.ble',
              'thingsboard_gateway.connectors.mqtt', 'thingsboard_gateway.connectors.opcua',
              'thingsboard_gateway.connectors.modbus', 'thingsboard_gateway.tb_utility'],
    url='https://thingsboard.io',
    license='Apache Software License (Apache Software License 2.0)',
    author='ThingsBoard',
    author_email='info@thingsboard.io',
    description='The Thingsboard IoT Gateway is an open-source solution that allows you to integrate devices connected to legacy and third-party systems with Thingsboard.',
    include_package_data=True,
    package_data={'thingsboard_gateway': ['config/*']},
    install_requires=[
        'attrs>=19.2.0',
        'cffi>=1.0.0',
        'cryptography>=2.4',
        'jsonpath-rw>=1.3.0',
        'jsonpath-rw-ext>=1.1.2',
        'jsonschema==3.1.1',
        'lxml>=4.3.1',
        'opcua>=0.9',
        'paho-mqtt>=1.3.0',
        'pymodbus>=2.1.0',
        'pyserial>=3.2',
        'pytz',
        'PyYAML>=5.0.2',
        'six>=1.0.0'
    ],
    entry_points={
        'console_scripts': [
            'thingsboard-gateway = thingsboard_gateway.thingsboard_gateway:daemon'
        ]
    }
)
