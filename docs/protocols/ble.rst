BLE Setup
====================


Installation Guide
--------------
1. Install linux and python libraries

.. code-block:: sh

    sudo apt install -y libglib2.0-dev zlib1g-dev
    sudo pip3 install bleak

JSON File Configuration Guide
--------------
1. Navigate and open ble.json file with text editor

.. code-block:: sh

    cd thingsboard-gateway/thingsboard-gateway/thingsboard_gateway/config/
    nano ble.json

This is sample basic configuration JSON file for BLE.

.. code-block:: sh

    {
    "name": "BLE Connector",
    "passiveScanMode": true,
    "showMap": false,
    "scanner": {
        "timeout": 10000
    },
    "devices": [
        {
        "name": "SENSOR_NAME",
        "MACAddress": "MAC_ADDRESS",
        "pollPeriod": 10000,
        "showMap": false,
        "timeout": 10000,
        "telemetry": [
            {
            "key": "TELEMETRY_NAME",
            "method": "METHOD",
            "characteristicUUID": "UUID",
            "valueExpression": "VALUE_EXPRESSION"
            }
        ],
        "attributes": [
            {
            "key": "Device Name",
            "method": "read",
            "characteristicUUID": "00002a00-0000-1000-8000-00805f9b34fb",
            "valueExpression": "[:]"
            }
        ],
        "attributeUpdates": [
            {
            }
        ],
        "serverSideRpc": [
            {
            }
        ]
        }
    ]
    }

2. Edit device object
Enter the ``SENSOR_NAME`` name of device, in double quotes, as you want it to appear in Thingsboard
Enter the hardware BLE mac address of the device 

3. Telemetry
All four fields in telemetry, key, method, characteristicUUID, and valueExpression, need to be filled out.

Key is the name of the telemetry that will be shown in thingsboard.
Method is how the gateway will retrieve  the data. (read, write, or notify)
CharacteristicUUID is the UUID where the telemetry is store on the senor.
ValueExpression is how the byte data from the senor will be displayed in thingsboard. This is similar  to python.


Web Portal Configuration Guide
--------------

mac address



