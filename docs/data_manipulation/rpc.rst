Using RPC Commands with Thingsboard
===================================

What is RPC?
------------
RPC stands for Remote Procedure Call. It is a protocol that allows a client or device connected to a thingsboard gateway to send and receive commnds to and from ThingsBoard. RPC commands are essentially just JSON objects that contain metadata and a command to execute. Below is the simplest example of an RPC command:

.. code-block:: json

    {
        "method": "setGPIO",
        "params": {
            "pin": 4,
            "value": 1
        },
    }

Command Breakdown
-----------------
This is an example of what would be sent to the gateway to be passed on to the device. The `method` field tells the gateway what the name of the RPC command is. RPC commands are defined in the connector JSON files. They are defined for each device indiviually. The payload of the command is the `params` field. The paramters can be any JSON object, and the gateway will send that data to the device in accordance with the device protocol (eg: BLE, MQTT, etc.).

Example Configuration
---------------------
Below is an example of an RPC command definition for a BLE device:

.. code-block:: json

    "serverSideRpc": [
                {
                    "methodRPC": "setGreenLEDValue",
                    "withResponse": true,
                    "characteristicUUID": "F0001112-0451-4000-B000-000000000000"
                    "methodProcessing": "write"
                },
                {
                    "methodRPC": "getGreenLEDStatus",
                    "withResponse": true,
                    "characteristicUUID": "F0001112-0451-4000-B000-000000000000"
                    "methodProcessing": "read"
                },
    ]

The `serverSideRpc` section is a part of the `ble.json` configuration file. As you can see, we define an RPC command named  `setGreenLEDValue`. This command expects a response from the device confiming the command executed successfully. BLE devices communicate using characteristics, and the `characteristicUUID` field tells the gateway essentially what memory address in the BLE device to write the data to. The data sent over the air is the JSON string that was contained in the `params` field in the packet sent by ThingsBoard. The second RPC command we define is called `getGreenLEDStatus`. It is also a command that expects a response, and it uses the same `characteristicUUID`. The difference between this command the the previous one is that `methodProcessing` is `read`. This means that there are no `params` sent from ThingsBoard. The data that the gateway reads from the device will be relayed to ThingsBoard. 

The above configuration will be different for every type of connector, due to the inherent differences between the way that data is communicated by different protocols. The `ThingsBoard API Reference <https://thingsboard.io/docs/api/>`_ is a great resource for the different ways RPC commands are laid out.