BLE Setup
====================


Installation Guide
--------------
1. Install linux and python libraries

.. code-block:: sh
    sudo apt-get install -y libglib2.0-dev zlib1g-dev
    sudo pip3 install bleak

JSON File Configuration Guide
--------------
1. Navigate and open ble.json file with text editor
.. code-block:: sh
    cd ThingsBoard/
    nano ble.json

2. Edit device object
Enter the name of device, in double quotes, as you want it to appear in Thingsboard
Enter the hardware BLE mac address of the device 

3. Telemetry
All four fields in telemetry, key, method, characteristicUUID, and valueExpression, need to be filled out.

Key is the name of the telemetry that will be shown in thingsboard.
Method is how the gateway will retrieve  the data. (read, write, or notify)
CharacteristicUUID is the UUID where the telemetry is store on the senor.
ValueExpression is how the byte data from the senor will be displayed in thingsboard. This is similar  to python.


Web Portal Configuration Guide
--------------


