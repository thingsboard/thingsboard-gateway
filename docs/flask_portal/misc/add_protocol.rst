Adding protocol connections onto the VarIoT Portal
==================================================

These will include instructions on how to add new devices/protocols for the portal.

The Thingsboard IoT Gateway
---------------------------
#. The Thingsboard IoT gateway already has several data protocols that it can connect to.
	-New Thingsboard ver. may have more protocols available, so updating the core Thingsboard code might be helpful
	-See the available connectors at: https://github.com/drexelwireless/thingsboard-gateway/tree/master/thingsboard-gateway
	-To connect other data protocols, you would have to convert whatever protocol you're using to one of these protocols(see our zigbee2mqtt example)
#. The Thingsboard and Python requires Python 3.7 or higher
#. Sample .json scripts that the Thingsboard would accept and interpret can be found in the config file:
	-Observe these sample here: https://github.com/drexelwireless/thingsboard-gateway/tree/master/thingsboard-gateway/thingsboard_gateway/config
	-Use these sample scripts when building the necessary scripts and testing out the code.

VarIoT Portal Backend
---------------------
The portal's purpose is to allow a convenient way for users to input their devices onto the Thingsboard. It will read any of the device's required specification and insert it onto the Thingsboard, preferably in a single menu. This way, the user does not have to navigate the Thingsboard to connect their devices to entities to brokers to connectors and so on.

#. The functionalities that connect the portal to the Thingsboard can be foudn in the backend folder
#. The types of data protocols and devices that the portal can connect to are described within the 'model' subfolder.
	-Currently it is set up to handle Zigbee and Bluetooth devices
	-If attempting to connect a data protocol that is available in the Thingsboard gateway, build the models around the sample .json scripts found in the Thingsboard config(See 'The Thingsboard IoT Gateway' section).
	-Any new protocol you add should have a model built here
#. The addDevice.py is used to add devices(using available protocols) to the Thigsboard
	-It is currently set up to connect directly from a device(Bluetooth, Bacnet, CAN, Modbus, and HTTP(S) Request could be connected through here once their models are added)
	-Does not work for protocols that do not directly connect the device ot the Thingsboard(MQTT requires brokers, OPC-UA requires a server, and SNMP requires a manager)
		-These 'mediums'(brokers, servers, and managers) will require their own models to be built.

TODO
----
Enable a way for the interface to store specs of previously added devices, or to maintain the connection when device is found so that user does not have to keep replacing device data eveytime the device is used.