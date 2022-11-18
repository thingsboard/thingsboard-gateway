# ThingsBoard IoT Gateway  

The Thingsboard **IoT Gateway** is an open-source solution that allows you to integrate devices connected to legacy and third-party systems with Thingsboard.  

Thingsboard is an open-source IoT platform for data collection, processing, visualization, and device management. See [**What is Thingsboard?**](https://thingsboard.io/docs/getting-started-guides/what-is-thingsboard/) if you are new platform user.  

[**What is ThingsBoard IoT Gateway?**](https://thingsboard.io/docs/iot-gateway/what-is-iot-gateway/)  
[**Getting started with ThingsBoard IoT Gateway**](https://thingsboard.io/docs/iot-gateway/getting-started/)

[![**What is ThingsBoard IoT Gateway?**](https://thingsboard.io/images/gateway/python-gateway-animd-ff.svg)](https://thingsboard.io/docs/iot-gateway/what-is-iot-gateway/)

### Gateway features

Thingsboard IoT Gateway provides following features:  

 - [**OPC-UA** connector](https://thingsboard.io/docs/iot-gateway/config/opc-ua/) to collect data from devices that are connected to OPC-UA servers.
 - [**MQTT** connector](https://thingsboard.io/docs/iot-gateway/config/mqtt/) to collect data that is published to external MQTT brokers. 
 - [**Modbus** connector](https://thingsboard.io/docs/iot-gateway/config/modbus/) to collect data from Modbus servers and slaves.
 - [**BLE** connector](https://thingsboard.io/docs/iot-gateway/config/ble/) to collect data from BLE devices.
 - [**Request** connector](https://thingsboard.io/docs/iot-gateway/config/request/) to collect data from HTTP API.
 - [**REST** connector](https://thingsboard.io/docs/iot-gateway/config/rest/) to collect data using REST API.
 - [**CAN** connector](https://thingsboard.io/docs/iot-gateway/config/can/) to collect data using CAN protocol.
 - [**BACnet** connector](https://thingsboard.io/docs/iot-gateway/config/bacnet/) to collect data from devices using BACnet protocol.
 - [**SNMP** connector](https://thingsboard.io/docs/iot-gateway/config/snmp/) to collect data from SNMP managers objects.
 - [**ODBC** connector](https://thingsboard.io/docs/iot-gateway/config/odbc/) to collect data from ODBC databases.
 - [**FTP** connector](https://thingsboard.io/docs/iot-gateway/config/ftp/) to collect data from files via FTP.
 - [**Socket** connector](https://thingsboard.io/docs/iot-gateway/config/socket/) to collect data from devices using sockets.
 - **XMPP** connector to collect data from XMPP devices.
 - [**Custom** connector](https://thingsboard.io/docs/iot-gateway/custom/) to collect data from custom protocols.
 - **Persistence** of collected data to guarantee data delivery in case of network and hardware failures.
 - **Automatic reconnect** to Thingsboard cluster.
 - Simple yet powerful **mapping** of incoming data and messages **to unified format**.
 - [Remote logging feature](https://thingsboard.io/docs/iot-gateway/guides/how-to-enable-remote-logging/) to monitor the gateway status through the ThingsBoard WEB interface.
 - [RPC gateway methods](https://thingsboard.io/docs/iot-gateway/guides/how-to-use-gateway-rpc-methods/) to control and get information from the gateway through ThingsBoard WEB interface.
 - [Remote shell](https://thingsboard.io/docs/iot-gateway/guides/how-to-enable-remote-shell/) to control operating system with ThingsBoard IoT Gateway from your ThingsBoard platform instance.
 - [Device renaming/removing handling](https://thingsboard.io/docs/iot-gateway/how-device-removing-renaming-works/) to keep the device list in actual state.
 - [Gateway Configurator](https://thingsboard.io/docs/iot-gateway/guides/how-to-configure-gateway-using-configurator/) easy-to-use CLI configurator.
 - **HOT Reload** for developers.
  
### Architecture  

The IoT Gateway is built on top of **Python**, however is different from similar projects that leverage OSGi technology.
The idea is distantly similar to microservices architecture.  
The gateway supports custom connectors to connect to new devices or servers and custom converters for processing data from devices.  
Especially, when we are talking about language APIs and existing libraries to work with serial ports, GPIOs, I2C, and new modules and sensors that are released every day.  

The Gateway provides simple integration APIs, and encapsulates common Thingsboard related tasks: device provisioning, local data persistence and delivery, message converters and other.  
For processing data from devices you also can write custom converter, it will receive information from device and send it to converter to convert to unified format before sending it to the ThingsBoard cluster.  

## Support

 - [Community chat](https://gitter.im/thingsboard/chat)
 - [Q&A forum](https://groups.google.com/forum/#!forum/thingsboard)
 - [Stackoverflow](http://stackoverflow.com/questions/tagged/thingsboard)
 
**Don't forget to star the repository to show your ❤️ and support.**

## Licenses

This project is released under [Apache 2.0 License](./LICENSE).
