# Thingsboard IoT Gateway
[![Join the chat at https://gitter.im/thingsboard/chat](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/thingsboard/chat?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

The Thingsboard **IoT Gateway** is an open-source solution that allows you to integrate devices connected to legacy and third-party systems with Thingsboard.

Thingsboard is an open-source IoT platform for data collection, processing, visualization, and device management. See [**What is Thingsboard?**](https://thingsboard.io/docs/getting-started-guides/what-is-thingsboard/) if you are new platform user. 

![IoT Gateway architecture](./img/tb-gateway.png?raw=true "IoT Gateway architecture")

### Documentation

Thingsboard IoT Gateway documentation is hosted on [thingsboard.io](https://thingsboard.io/docs/iot-gateway/).

### Gateway features

Thingsboard IoT Gateway provides following features:

 - **OPC-UA** extension to collect data from devices that are connected to OPC-UA servers.
 - **MQTT** extension to collect data that is published to external MQTT brokers.
 - **Persistence** of collected data to guarantee data delivery in case of network and hardware failures.
 - **Automatic reconnect** to Thingsboard cluster.
 - Simple yet powerful **mapping** of incoming data and messages **to unified format**.
  
### Architecture  

The IoT Gateway is built on top of **Java**, however is different from similar projects that leverage OSGi technology.
The idea is distantly similar to microservices architecture.
There are **other programming languages** (C, C++, Python, Javascript, Go..) that may be more suitable for application development that target IoT devices.
Especially, when we are talking about language APIs and existing libraries to work with serial ports, GPIOs, I2C, and new modules and sensors that are released every day. 

The Gateway provides simple integration APIs, and encapsulates common Thingsboard related tasks: device provisioning, local data persistence and delivery, message converters/adaptors and other.
As an application developer, you are able to choose Python, Go, C/C++ and other languages and connect to Thingsboard Gateway through external MQTT broker or OPC-UA server. 
Devices that support other protocols may be connected to gateway by implementing custom extensions.

### Sample Dashboards

[**Smart energy monitoring**](https://demo.thingsboard.io/demo?dashboardId=e8e409c0-f2b5-11e6-a6ee-bb0136cc33d0&source=github)
[![Smart energy monitoring demo](https://cloud.githubusercontent.com/assets/8308069/23790111/62c8a61e-0586-11e7-84eb-51febc54ec09.png "Smart energy monitoring demo")](https://demo.thingsboard.io/demo?dashboardId=e8e409c0-f2b5-11e6-a6ee-bb0136cc33d0&source=github)

[**Silos monitoring**](https://demo.thingsboard.io/demo?dashboardId=1f9828d0-058e-11e7-87f7-bb0136cc33d0&source=github)
[![Silos monitoring demo](https://cloud.githubusercontent.com/assets/8308069/23996135/00214844-0a55-11e7-9623-d1e3be0702ca.png "Silos monitoring demo")](https://demo.thingsboard.io/demo?dashboardId=1f9828d0-058e-11e7-87f7-bb0136cc33d0&source=github)

[**Smart bus tracking**](https://demo.thingsboard.io/demo?dashboardId=3d0bf910-ee09-11e6-b619-bb0136cc33d0&source=github)
[![Smart bus tracking demo](https://cloud.githubusercontent.com/assets/8308069/23790110/62c6ecde-0586-11e7-8249-19eafd5bf8cc.png "Smart bus tracking demo")](https://demo.thingsboard.io/demo?dashboardId=3d0bf910-ee09-11e6-b619-bb0136cc33d0&source=github)

### Getting Started

Connect to your OPC-UA server or MQTT broker in minutes by following this [guide](https://thingsboard.io/docs/iot-gateway/getting-started).

### Project Roadmap

The initial Gateway release goal is to bring Thingsboard [data collection](https://thingsboard.io/docs/user-guide/telemetry/) feature to OPC-UA and MQTT enabled devices.  
The Gateway project is currently in active development stage and you should expect following major features in next releases:

 - Ability to configure devices connected through the Gateway using Thingsboard [Attributes](https://thingsboard.io/docs/user-guide/attributes) feature.
 - Ability to control devices connected through the Gateway using Thingsboard [RPC](https://thingsboard.io/docs/user-guide/rpc/) feature.
 - Ability to configure Gateway distantly from Thingsboard [Dashboards](https://thingsboard.io/docs/user-guide/visualization/).
 - Client-side load balancing based on information about Thingsboard cluster.
 - Ability to visualize collected device data on the Gateway Web UI. 
 - Configurable edge analytics.

## Support

 - [Community chat](https://gitter.im/thingsboard/chat)
 - [Q&A forum](https://groups.google.com/forum/#!forum/thingsboard)
 - [Stackoverflow](http://stackoverflow.com/questions/tagged/thingsboard)

## Licenses

This project is released under [Apache 2.0 License](./LICENSE).
