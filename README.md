# ThingsBoard IoT Gateway

The **ThingsBoard IoT Gateway** is an open-source, Python-based application that enables seamless integration of legacy and third-party devices with the [ThingsBoard IoT platform](https://thingsboard.io/). It serves as a protocol adapter that collects data from external sources and publishes it to ThingsBoard via a unified format.

📖 [**What is ThingsBoard IoT Gateway?**](https://thingsboard.io/docs/iot-gateway/what-is-iot-gateway/)

> 💡 New to ThingsBoard? [**Learn what ThingsBoard is**](https://thingsboard.io/docs/getting-started-guides/what-is-thingsboard/)

---

## 🚀 Getting Started

Curious how the ThingsBoard IoT Gateway works in action?

➡️ **Follow our [Getting Started Guide](https://thingsboard.io/docs/iot-gateway/getting-started/)** to:

- 🚀 Run the gateway in minutes using pre-configured **demo servers**  
- 🔄 Simulate device data collection and processing workflows  
- 🧪 Explore telemetry, attributes, and RPCs without real devices  
- 🖥️ Test integration with ThingsBoard Community or Professional Edition  

> 💡 Ideal for evaluation, proof-of-concept, and learning how the gateway processes and routes data.

---

[![What is ThingsBoard IoT Gateway?](https://thingsboard.io/images/gateway/python-gateway-animd-ff.svg)](https://thingsboard.io/docs/iot-gateway/what-is-iot-gateway/)

## 🔌 Gateway Features

ThingsBoard IoT Gateway supports a wide range of industrial and IoT protocols out of the box. Each connector enables collecting data from external systems, transforming it into a unified format, and forwarding it to ThingsBoard.

### 📟 Industrial & SCADA Protocols
- [**Modbus**](https://thingsboard.io/docs/iot-gateway/config/modbus/) – Integrate with Modbus TCP/RTU devices like PLCs and energy meters.
- [**OPC-UA**](https://thingsboard.io/docs/iot-gateway/config/opc-ua/) – Connect to industrial automation systems using the OPC-UA protocol.
- [**CAN**](https://thingsboard.io/docs/iot-gateway/config/can/) – Communicate with devices over the Controller Area Network (e.g., automotive, industrial equipment).
- [**ODBC**](https://thingsboard.io/docs/iot-gateway/config/odbc/) – Retrieve telemetry from SQL-compliant databases.

### 🌐 IoT & Networking Protocols
- [**MQTT**](https://thingsboard.io/docs/iot-gateway/config/mqtt/) – Subscribe to external MQTT brokers and ingest messages.
- [**REST API**](https://thingsboard.io/docs/iot-gateway/config/rest/) – Push data to REST endpoints created by gateway.
- [**Request Connector**](https://thingsboard.io/docs/iot-gateway/config/request/) – Periodically pull data from HTTP(S) APIs and ingest response payloads.
- [**FTP**](https://thingsboard.io/docs/iot-gateway/config/ftp/) – Read files from FTP/SFTP servers for batch data ingestion.
- [**Socket**](https://thingsboard.io/docs/iot-gateway/config/socket/) – Request data via raw TCP/UDP socket connections.  
- [**SNMP**](https://thingsboard.io/docs/iot-gateway/config/snmp/) – Poll SNMP devices to collect MIB data from routers, switches, sensors.  
- [**XMPP**](https://thingsboard.io/docs/iot-gateway/config/xmpp/) – Receive telemetry from XMPP-based chat/device networks.  

### 📡 Smart Energy & Charging
- [**OCPP**](https://thingsboard.io/docs/iot-gateway/config/ocpp/) – Integrate EV charging stations using Open Charge Point Protocol.

### 🏠 Smart Building & Home Automation
- [**BACnet**](https://thingsboard.io/docs/iot-gateway/config/bacnet/) – Gather building automation data (HVAC, lighting, fire systems).
- [**KNX**](https://thingsboard.io/docs/iot-gateway/config/knx/) – Interface with KNX-based building automation systems.
- [**BLE**](https://thingsboard.io/docs/iot-gateway/config/ble/) – Scan and connect to BLE-enabled devices (e.g., beacons, wearables).

### 🧩 Extensibility
- [**Custom Connectors**](https://thingsboard.io/docs/iot-gateway/custom/) – Build your own protocol handlers using Python to support any proprietary system or emerging protocol.

> ✨ All connectors support flexible configuration, data transformation, and integration with the ThingsBoard platform's device model.

## 🧰 Core Gateway Capabilities

In addition to multi-protocol support, the ThingsBoard IoT Gateway includes robust features for reliability, remote management, and automation:

### 🔒 Reliability & Resilience
- **Data persistence** – Buffers telemetry locally to prevent data loss during network or system outages.
- **Automatic reconnection** – Automatically restores connection to the ThingsBoard cluster after temporary failures.

### 🔄 Unified Data Processing
- **Data mapping engine** – Transforms raw input from devices into ThingsBoard’s unified data format using customizable converters.

### 🛠️ Remote Management & Control
- [**Remote configuration**](https://thingsboard.io/docs/iot-gateway/guides/how-to-enable-remote-configuration/) – Update and manage gateway configuration directly from the ThingsBoard web UI.
- [**Remote logging**](https://thingsboard.io/docs/iot-gateway/guides/how-to-enable-remote-logging/) – View and stream logs remotely for troubleshooting and monitoring.
- [**Gateway service RPC methods**](https://thingsboard.io/docs/iot-gateway/guides/how-to-use-gateway-rpc-methods/) – Interact with the gateway using platform-initiated RPC commands.
- [**Remote shell access**](https://thingsboard.io/docs/iot-gateway/guides/how-to-enable-remote-shell/) – Run shell commands on the gateway host via the ThingsBoard platform.

### 🔄 Device Lifecycle Handling
- [**Device rename/removal detection**](https://thingsboard.io/docs/iot-gateway/how-device-removing-renaming-works/) – Automatically synchronizes device renames and deletions to keep the platform device list up to date.  

## 🏗️ Architecture Overview

The IoT Gateway is implemented in **Python**, allowing powerful extension and customization. It follows a modular architecture resembling microservices.

- **Custom connectors** let you interface with new devices or services.
- **Custom converters** allow transformation of incoming messages to a ThingsBoard-compatible format.
- The Gateway provides simple integration APIs, and encapsulates common Thingsboard related tasks: device provisioning, local data persistence and delivery, message converters and other.  

> Ideal for edge use cases where flexibility and protocol diversity are key.

---

## 💬 Support & Community

Need help or want to share ideas?

- 💬 [**GitHub Discussions**](https://github.com/thingsboard/thingsboard-gateway/discussions) – Ask questions, propose features, or share use cases.
- ❓ [**StackOverflow**](http://stackoverflow.com/questions/tagged/thingsboard-gateway) – Use the `thingsboard-gateway` tag.

> 🐞 Found a bug? Please open an [issue](https://github.com/thingsboard/thingsboard-gateway/issues).

---

## ⭐ Contributing

We welcome contributions! Feel free to fork the repo, open PRs, or help triage issues.

---

## ⚖️ License

This project is licensed under the [Apache 2.0 License](./LICENSE).

---

🌟 **Don't forget to star the repository to show your ❤️ and support!**
