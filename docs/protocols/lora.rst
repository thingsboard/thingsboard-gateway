LoRa Gateway Setup
====================

NOTE: In order to use the RAK2245 RPi HAT, the SPI and I2C interfaces must be enabled on the Pi as noted in `Configuring the Gateway <https://docs.rakwireless.com/Product-Categories/WisLink/RAK2245-Pi-HAT/Quickstart/#configuring-the-gateway>`_ in the official RAK2245 Pi Hat Quick Start Guide. When these interfaces are enabled, the UART RX/TX lines are no longer available to the embedded Bluetooth hardware, and thus the bluetooth service is effectively disabled. It is highly recommended that a Bluetooth USB-dongle be acquired in order to use Bluetooth in conjunction with the LoRaWAN protocol.

Hardware
--------
This system was configured using a `RAK2245 RPi HAT Edition (US915 MHz) <https://store.rakwireless.com/products/rak2245-pi-hat?variant=39945102000326>`_

Software Setup
--------------
#. Follow the installation procedure as outlined in the RAK Wireless Gateway instructions under `Installation Procedure <https://github.com/RAKWireless/rak_common_for_gateway#installation-procedure>`_

#. Follow the steps listed under `Configuring the Gateway <https://docs.rakwireless.com/Product-Categories/WisLink/RAK2245-Pi-HAT/Quickstart/#configuring-the-gateway>`_ in the official RAK2245 Pi Hat Quick Start Guide

#. Follow the steps listed under `Connect the Gateway with ChirpStack <https://docs.rakwireless.com/Product-Categories/WisLink/RAK2245-Pi-HAT/Quickstart/#connect-the-gateway-with-chirpstack>`_ in the official RAK2245 Pi Hat Quick Start Guide

Connecting a LoRa Sensor
------------------------
The process for connecting each LoRa sensor will likely be different. Before purchasing a LoRa sensor, be sure there is
proven documentation on how to both connect the sensor to a ChirpStack application and decode the data into usable
values.

#. To see an example of how to connect a LoRa sensor to ChirpStack, please reference page 47 and onwards `of this SenseCAP sensor guide <../_static/Guide-for-SenseCAP-Adaption-to-3rd-Party-Gateways-Servers-V1.2.pdf>`_

#. To connect the sensor to ThingsBoard, follow the instructions as laid on in ChirpStack's `official ThingsBoard Integration documentation <https://www.chirpstack.io/project/guides/thingsboard/#integrate-chirpstack-application-server-with-thingsboard>`_.

#. To create a rule chain to decode the data values sent to ThingsBoard, follow the instructions as laid out in `ThingsBoard's official documentation on how to transform incoming telemetry <https://thingsboard.io/docs/user-guide/rule-engine-2-0/tutorials/transform-incoming-telemetry/>`_.
