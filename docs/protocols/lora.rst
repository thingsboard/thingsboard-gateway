LoRa Gateway Setup
====================

NOTE: Due to hardware limitations, LoRa has only been tested on gateways without any additional protocols. This was
because when LoRa was confirmed to be working, the team did not want to risk messing with the hardware setup and cause
an issue with the only available LoRa hat

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

#. To connect the sensor to ThingsBoard, follow the instructions as laid on in ChirpStack's `official ThingsBoard Integration documentation <https://www.chirpstack.io/project/guides/thingsboard/#integrate-chirpstack-application-server-with-thingsboard>`_.