# Campus-Scale IoT Gateway Components
This repository contains the necessary instructions and components to set up a gateway within the Campus-Scale IoT infrastruture

## Initialize Gateway

To connect with ThingsBoard, the gateways utilize an open-source software stack called ThingsBoard gateway, the source code for which can be found in [thingsboard-gateway](./thingsboard-gateway)

Follow the instructions [here](https://thingsboard.io/docs/iot-gateway/getting-started/) to configure the gateway and the instructions [here](https://thingsboard.io/docs/iot-gateway/install/source-installation/) to install the gateway from source.

**NOTE:** Skip "Step 2" of the source install instructions. There is no need to clone from the official repository as the source code already lives in [thingsboard-gateway](./thingsboard-gateway)

## Git Setup
1. `sudo apt install git` - Install git onto the gateway
2. `ssh-keygen` - Generate an ssh key
    * You do not need to enter a passphrase or specify an alternate location to store the key - simply leave the prompts blank and hit enter. However, take note of the location the prompt says the key will be saved to (ex.: /home/pi/.ssh/id_rsa.pub)
3. On GitHub, go to your [settings](https://github.com/settings) and select "SSH and GPG keys"
4. Select the green "New SSH Key" button
5. Give the key a title of your choice (ex.:VarIoT Board)
6. `cat [KEY LOCATION]/id_rsa.pub` - Print your ssh key to the command line
7. Copy the result of the above command, paste it into "Key" on GitHub, and save
8. `git clone git@github.com:drexelwireless/thingsboard-gateway.git` - Clone the repository
## BLE
Coming soon...

## WiFi
Coming soon...

## ZigBee
### Hardware:
* Texas Instruments LAUNCHXL-CC1352P-2

Follow instructions [here](https://www.zigbee2mqtt.io/guide/adapters/#recommended) under the aforementioned hardware to set the development board up as a ZigBee coordinator

### Software:
To set up zigbee2mqtt, follow the instructions as laid out in the [official documentation](https://www.zigbee2mqtt.io/guide/installation/01_linux.html)

* In addition to the required steps, follow the steps as laid out in "(Optional) Running as a daemon with systemctl" to run zigbee2mqtt as a service in the background

Pair your ZigBee devices by following the steps as laid out [here](https://www.zigbee2mqtt.io/guide/usage/pairing_devices.html)

Update the [test Python script](./zigbee2mqtt/test_mqtt.py) with any updated information (access tokens, zigbee2mqtt topics, etc.) and run the script by executing:
* `python ./zigbee2mqtt/test_mqtt.py`
## LoRa
## Hardware:

## Software:
Reference Documentation:
- Rak Wireless GitHub Repository: https://github.com/RAKWireless/rak_common_for_gateway#readme
- RAK 2245 Pi Hat Quick Start Guide: https://docs.rakwireless.com/Product-Categories/WisLink/RAK2245-Pi-HAT/Quickstart/

1. `sudo raspi-config` - Enter Raspberry Pi settings, select "Interfacing Options"
    - Enable SPI interface
    - Enable i2C interface
    - Disable login shell over serial
    - Enable serial port hardware
2. `git clone https://github.com/RAKWireless/rak_common_for_gateway.git ~/rak_common_for_gateway` - Clone the RAK Wireless Gateway Software
3. `cd ~/rak_common_for_gateway` - Move into the Rak software directory
4. `sudo ./install.h` - Install the RAK gateway software
    a. When prompted, select `RAK2245` as the gateway model
5. Reboot the gateway
6. `sudo raspi-config` - Enter gateway configuration
