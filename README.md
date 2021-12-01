# Campus-Scale IoT Gateway Components
This repository contains the necessary instructions and components to set up a gateway within the Campus-Scale IoT infrastruture

## Initialize Gateway

To connect with ThingsBoard, the gateways utilize an open-source software stack called ThingsBoard gateway, the source code for which can be found in [thingsboard-gateway](./thingsboard-gateway)

Follow the instructions [here](https://thingsboard.io/docs/iot-gateway/getting-started/) to configure the gateway and the instructions [here](https://thingsboard.io/docs/iot-gateway/install/source-installation/) to install the gateway from source.

**NOTE:** Skip "Step 2" of the source install instructions. There is no need to clone from the official repository as the source code already lives in [thingsboard-gateway](./thingsboard-gateway)
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
## LoRa
Coming soon...
