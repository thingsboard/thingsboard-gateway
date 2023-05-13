Hardware and Operating System Prerequisites
===========================================

The **VarIoT Gateway** is an amalgamation of both open-source hardware and front-end, open-source software. Before diving into the Setup instructions found in this instance of ReadTheDocs, it is important 
the end-user posses the appropriate hardware and operating system (OS) for proper functionality of the VarIoT gateway.

Hardware
--------
* The VarIoT gateway shall be instantiated on a Raspberry Pi 4B, or Raspberry Pi Compute Model 4. Models older than the Raspberry Pi 4B do not have adequate RAM to run the ThingsBoard service.
* A corresponding microSD card must also be used in conjunction with the Raspberry Pi in order to store the operating system. A minimum of 32 GB is *highly* recommended.

Operating System
----------------
* An Ubuntu distrobution for the Raspberry Pi is required for proper functionality of the gateway. There are two options available to the user:
 
  1. Download the official Raspberry Pi Imager and flash a microSD card with a Raspian OS (Bullseye) image. The imager can be found on the `official Raspberry Pi website <https://www.raspberrypi.com/software/>`_.
  2. Flash a microSD card with a VarIoT Linux image that has been preconfigured with support for both LTE-M and LoRaWaN hardware. This image is found on the `VarIoT Google Drive <https://drive.google.com/file/d/1JmDyyV_89n3nTgnFq6ypgmyraMQ1k05P/view?usp=share_link>`_.     
    
    * Note: This image already has the following installed:
      
      * rak_common_for_gateway firmware for both RAK2284 LoRa RPi Hat and RAK2013 LTE-M module
      * A partially configured thingsboard-gateway service, with all required dependencies for a full fledged VarIoT gateway

* If the user elects option 2, please ensure to modify the *tb_gateway.yaml* file, as mentioned in `Configure ThingsBoard Gateway <https://variot.readthedocs.io/en/latest/installation/tb-gateway.html#configure-thingsboard-gateway>`_ before use.
  
  * **DO NOT SKIP THIS STEP!**
