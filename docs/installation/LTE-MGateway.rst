LTE Gateway Setup
=======================


Background Information
----------------
Cellular IoT, also known as Machine-to-Machine (M2M) communication, refers to the ability of devices to communicate with each other using cellular networks. One of the popular cellular IoT protocols is LTE-M, which stands for Long-Term Evolution for Machines. LTE-M is a low-power wide area network (LPWAN) technology that was designed to support IoT devices with low data rates and extended battery life. It operates on licensed cellular spectrum and provides reliable connectivity and security for IoT applications. LTE-M technology offers several benefits, including long-range coverage, low power consumption, and better penetration through walls and other obstacles. It also supports firmware over-the-air (FOTA) updates, enabling remote device management and updates. To implement this wireless protocol on an existing VarIoT gateway, the RAK2013 BG96 variant from RAK Wireless has proven to be quite advantageous.

The RAK2013 is a cellular IoT module that supports LTE-M and NB-IoT protocols. It is designed to provide reliable and efficient connectivity for a wide range of IoT applications, including smart cities, agriculture, industrial automation, and more. The module is based on the Quectel BG96-Global module and features a compact and robust design with support for a variety of interfaces, including UART, USB, and GPIOs. It also includes an embedded SIM card slot, which allows for easy and secure deployment of IoT devices. With LTE-M and NB-IoT support, the RAK2013 provides low-power, wide-area coverage for IoT applications. It features extended battery life and enables remote device management and firmware updates using FOTA technology. Additionally, it offers built-in security features, including secure boot, secure storage, and hardware encryption, to ensure the protection of sensitive data.

Installation and Configuration
------------------
To get started with the RAK2013, the user must first install the appropriate firmware provided by the manufacturer. Please follow these steps in sequential order, or risk corrupting the hardware configuration of the mounted Linux image:

1. Configure the Raspberry Pi to **enable** *SPI* and *I2C* interfaces, **disable** *login shell over serial*, and **enable** *serial port hardware*: :code:`sudo raspi-config`
  * The raspi-config menu brings up a GUI with configuration parameters
  * Select the “Interface Options”
  * Each interface is appropriately named. Be sure to enable/disable according to these instructions
	* After each interface configuration has been properly modified, navigate back to the landing page for raspi-config and select “finish”. 
  * The user will be prompted to reboot the Pi; please do so before proceeding.
2.  Clone the rak_common_for_gateway github repository in the Pi’s home directory: :code:`git clone https://github.com/RAKWireless/rak_common_for_gateway.git`
3. Install the proper firmware:
  * Navigate into the rak_common_for_gateway directory: :code:`cd ~/rak_common_for_gateway`
	* Run the install.sh script: :code:`sudo ./install.sh`
	* A list should appear asking to choose the correct model for the RAK Gateway. Please enter the number 8 in order to choose the RAK7248(SPI) with LTE (RAK2287(SPI) + LTE + raspberry pi). The menu looks like this:
	::

		*	 1.RAK2245
		*	 2.RAK7243/RAK7244 no LTE
		*	 3.RAK7243/RAK7244 with LTE
		*	 4.RAK2247(USB)
		*	 5.RAK2247(SPI)
		*	 6.RAK2246
		*	 7.RAK7248(SPI) no LTE (RAK2287(SPI) + raspberry pi)
		*	 8.RAK7248(SPI) with LTE (RAK2287(SPI) + LTE + raspberry pi)
		*	 9.RAK2287(USB)
		*	 10.RAK5146(USB)
		*	 11.RAK5146(SPI)
		*	 12.RAK5146(SPI) with LTE
		Please enter 1-12 to select the model:
	Once the script completes, continue on to step #4
    * If this menu does not populate, do not panic, there is an individual choose_model.sh script the user can execute.
	  * If the select model menu did not show up during the install script, execute the choose_model.sh script found in the rak_common_for_gateway parent directory: :code:`sudo ./choose_model.sh`
	  * Be sure to choose number 8 when prompted
4. Reboot the system: :code:`sudo reboot`
  * Once the Raspberry Pi reboots, the hostname should now show as pi@rak-gateway and an ASCII message that reads “RAK WIRELESS” should be printed on the top of the terminal screen. This is confirmation that the firmware was successfully installed.
5. Configure the LTE-M settings: :code:`sudo gateway-config`
  * First select option 6: Configure APN Name
    * This will be different depending on the provider of your eSIM, however, for this example an iBasis eSIM was used. The correct APN Name is iBASIS.iot (case sensitive).
	  * If using a different eSIM, simply Google search for the appropriate APN Name.
  * Choose option 7: Configure LTE Module
  * Then select "Enable LTE Automatic Dial-Up"
      * This ensures that the LTE module is powered-on at boot time and begins to establish connection with the servicing cellular network in the area
6. At this point, it would be a good idea to change the gateway password to something other than the default password. Choose option 1: Set pi password
  * Set the password according to the standard all other VarIoT gateways follow within DWSL. Contact the appropriate advisor for more information.
7. Reboot the system: :code:`sudo reboot`
8. Verify that the LTE module is powered on by observing the three status LEDs mounted on the board. There should be two steady LEDs, one blue and one green, and another flashing red LED.
9. After about a minute of boot time, verify that the LTE connection was established by running :code:`ip a` or :code:`ifconfig`. A list of network devices will populate, and the LTE device is named ppp0. If an IP address shows under the ppp0 device, the LTE connection was successfully established.
10. To confirm the network is reachable via LTE-M connection first remove the ethernet cable if connected, then ping Google’s DNS server: :code:`ping 8.8.8.8`
  * If the network is reachable, the packets will successfully return every few milliseconds.
  * If the network is unreachable, the shell will display the appropriate error message. If this is the case, please refer to the troubleshooting section.


**Congratulations!** You now have an LTE-M enabled VarIoT gateway.


Using the LTE Connection for ThingsBoard Telemetry
----------------------------------
In order to use the LTE connection to publish telemetry to ThingsBoard, the user must first VPN into the Drexel network using their own login credentials. Instructions on how to login to the Drexel VPN are found in the Install ThingsBoard Gateway from Source section, under Optional: VPN. Before beginning the VPN login process, it would behoove the user to begin a tmux session to allow multiple shells to run. If the user elects not to use tmux, the VPN connection will take ownership of the shell’s foreground, barring the user from interacting with the gateway in their current session.

1. Open a tmux session: :code:`tmux`
	* If the command tmux is not available, the package must first be installed: :code:`sudo apt-get install tmux`
	* Once it finishes installing, execute the command: :code:`tmux`
  * While in tmux, fork a new shell by pressing *ctrl+B*, followed immediately by *shift+5* or *shift+”*. The keystroke ctrl+B is the prefix key for all tmux commands. The keystrokes shift+5 and shift+” are to fork a new shell either vertically (shift+5) or horizontally (shift+”).
  
    * Note: The keystrokes are not typed into the command line interface, they are simply keystrokes to interact with tmux.
  * Select either of the two shell sessions to begin the Drexel VPN login sequence. 
  Upon successful SSL negotiation, navigate to the unutilized shell window by pressing *ctrl+B*, followed by one of the arrow keys; *up* or *down* arrow for vertical splits, *left* or *right* for horizontal. Do not press the arrow key while holding ctrl+B, as this will resize the shell the user is currently controlling. 
  
  * Verify that the gateway can publish telemetry to the VarIoT cloud by executing the command: :code:`mosquitto_pub -d -q 1 -h "variot.ece.drexel.edu" -p "1883" -t "v1/devices/me/telemetry" -u "$ACCESS_TOKEN" -m {"connectionTest":telemetry_via_LTE}`
  
    * Be sure to modify $ACCESS_TOKEN with the appropriate device access token for ThingsBoard.
    * The “connectionTest” telemetry should populate in ThingsBoard under the Telemetry field of the user’s target device.
  * If the telemetry test was successful, the gateway is now fully connected to the VarIoT network and the ThingsBoard service is able to function properly. 
  * It is a good idea at this point to restart the thingsboard-gateway.service so it may establish connection to the network now that the gateway has been properly configured: :code:`sudo systemctl restart thingsboard-gateway`

