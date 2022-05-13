Wi-Fi Router Setup
====================

The idea with the WiFI protocol is to be able to provide WiFi to the various devices that will be connected to the VarIoT gateway, this in turn will allow the WiFi devices to get internet and post to the ThingsBoard dashboard directly. If you turn your Raspberry Pi into a wireless access point, you can make it act as a router.

Simply put it involves using Raspbian and installing a couple packages that give the Pi the ability to do router-like things like assign IP addresses to devices that connect to it.

Installation
--------------
1. Install and update Raspbian

.. code-block:: sh
  
  sudo apt-get update
  sudo apt-get upgrade

* **If you get an upgrade**, It’s a good idea to reboot with sudo reboot.

2. Install hostapd and iptables

:code:`sudo apt-get install hostapd isc-dhcp-server`

* hostapd is the package that lets you create a wireless hotspot using a Raspberry Pi

:code:`sudo apt-get install iptables-persistent`

* Nice iptables manager!

* **Hit y to continue for both and then Yes to both on the configuration**

3. Set up DHCP server

* Next we will edit **/etc/dhcp/dhcpd.conf**, a file that sets up our DHCP server - this allows wifi connections to automatically get IP addresses, DNS, etc.
* Run this command to edit the file: :code:`sudo nano /etc/dhcp/dhcpd.conf`
* Find the lines that say:
.. code-block:: sh

  option domain-name "example.org";
  option domain-name-servers ns1.example.org, ns2.example.org;

* Change them by adding a # in the beginning so they are commented out
* Next find the lines that say:
.. code-block:: sh

  # If this DHCP server is the official DHCP server for the local
  # network, the authoritative directive should be uncommented.
  #authoritative;

* Then remove the # in front of authoritative. After this we will be adding the following lines to the bottom of the document
.. code-block:: sh
  
  subnet 192.168.XXX.0 netmask 255.255.255.0 {
	  range 192.168.XXX.10 192.168.XXX.50;
	  option broadcast-address 192.168.XXX.255;
	  option routers 192.168.XXX.1;
	  default-lease-time 600;
	  max-lease-time 7200;
	  option domain-name "local";
	  option domain-name-servers 8.8.8.8, 8.8.4.4;
  }
  
* Where XXX is any number you would like to choose as long as it's between 0-255
* Save the file by typing in Control-X then Y then return. Run: :code:`sudo nano /etc/default/isc-dhcp-server`
* Scroll down to INTERFACES="" and update it to say INTERFACES="wlan0". It may say INTERFACESv4 and v6 - in this case add wlan0 to both close and save the file

4.Set up wlan0 for static IP

* Next we will set up the wlan0 connection to be static and incoming. Run :code:`sudo nano /etc/network/interfaces` to edit the file
* Find the line auto wlan0 and add a # in front of the line, and in front of every line afterwards. If you don't have that line, just make sure it looks like the screenshot below in the end! Basically just remove any old wlan0 configuration settings, we'll be changing them up
* **IF THE FILE IS EMPTY DO NOT WORRY** Just add the following lines at the end of it:
.. code-block:: sh
  
  auto lo
  iface lo inet loopback
  iface eth0 inet dhcp
  allow-hotplug wlan0
  iface wlan0 inet static
    address 192.168.XXX.1
    netmask 255.255.255.0
    
* Save the file (Control+X,  then Y )
* Assign a static IP address to the wifi adapter by running:  :code:`sudo ifconfig wlan0 192.168.XXX.1`

5. Configure Access Point
* Now we can configure the access point details. We will set up a password-protected network so only people with the password can connect.
* Create a new file by running :code:`sudo nano /etc/hostapd/hostapd.conf`
* The next lines will be placed and can change the text after ssid= to another name, that will be the network broadcast name. The password can be changed with the text after wpa_passphrase=

.. code-block:: sh
  
  interface=wlan0
  ssid=NETWORK_NAME
  country_code=US
  hw_mode=g
  channel=6
  macaddr_acl=0
  auth_algs=1
  ignore_broadcast_ssid=0
  wpa=2
  wpa_passphrase=PASWORD_NAME
  wpa_key_mgmt=WPA-PSK
  wpa_pairwise=CCMP
  wpa_group_rekey=86400
  ieee80211n=1
  wme_enabled=1
  
* Save as usual. Make sure each line has no extra spaces or tabs at the end or beginning.
* Now we will tell the Pi where to find this configuration file. Run: :code:`sudo nano /etc/default/hostapd`
* Find the line #DAEMON_CONF="" and edit it so it says DAEMON_CONF="/etc/hostapd/hostapd.conf"
* Don't forget to remove the # in front to activate it! Then save the file
* Likewise, run sudo nano /etc/init.d/hostapd and find the line DAEMON_CONF= and change it to DAEMON_CONF=/etc/hostapd/hostapd.conf

6.Configure Network Address Translation

* Setting up NAT will allow multiple clients to connect to the WiFi and have all the data 'tunneled' through the single Ethernet IP. (But you should do it even if only one client is going to connect). Run :code:`sudo nano /etc/sysctl.conf`
* Scroll to the bottom and add net.ipv4.ip_forward=1 on a new line. Save the file. This will start IP forwarding on boot up
* Also run :code:`sudo sh -c "echo 1 > /proc/sys/net/ipv4/ip_forward"` to activate it immediately
* Run the following commands to create the network translation between the ethernet port eth0 and the wifi port wlan0
.. code-block:: sh
  
  sudo iptables -t nat -A POSTROUTING -o eth0 -j MASQUERADE
  sudo iptables -A FORWARD -i eth0 -o wlan0 -m state --state RELATED,ESTABLISHED -j ACCEPT
  sudo iptables -A FORWARD -i wlan0 -o eth0 -j ACCEPT

* You can check to see what's in the tables with
.. code-block:: sh
  
  sudo iptables -t nat -S
  sudo iptables -S

* To make this happen on reboot (so you don't have to type it every time) run :code:`sudo sh -c "iptables-save > /etc/iptables/rules.v4"`
* The iptables-persistent tool you installed at the beginning will automatically reload the configuration on boot for you.
* Finally we can test the access point host! Run: :code:`sudo /usr/sbin/hostapd /etc/hostapd/hostapd.conf`
* To manually run hostapd with our configuration file. You should see it set up and use wlan0 then you can check with another wifi computer that you see your SSID show up. If so, you have successfully set up the access point.
