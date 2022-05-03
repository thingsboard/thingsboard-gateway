Wifi Router Setup
====================
The idea with the WiFI protocol is to be able to provide WiFi to the various devices that will be connected to the VarIoT gateway, this in turn will allow the WiFi devices to get internet and post to the ThingsBoard dashboard directly. If you turn your Raspberry Pi into a wireless access point, you can make it act as a router.

Simply put it involves using Raspbian and installing a couple packages that give the Pi the ability to do router-like things like assign IP addresses to devices that connect to it.


Installation Guide
--------------
1. Install and update Raspbian

.. code-block:: sh
  
  sudo apt-get update
  sudo apt-get upgrade

* **If you get an upgrade**, It’s a good idea to reboot with sudo reboot.

2. Install hostapd and dnsmasq

:code:`sudo apt-get install hostapd`

* hostapd is the package that lets you create a wireless hotspot using a Raspberry Pi

:code:`sudo apt-get install dnsmasq`

* dnsmasq is an easy-to-use DHCP and DNS server

* **Hit y to continue for both**

3. Turn the programs off before any changes are made to configuration files

.. code-block:: sh
  sudo systemctl stop hostapd
  sudo systemctl stop dnsmasq

4. Configure a static IP for the wlan0 interface

* Will have to ask the Drexel Tech Team for them to assure we can provision other IPs the importance will be seen in later steps
* For this purpose here, replace IP addresses: 192.168.###.### with your own.
* Open dhcpcd configuration file :code:`sudo nano /etc/dhcpcd.conf`
* In the file, add the following lines to the end, assure the last 2 lines are in the order seen:

.. code-block:: sh

  interface wlan0
  static ip_address=192.168.0.10/24
  denyinterfaces eth0
  denyinterfaces wlan0

* After that, press Ctrl+X, then Y, then Enter to save the file and exit the editor.

5. Configure the DHCP server (dnsmasq)

* The DHCP server is used to dynamically distribute network configuration parameters, such as IP addresses, for interfaces and services. In this case to provision a new IP address for every new device added to the system.
* dnsmasq’s default configuration file contains a lot of unnecessary information, so it’s easier to start from scratch. 
* Rename the default configuration file and write a new one:

:code:`sudo mv /etc/dnsmasq.conf /etc/dnsmasq.conf.orig`
:code:`sudo nano /etc/dnsmasq.conf`

* You’ll be editing a new file now, this is the config file that dnsmasq will use.Type these lines into your new configuration file:

.. code-block:: sh

  interface=wlan0
  dhcp-range=192.168.0.11,192.168.0.30,255.255.255.0,24h

* The lines we added mean that we’re going to provide IP addresses between 192.168.0.11 and 192.168.0.30 for the wlan0 interface.

6. Configure the access point host software (hostapd)

* Now we will edit the hostapd configuration file: :code:`sudo nano /etc/hostapd/hostapd.conf`
* In the new file add the following:

.. code-block:: sh

  interface=wlan0
  bridge=br0  
  hw_mode=g
  channel=7
  wmm_enabled=0
  macaddr_acl=0
  auth_algs=1
  ignore_broadcast_ssid=0
  wpa=2
  wpa_key_mgmt=WPA-PSK
  wpa_pairwise=TKIP
  rsn_pairwise=CCMP
  ssid=**NETWORK**
  wpa_passphrase=**PASSWORD**

* Where NETWORK and PASSWORD are your own names you must create for the PI, this is what will appear to others and your devices
* Now we need to show the system this file: :code:`sudo nano /etc/default/hostapd`
* In this file, find the line that says :code:`#DAEMON_CONF=””`, delete that # and put the path to our config file in the quotes, so that it looks like this: :code:`DAEMON_CONF="/etc/hostapd/hostapd.conf"`

7. Set up traffic forwarding

* When you connect to your Pi, it will forward the traffic over your Ethernet cable. So we’re going to have wlan0 forward via Ethernet cable to your modem. This involves editing another config file:
:code:`sudo nano /etc/sysctl.conf`
* Find this line: :code:`#net.ipv4.ip_forward=1`
* Delete the #, so it looks like this: :code:`net.ipv4.ip_forward=1`

8. Add a new iptables rule

* Add IP masquerading for outbound traffic on eth0 using iptables: :code:`sudo iptables -t nat -A POSTROUTING -o eth0 -j MASQUERADE`
* Save the table: :code:`sudo sh -c "iptables-save > /etc/iptables.ipv4.nat"`
* To load the rule on boot, we need to edit the file /etc/rc.local and add the following line just above the line exit 0: :code:`iptables-restore < /etc/iptables.ipv4.nat`

9. Enable internet connection

* Now the Pi is set up to be an access point but we still need to enable the internet on it by building a bridge between wlan0 and eth0 interfaces. Otherwise devices will connect but not be able to access the internet.
* Install the following package: :code:`sudo apt-get install bridge-utils`
* Add a new bridge (called br0): :code:`sudo brctl addbr br0`
* Connect eth0 interface to bridge just created: :code:`sudo brctl addif br0 eth0`
* Edit the interfaces file: :code:`sudo nano /etc/network/interfaces`
* Add the following lines at the end of the file:

.. code-block:: sh

  auto br0
  iface br0 inet manual
  bridge_ports eth0 wlan0

10. Reboot

* Restart the Pi to save all settings and configurations files to go into effect: :code:`sudo reboot`
* The Pi is now a Wireless Access Point
