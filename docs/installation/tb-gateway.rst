Install ThingsBoard Gateway from Source
=======================================

Generate an SSH Key to Clone the GitHub Repository
--------------------------------------------------
#. Move to the home directory of the gateway: :code:`cd ~`
#. Generate an SSH key: :code:`ssh-keygen`

   * You don't need to enter any special configuration values for your SSH key if prompted. Simply  press **Enter** for each prompt until the command has finished running.
#. Visit the directory where your SSH key can now be found: :code:`cd .ssh`
#. Display the public key. Copy it.: :code:`cat id_rsa.pub`
#. Go to the `GitHub <https://github.com/>`_ and log in with your account associated with the gateway repository
#. Navigate to *Settings* > *SSH and GPG keys* > *New SSH Key*
#. Paste the public key in the Key box

Install ThingsBoard Gateway
---------------------------
#. Move to the home directory of the gateway: :code:`cd ~`
#. Install dependencies: :code:`sudo apt update && sudo apt install python3-dev python3-pip libglib2.0-dev git mosquitto mosquitto-clients`
#. Install python dependencies: :code:`pip install cython PyYAML`
#. Clone the repository: :code:`git clone --single-branch --branch thingsboard-master git@github.com:drexelwireless/thingsboard-gateway.git`
#. Move to the repository: :code:`cd thingsboard-gateway`
#. Install the gateway python module: :code:`sudo python3 setup.py install`
#. In order to run your gateway from the source, copy :code:`thingsboard-gateway.service` to the :code:`/etc/systemd/system` directory

   * Before copying :code:`thingsboard-gateway.service` you need to edit the file. Open :code:`thingsboard-gateway.service` in your editor of choice and delete all contents. Then copy and paste the following : :code:```[Unit]
Description=ThingsBoard Gateway
After=network-online.target wait-for-dns.service
Wants=network-online.target wait-for-dns.service

[Service]
ExecStart=/usr/bin/python3 /home/pi/thingsboard-gateway/thingsboard-gateway/thingsboard_gateway/tb_gateway.py
ExecStopPost=/home/pi/thingsboard-gateway/setup_files/bluetooth-kill.sh

[Install]
WantedBy=multi-user.target```

   * Be sure to save the file before closing. 
   
   * Move to the directory the service file will live in: :code:`cd /etc/systemd/system`
   * Copy the service files to the current directory: :code:`cp ~/setup_files/*.service .` <-- the period here is important
#. Notify the system that daemons were changed: :code:`sudo systemctl daemon-reload`

Create Gateway Device
---------------------
#. Go to the `VarIoT Devices <http://variot.ece.drexel.edu/devices>`_ panel
#. Click the "+" button near the upper right corner to create a new device:

   * Give it a name
   * Check the "Is a gateway" box
   * Click "Add"
#. Click on the new device in the list then click the "Copy access token" button

Configure ThingsBoard Gateway
-----------------------------
#. Move to the config directory of the gateway: :code:`cd ~/thingsboard-gateway/thingsboard-gateway/thingsboard_gateway/config`
#. Edit the main gateway config file: :code:`nano tb_gateway.yaml`
   
   * Replace :code:`host: thingsboard.cloud` with :code:`host: variot.ece.drexel.edu`
   * Replace :code:`accessToken: PUT_YOUR_ACCESS_TOKEN_HERE` with the copied access token
   * Press :code:`Ctrl-O <Enter> Ctrl-X` to save and exit

Optional: VPN
-------------
#. If VPN access is needed, install it with: :code:`sudo apt install openconnect network-manager-openconnect-gnome`
#. Connect with: :code:`sudo openconnect -u <your username> -b <VPN host server>`

Run ThingsBoard Gateway
-----------------------
#. Start the ThingsBoard service: :code:`sudo service thingsboard-gateway start`
#. Enable the Thingsboard service on start-up: :code:`sudo systemctl enable thingsboard-gateway.service`
#. Check the service status: :code:`sudo service thingsboard-gateway status` (Press q to return to the prompt)
   
   The ouput should look like the following:
   ::

      ● thingsboard-gateway.service - ThingsBoard Gateway
           Loaded: loaded (/etc/systemd/system/thingsboard-gateway.service; disabled; vendor preset: enabled)
           Active: active (running) since Thu 2022-04-14 20:18:27 EDT; 1s ago
         Main PID: 16994 (python3)
            Tasks: 11 (limit: 8986)
              CPU: 935ms
           CGroup: /system.slice/thingsboard-gateway.service
                   └─16994 /usr/bin/python3 /home/pi/thingsboard-gateway/thingsboard-gateway/thingsboard_gateway/tb_gateway.py
#. Be sure to check there is no line in the status that contains :code:`|ERROR| - [tb_device_mqtt.py] - tb_device_mqtt - _on_connect - 146 - connection FAIL with error 5 not authorised"`. This error means the gateway failed to connect to ThingsBoard due to a bad access token.
#. Once everything is running smoothly, use the following command to send a telemetry packet to ThingsBoard: :code:`mosquitto_pub -d -q 1 -h "variot.ece.drexel.edu" -p "1883" -t "v1/devices/me/telemetry" -u "$ACCESS_TOKEN" -m {"temperature":25}`. Be sure to replace :code:`$ACCESS_TOKEN` with your access code.
#. Navigate to the devices panel of ThingsBoard, click on your gateway device, then the "Latest Telemetry" tab. If everything worked the way it should, you should see an entry with key "temperature" and value "25".
