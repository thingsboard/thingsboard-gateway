Install ThingsBoard Gateway from Source
=======================================

Generate an SSH Key
-------------------
#. Generate ssh key on pi using ssh-keygen
#. :code:`cd .ssh` - Visit the directory where your SSH key can now be found
#. :code:`cat id_rsa.pub` - Display the public key. Copy it.
#. Go to the `GitHub <https://github.com/>`_ and log in with the account associated with the gateway repository
#. Navigate to *Settings* > *SSH and GPG keys* > *New SSH Key*
#. Paste the public key in the Key box

Running ThingsBoard Gateway
---------------------------
#. :code:`cd ~` - Move to the home directory of the gateway
#. Follow the steps laid out in the `ThingsBoard Gateway Source Installation Instructions <https://thingsboard.io/docs/iot-gateway/install/source-installation/>`_

   * Step 2 - Instead perform :code:`git clone --single-branch --branch thingsboard-master git@github.com:drexelwireless/thingsboard-gateway.git`
   * Step 4 - Be sure to use :code:`sudo python3 setup.py install` instead
#. In order to run your gateway from the source, you need to add :code:`thingsboard-gateway.service` in :code:`/etc/systemd/system directory`

   * :code:`cd /etc/systemd/system` - Move to the directory the service file will live in
   * :code:`cp ~/setup_files/thingsboard-gateway.service .` - Copy the service file to the directory you just moved to
#. :code:`sudo systemctl daemon-reload` -
#. :code:`sudo systemctl start thingsboard-gateway.service` - Start ThingsBoard Gateway
#. :code:`sudo journalctl -u thingsboard-gateway.service -f` - Check the status of ThingsBoard Gateway


