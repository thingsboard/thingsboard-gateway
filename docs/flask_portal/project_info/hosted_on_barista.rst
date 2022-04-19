How it is hosted on Barista
=================

#. Repo in :code:`/var/www/VarIOT_Portal``
#. Created VarIOT_Portal.service in :code:`/etc/systemd/system`` directory
#. Executed following

.. code-block:: sh

  sudo systemctl daemon-reload
  sudo systemctl enable VarIOT_Portal.service
  sudo systemctl start VarIOT_Portal.service

#. To check the status, use the following command

.. code-block:: sh

  sudo systemctl status VarIOT_Portal.service

or

.. code-block:: sh

  sudo journalctl -u VarIOT_Portal.service -f