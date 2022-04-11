.. _get-personal-access-token:

Get ThingsBoard Personal Access Token
=====================================

In order to perform some API commands such as getting the latest telemetry values for a device, you will need to find
your JWT (JSON Web Token). To do so, perform the following curl command in your command prompt:

.. code-block:: console

   curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d '{"username":"<ENTER USERNAME HERE>", "password":"<ENTER PASSWORD HERE>"}' 'http://variot.ece.drexel.edu/api/auth/login'

Where:

* "username" is your ThingsBoard username
* "password" is your ThingsBoard password

Your response should look something like this:

.. code-block:: console

   {"token":"<TOKEN VALUE HERE>","refreshToken":"<REFRESH TOKEN VALUE HERE>"}

