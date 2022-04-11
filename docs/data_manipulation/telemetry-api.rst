Get Latest Telemetry Values via API
===================================

#. Get your :ref:`personal access token<get-personal-access-token>`
#. Get your Device ID

   * In ThingsBoard, go to *Devices*, select your device, and click *Copy device id*

.. image:: images/get-device-id.JPG
  :width: 800
  :alt: Get Device ID

3. Perform the following curl command in your command prompt:

.. code-block:: console

   curl -v -X GET http://variot.ece.drexel.edu:8080/api/plugins/telemetry/DEVICE/<DEVICE ID>/values/timeseries?keys=<TELEMETRY FIELDS> --header "Content-Type:application/json" --header "X-Authorization:Bearer <PERSONAL ACCESS TOKEN>"

Where:

* *DEVICE ID* is the Device ID you received in Step 2
* *TELEMETRY FIELDS* are the telemetry fields you wish to receive in the form :code:`key1,key2,key3`
* *PERSONAL ACCESS TOKEN* is the token you received in Step 1

The response should look something like this:

.. code-block:: JSON

   {"<KEY>":[{"ts":1649704161368,"value":"425.534"}]}


For more on working with telemetry data, review the
`Time-series data section <https://thingsboard.io/docs/user-guide/telemetry/#get-latest-time-series-data-values-for-specific-entity>`_ of the ThingsBoard documentation