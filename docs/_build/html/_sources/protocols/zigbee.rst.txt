ZigBee Gateway Setup
====================

NOTE: Many of these instructions are lifted from the
`official zigbee2mqtt setup instructions <https://www.zigbee2mqtt.io/guide/getting-started/>`_ and
consolidated here.

Hardware Setup
--------------

This guide assumes the user has a `Texas Instruments LAUNCHXL-CC26X2R1 <https://www.ti.com/tool/LAUNCHXL-CC26X2R1>`_,
which will act as the coordinator for the gateway's ZigBee network. You will need to flash the adapter with ZigBee
coordinator firmware. This should be done from your laptop/desktop/ a system separate from your gateway.

#. Download `coordinator firmware <https://github.com/Koenkk/Z-Stack-firmware/raw/master/coordinator/Z-Stack_3.x.0/bin/CC2652R_coordinator_20220219.zip>`_ for the TI Launchpad

   * Firmware path and filename should not contain any spaces (otherwise flashing may fail with "Error! Unable to open file ...").

#. Download and install `UNIFLASH <http://www.ti.com/tool/download/UNIFLASH>`_

#. Connect your LAUNCHXL-CC26X2R1 to your laptop/desktop/system via USB

#. Open UNIFLASH. UNIFLASH should automatically detect your device, it should appear under "Detected Devices" as "CC26X2R1 Launchpad". Press "Start"

#. Go to *Settings & Utilities* -> *Manual Erase* and press *Erase Entire Flash*

#. Go to *Settings & Utilities* -> *Program Load* and select *All Unprotected Sectors*, click *Perform Blank Check*

#. Go to *Program* -> *Flash Image(s)*, press *Browse* to select the firmware.

#. Click *Load image* to upload the firmware

Software Setup
--------------

#.