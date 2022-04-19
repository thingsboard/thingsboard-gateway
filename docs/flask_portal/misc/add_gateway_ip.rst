Add Gateway IP Address
======================

This page has instructions on how to add an IP address of a newly added gateway.

Steps to Add
------------
#. Open :code:`templates\add_device.html`
#. Look for the :code:`select` tag with id of :code:`gateway`
#. Add an :code:`option` tag with the following convention :code:`<option value='Gateway_IP_Address'>Gateway Name</option>`

TO DO
-----
The way IP address is handled should be updated so that IP addresses are not exposed. Ideally, all IP addresses should be saved in a config that only lives locally and dynamically create a dropdown menu.