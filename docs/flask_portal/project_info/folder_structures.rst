Folder Structures
=================

Files on Root Level
-------------------
* :code:`.gitignore`: contains files or folders that are not source controlled.
* :code:`README.md`: contains installation steps and how the project is hosted on the Barista server.
* :code:`requirements.txt`: contains project dependencies (libraries used in this project), should be recreated whenever there is a new dependency (new library installed) by using the following command :code:`pip freeze > requirements.txt`

Backend
-------
Backend folder contains files needed to control backend logics.

Folder level files
^^^^^^^^^^^^^^^^^^
* :code:`config.py`: When you add a new python script handling routing in Flask, you need import the new file.
* :code:`restClient.py`: used to make API calls to VarIoT ThingsBoard (variot.ece.drexel.edu)
* Other files: control routing in Flask project

model
^^^^^
Model directory contains device models (classes) for protocol specific protocols.

scripts
^^^^^^^
Shell scripts used to retrieve and push ble.json file from and to Barista server. 

public
------
* css: css files
* img: logo
* js: javascript files that control web socket and dynamic form logic
* json: JSON files containing protocol specific questions to show on `Add Device` form

temp
----
It contains JSON files temporarily used when adding a BLE device. It should not be sourced controlled.

templates
---------
This is where all html files live

variot_portal_env
-----------------
This is where files related to the Python environment you set during installation live.