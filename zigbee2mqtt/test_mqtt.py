#!/usr/bin/env python3
import paho.mqtt.client as mqtt

TEMP_ACCESS_TOKEN = "RTkN61pnPIb7Hp4PtdRJ"
MOTION_ACCESS_TOKEN = "UIjn08YIkHjfE4jzF8N3"

# The callback for when the client receives a CONNACK response from the server
def on_connect_local_temp(client, userdata, flags, rc):
    print("Temperature Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("zigbee2mqtt/0x00158d00044f0657")

# The callback for when the client receives a CONNACK response from the server
def on_connect_local_motion(client, userdata, flags, rc):
    print("Motion Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("zigbee2mqtt/0x00158d0002a3516b")

# The callback for when a PUBLISH message is received from the server
def on_message_temp(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    temp_thingsboard_client.publish(f"v1/devices/me/telemetry", payload=msg.payload)
    print("Published message")

# The callback for when a PUBLISH message is received from the server
def on_message_motion(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    motion_thingsboard_client.publish(f"v1/devices/me/telemetry", payload=msg.payload)
    print("Published message")

local_client_temp = mqtt.Client()
local_client_temp.on_connect = on_connect_local_temp
local_client_temp.on_message = on_message_temp
local_client_temp.connect("localhost", 1883, 60)

local_client_motion = mqtt.Client()
local_client_motion.on_connect = on_connect_local_motion
local_client_motion.on_message = on_message_motion
local_client_motion.connect("localhost", 1883, 60)

temp_thingsboard_client = mqtt.Client()
temp_thingsboard_client.username_pw_set(TEMP_ACCESS_TOKEN)
temp_thingsboard_client.connect("variot.ece.drexel.edu", 1883, 60)

motion_thingsboard_client = mqtt.Client()
motion_thingsboard_client.username_pw_set(MOTION_ACCESS_TOKEN)
motion_thingsboard_client.connect("variot.ece.drexel.edu", 1883, 60)

local_client_temp.loop_start()
local_client_motion.loop_start()
temp_thingsboard_client.loop_start()
motion_thingsboard_client.loop_start()

try:
    while True:
        pass
except KeyboardInterrupt:
    pass

local_client_temp.loop_stop()
local_client_motion.loop_stop()
temp_thingsboard_client.loop_stop()
motion_thingsboard_client.loop_stop()