#!/usr/bin/env python3
import paho.mqtt.client as mqtt

# Function Setup
TEMP_ACCESS_TOKEN = "LBkmUN20TdUNE5SZ3E1j"
def on_connect_local_temp(client, userdata, flags, rc):
    print("Temperature Connected with result code "+str(rc))
    client.subscribe("zigbee2mqtt/0x00158d0006bcd386")

def on_message_temp(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    temp_thingsboard_client.publish(f"v1/devices/me/telemetry", payload=msg.payload)
    print("Published message")


MOTION_ACCESS_TOKEN = "LjMfJgL9la3yjvSkGGmR"
def on_connect_local_motion(client, userdata, flags, rc):
    print("Motion Connected with result code "+str(rc))
    client.subscribe("zigbee2mqtt/0x00158d0002a3516b")

def on_message_motion(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    motion_thingsboard_client.publish(f"v1/devices/me/telemetry", payload=msg.payload)
    print("Published message")
    
ILLUMINANCE_ACCESS_TOKEN = "21ZRM6w1WlwiwsOeZ7Gf"
def on_connect_local_illuminance(client, userdata, flags, rc):
    print("Illuminance Connected with result code "+str(rc))
    client.subscribe("zigbee2mqtt/0x00158d000365879b")

def on_message_illuminance(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    illuminance_thingsboard_client.publish(f"v1/devices/me/telemetry", payload=msg.payload)
    print("Published message")

# Mosquitto Connections
local_client_temp = mqtt.Client()
local_client_temp.on_connect = on_connect_local_temp
local_client_temp.on_message = on_message_temp
local_client_temp.connect("localhost", 1883, 60)

local_client_motion = mqtt.Client()
local_client_motion.on_connect = on_connect_local_motion
local_client_motion.on_message = on_message_motion
local_client_motion.connect("localhost", 1883, 60)

local_client_illuminance = mqtt.Client()
local_client_illuminance.on_connect = on_connect_local_illuminance
local_client_illuminance.on_message = on_message_illuminance
local_client_illuminance.connect("localhost", 1883, 60)

# ThingsBoard Connections
temp_thingsboard_client = mqtt.Client()
temp_thingsboard_client.username_pw_set(TEMP_ACCESS_TOKEN)
temp_thingsboard_client.connect("variot.ece.drexel.edu", 1883, 60)

motion_thingsboard_client = mqtt.Client()
motion_thingsboard_client.username_pw_set(MOTION_ACCESS_TOKEN)
motion_thingsboard_client.connect("variot.ece.drexel.edu", 1883, 60)

illuminance_thingsboard_client = mqtt.Client()
illuminance_thingsboard_client.username_pw_set(ILLUMINANCE_ACCESS_TOKEN)
illuminance_thingsboard_client.connect("variot.ece.drexel.edu", 1883, 60)

# Loop Starts
local_client_temp.loop_start()
temp_thingsboard_client.loop_start()

local_client_motion.loop_start()
motion_thingsboard_client.loop_start()

local_client_illuminance.loop_start()
illuminance_thingsboard_client.loop_start()

try:
    while True:
        pass
except KeyboardInterrupt:
    pass

local_client_temp.loop_stop()
temp_thingsboard_client.loop_stop()

local_client_motion.loop_stop()
motion_thingsboard_client.loop_stop()

local_client_illuminance.loop_stop()
illuminance_thingsboard_client.loop_stop()
