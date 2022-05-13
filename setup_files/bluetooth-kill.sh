#!/bin/bash
for device in $(hcitool con | grep -o "[[:xdigit:]:]\{8,17\}"); do
    bluetoothctl disconnect $device
done