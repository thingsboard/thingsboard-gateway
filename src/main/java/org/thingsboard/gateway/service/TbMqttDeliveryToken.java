package org.thingsboard.gateway.service;

import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * Created by ashvayka on 26.01.17.
 */
public class TbMqttDeliveryToken extends MqttDeliveryToken {

    public TbMqttDeliveryToken(String logContext) {
        super(logContext);
    }

    protected void setMessage(MqttMessage msg) {
        super.setMessage(msg);
    }
}
