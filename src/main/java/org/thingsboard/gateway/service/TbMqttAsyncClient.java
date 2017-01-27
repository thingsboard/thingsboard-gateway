package org.thingsboard.gateway.service;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttPublish;

/**
 * Created by ashvayka on 26.01.17.
 */
@Slf4j
public class TbMqttAsyncClient extends MqttAsyncClient {

    public TbMqttAsyncClient(String serverURI, String clientId, MqttClientPersistence persistence) throws MqttException {
        super(serverURI, clientId, persistence);
    }

    public IMqttDeliveryToken publish(String topic, MqttMessage message, Object userContext, IMqttActionListener callback) throws MqttException,
            MqttPersistenceException {
        final String methodName = "publish";

        //Checks if a topic is valid when publishing a message.
        MqttTopic.validate(topic, false/*wildcards NOT allowed*/);

        TbMqttDeliveryToken token = new TbMqttDeliveryToken(getClientId());
        token.setActionCallback(callback);
        token.setUserContext(userContext);
        token.setMessage(message);
        token.internalTok.setTopics(new String[] {topic});


        MqttPublish pubMsg = new MqttPublish(topic, message);
        pubMsg.setMessageId(message.getId());
        comms.sendNoWait(pubMsg, token);


        return token;
    }

}
