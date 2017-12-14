package org.thingsboard.gateway.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;

import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by Valerii Sosliuk on 12/12/2017.
 */
@Slf4j
@AllArgsConstructor
public class MqttMessageSender implements Runnable {

    private PriorityBlockingQueue<MqttMessageWrapper> mqttMessageQueue;
    private MqttAsyncClient tbClient;
    private long retryInterval;
    private int maxQueueSize;

    @Override
    public void run() {
        while (true) {
            try {
                final MqttMessageWrapper messageWrapper = mqttMessageQueue.take();
                try {
                    tbClient.publish(messageWrapper.getTopic(), messageWrapper.getMsg(), null, new IMqttActionListener() {
                        @Override
                        public void onSuccess(IMqttToken asyncActionToken) {
                            messageWrapper.getOnSuccess().accept(asyncActionToken);
                        }

                        @Override
                        public void onFailure(IMqttToken asyncActionToken, Throwable e) {
                            messageWrapper.getOnFailure().accept(e);
                        }
                    });
                } catch (MqttException e) {
                    log.error(e.getMessage(), e);
                    if (mqttMessageQueue.size() < maxQueueSize) {
                        mqttMessageQueue.add(messageWrapper);
                    }
                    Thread.sleep(retryInterval);
                }
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
                Thread.currentThread().interrupt();
            }

        }
    }
}
