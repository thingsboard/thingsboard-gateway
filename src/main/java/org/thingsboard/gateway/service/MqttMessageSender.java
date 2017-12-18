package org.thingsboard.gateway.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;

/**
 * Created by Valerii Sosliuk on 12/12/2017.
 */
@Slf4j
@AllArgsConstructor
public class MqttMessageSender implements Runnable {

    private PersistentQueue<MqttMessageWrapper> queue;
    private MqttAsyncClient tbClient;
    private long retryInterval;

    @Override
    public void run() {
        while (true) {
            if (!queue.isEmpty()) {
                MqttMessageWrapper messageWrapper = queue.removeFirst();
                try {
                    tbClient.publish(messageWrapper.getTopic(), new MqttMessage(messageWrapper.getPayload()), null, new IMqttActionListener() {
                        @Override
                        public void onSuccess(IMqttToken asyncActionToken) {
                            log.info("Message send succeded");
                            //messageWrapper.getOnSuccess().accept(asyncActionToken);
                        }

                        @Override
                        public void onFailure(IMqttToken asyncActionToken, Throwable e) {
                            log.info("Message send failed");
                           // messageWrapper.getOnFailure().accept(e);
                        }
                    });
                }  catch (MqttException e) {
                    log.error(e.getMessage(), e);
                    queue.add(messageWrapper);
                }
            } else {
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
