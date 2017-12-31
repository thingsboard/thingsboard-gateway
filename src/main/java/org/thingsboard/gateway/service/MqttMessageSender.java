/**
 * Copyright © 2017 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.gateway.service;

import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import nl.jk5.mqtt.MqttClient;
import org.thingsboard.gateway.dao.PersistentMqttMessage;
import org.thingsboard.gateway.dao.PersistentMqttMessageRepository;
import org.thingsboard.gateway.service.conf.TbPersistenceConfiguration;

import java.util.List;
import java.util.function.Consumer;

/**
 * Created by Valerii Sosliuk on 12/12/2017.
 */
@Slf4j
public class MqttMessageSender implements Runnable {

    private MqttClient tbClient;

    private MqttMessageService mqttSenderService;

    private TbPersistenceConfiguration persistence;

    private PersistentMqttMessageRepository messageRepository;

    private Consumer<Void> defaultSuccessCallback = message -> log.debug("Successfully sent message: [{}]", message);
    private Consumer<Throwable> defaultFailureCallback = e -> log.warn("Failed to send message: [{}]", e);

    public MqttMessageSender(TbPersistenceConfiguration persistence,
                             MqttClient tbClient,
                             MqttMessageService mqttSenderService,
                             PersistentMqttMessageRepository messageRepository) {
        this.persistence = persistence;
        this.tbClient = tbClient;
        this.mqttSenderService = mqttSenderService;
        this.messageRepository = messageRepository;
    }

    @Override
    public void run() {
        while (true) {
            try {
                List<PersistentMqttMessage> storedMessages = null;
                storedMessages = mqttSenderService.getMessages(persistence.getMaxMessagesPerPoll());
                if (!storedMessages.isEmpty()) {
                    for (PersistentMqttMessage message : storedMessages) {
                        messageRepository.delete(message);
                        tbClient.publish(message.getTopic(), Unpooled.wrappedBuffer(message.getPayload())).addListener(
                                future -> {
                                    if (future.isSuccess()) {
                                        Consumer<Void> successCallback = mqttSenderService.getSuccessCallback(message.getId()).orElse(defaultSuccessCallback);
                                        successCallback.accept(null);
                                        mqttSenderService.resolveFutureSuccess(message.getId());
                                    } else {
                                        messageRepository.save(new PersistentMqttMessage(message));
                                        mqttSenderService.getFailureCallback(message.getId()).orElse(defaultFailureCallback).accept(future.cause());
                                        mqttSenderService.resolveFutureFailed(message.getId(), future.cause());
                                        log.warn("Failed to send message [{}] due to [{}]", message, future.cause());
                                    }
                                }
                        );
                    }
                } else {
                    try {
                        Thread.sleep(persistence.getPollingInterval());
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                        Thread.currentThread().interrupt();
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

    }
}
