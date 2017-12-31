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

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.thingsboard.gateway.dao.PersistentMqttMessage;
import org.thingsboard.gateway.dao.PersistentMqttMessageRepository;
import org.thingsboard.gateway.service.conf.TbPersistenceConfiguration;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * Created by Valerii Sosliuk on 12/28/2017.
 */
@Service
@Slf4j
public class MqttMessageServiceImpl implements MqttMessageService {

    @Autowired
    private PersistentMqttMessageRepository messageRepository;

    @Autowired
    private TbPersistenceConfiguration persistence;

    private ConcurrentMap<UUID, MqttCallbackWrapper> callbacks;
    private Map<UUID, MqttDeliveryFuture> futures;

    @PostConstruct
    public void init() throws Exception {
        callbacks = new ConcurrentHashMap<>();
        futures = new ConcurrentHashMap<>();
    }

    @Override
    public List<PersistentMqttMessage> getMessages(int limit) {
        return messageRepository.findAll(new PageRequest(0, persistence.getMaxMessagesPerPoll(), Sort.Direction.ASC, "timestamp")).getContent();
    }

    @Override
    public MqttDeliveryFuture sendMessage(String topic,
                                          MqttMessage msg,
                                          String deviceId,
                                          Consumer<Void> onSuccess,
                                          Consumer<Throwable> onFailure) {

        PersistentMqttMessage mqttMessage = PersistentMqttMessage.builder().id(null).timestamp(System.currentTimeMillis())
                .deviceId(deviceId).messageId(msg.getId()).payload(msg.getPayload()).topic(topic).build();
        mqttMessage = messageRepository.save(mqttMessage);
        MqttDeliveryFuture future = new MqttDeliveryFuture();
        callbacks.put(mqttMessage.getId(), new MqttCallbackWrapper(onSuccess, onFailure));
        futures.put(mqttMessage.getId(), future);
        return future;
    }

    @Override
    public void resolveFutureSuccess(UUID id) {
        callbacks.remove(id);
        MqttDeliveryFuture future = futures.remove(id);
        if (future != null) {
            future.complete(Boolean.FALSE);
        }
    }

    @Override
    public void resolveFutureFailed(UUID id, Throwable e) {
        MqttDeliveryFuture future = futures.remove(id);
        if (future != null) {
            future.completeExceptionally(e);
        }
    }

    @Override
    public Optional<MqttDeliveryFuture> getMqttDeliveryFuture(UUID id) {
        return Optional.of(futures.get(id));
    }

    @Override
    public boolean deleteMqttDeliveryFuture(UUID id) {
        return futures.remove(id) != null;
    }

    @Override
    public Optional<Consumer<Void>> getSuccessCallback(UUID id) {
        MqttCallbackWrapper mqttCallbackWrapper = callbacks.get(id);
        if (mqttCallbackWrapper == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(mqttCallbackWrapper.getSuccessCallback());
    }

    @Override
    public Optional<Consumer<Throwable>> getFailureCallback(UUID id) {
        MqttCallbackWrapper mqttCallbackWrapper = callbacks.get(id);
        if (mqttCallbackWrapper == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(mqttCallbackWrapper.getFailureCallback());
    }

}
