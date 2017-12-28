package org.thingsboard.gateway.service;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.thingsboard.gateway.dao.PersistentMqttMessage;
import org.thingsboard.gateway.dao.PersistentMqttMessageRepository;
import org.thingsboard.gateway.service.conf.TbPersistenceConfiguration;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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

    private Map<UUID, Consumer<PersistentMqttMessage>> successCallbacks;
    private Map<UUID, Consumer<Throwable>> failureCallbacks;
    private Map<UUID, MqttDeliveryFuture> futures;

    @PostConstruct
    public void init() throws Exception {
        successCallbacks = new ConcurrentHashMap<>();
        failureCallbacks = new ConcurrentHashMap<>();
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
                                          Consumer<PersistentMqttMessage> onSuccess,
                                          Consumer<Throwable> onFailure) {

        PersistentMqttMessage mqttMessage = PersistentMqttMessage.builder().id(null).timestamp(System.currentTimeMillis())
                .deviceId(deviceId).messageId(msg.getId()).payload(msg.getPayload()).topic(topic).build();
        messageRepository.save(mqttMessage);
        return null;
    }

    @Override
    public void resolveFutureSuccess(UUID id) {
        successCallbacks.remove(id);
        failureCallbacks.remove(id);
        // TODO: replace MqttDeliveryFuture with Promise and resolve it here as successful
        MqttDeliveryFuture future = futures.remove(id);
    }

    @Override
    public void resolveFutureFailed(UUID id) {
        // TODO: replace MqttDeliveryFuture with Promise and resolve it here as failed
        MqttDeliveryFuture future = futures.remove(id);
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
    public Optional<Consumer<PersistentMqttMessage>> getSuccessCallback(UUID id) {
        return Optional.ofNullable(successCallbacks.get(id));
    }

    @Override
    public Optional<Consumer<Throwable>> getFailureCallback(UUID id) {
        return Optional.ofNullable(failureCallbacks.get(id));
    }

}
