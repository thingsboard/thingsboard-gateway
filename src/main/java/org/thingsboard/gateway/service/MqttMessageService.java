package org.thingsboard.gateway.service;

import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.thingsboard.gateway.dao.PersistentMqttMessage;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Created by Valerii Sosliuk on 12/28/2017.
 */
public interface MqttMessageService {

    List<PersistentMqttMessage> getMessages(int limit);

    MqttDeliveryFuture sendMessage(final String topic, MqttMessage msg, String deviceId,
                                   Consumer<PersistentMqttMessage> onSuccess, Consumer<Throwable> onFailure);

    void resolveFutureSuccess(UUID id);

    void resolveFutureFailed(UUID id);

    Optional<MqttDeliveryFuture> getMqttDeliveryFuture(UUID id);

    boolean deleteMqttDeliveryFuture(UUID id);

    Optional<Consumer<PersistentMqttMessage>> getSuccessCallback(UUID id);

    Optional<Consumer<Throwable>> getFailureCallback(UUID id);
}
