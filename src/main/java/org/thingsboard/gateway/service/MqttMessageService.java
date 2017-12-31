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
                                   Consumer<Void> onSuccess, Consumer<Throwable> onFailure);

    void resolveFutureSuccess(UUID id);

    void resolveFutureFailed(UUID id, Throwable e);

    Optional<MqttDeliveryFuture> getMqttDeliveryFuture(UUID id);

    boolean deleteMqttDeliveryFuture(UUID id);

    Optional<Consumer<Void>> getSuccessCallback(UUID id);

    Optional<Consumer<Throwable>> getFailureCallback(UUID id);
}
