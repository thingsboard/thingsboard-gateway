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
import org.thingsboard.gateway.service.MqttDeliveryFuture;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Created by Valerii Sosliuk on 1/2/2018.
 */
public interface PersistentFileService {

    MqttDeliveryFuture persistMessage(String topic, int msgId, byte[] payload, String deviceId,
                                      Consumer<Void> onSuccess, Consumer<Throwable> onFailure) throws IOException;

    /**
     * Returns a list of the messages that are to be sent. If storage files exist, then the messages from oldest
     * storage file are returned and file is deleted. If no storage file exists, then returns the messages that are currently
     * in storage buffer and clears it
     * @return {@see List} of {@see MqttPersistentMessage} to be sent
     * @throws IOException
     */
    List<MqttPersistentMessage> getPersistentMessages() throws IOException;

    /**
     * Returns a list of the messages that are to be re-sent
     * @return {@see List} of {@see MqttPersistentMessage} to be re-sent
     * @throws IOException
     */
    List<MqttPersistentMessage> getResendMessages() throws IOException;

    void resolveFutureSuccess(UUID id);

    void resolveFutureFailed(UUID id, Throwable e);

    Optional<MqttDeliveryFuture> getMqttDeliveryFuture(UUID id);

    boolean deleteMqttDeliveryFuture(UUID id);

    Optional<Consumer<Void>> getSuccessCallback(UUID id);

    Optional<Consumer<Throwable>> getFailureCallback(UUID id);

    void saveForResend(MqttPersistentMessage message) throws IOException;

    void saveForResend(List<MqttPersistentMessage> messages) throws IOException;
}
