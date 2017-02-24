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
package org.thingsboard.gateway.extensions.mqtt.client;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.thingsboard.gateway.extensions.mqtt.client.conf.mapping.MqttDataConverter;
import org.thingsboard.gateway.service.data.DeviceData;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by ashvayka on 24.01.17.
 */
@Data
@Slf4j
public class MqttMessageListener implements IMqttMessageListener {

    private final Consumer<List<DeviceData>> consumer;
    private final MqttDataConverter converter;

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        try {
            consumer.accept(converter.convert(topic, message));
        } catch (Exception e) {
            log.info("[{}] Failed to decode message: {}", topic, Arrays.toString(message.getPayload()), e);
        }
    }
}
