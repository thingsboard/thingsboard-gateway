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
package org.thingsboard.gateway.extensions.mqtt.client.listener;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.thingsboard.gateway.service.data.RpcCommandResponse;

import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

/**
 * Created by ashvayka on 07.03.17.
 */
@Data
@Slf4j
public class MqttRpcResponseMessageListener implements IMqttMessageListener {

    private final int requestId;
    private final String deviceName;
    private final BiConsumer<String, RpcCommandResponse> consumer;

    @Override
    public void messageArrived(String topic, MqttMessage msg) throws Exception {
        RpcCommandResponse response = new RpcCommandResponse();
        response.setRequestId(requestId);
        response.setDeviceName(deviceName);
        response.setData(new String(msg.getPayload(), StandardCharsets.UTF_8));
        consumer.accept(topic, response);
    }
}
