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
package org.thingsboard.gateway.mqtt;

import io.netty.buffer.ByteBuf;
import nl.jk5.mqtt.MqttHandler;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Valerii Sosliuk on 5/9/2018.
 */
public class TestMqttHandler implements MqttHandler {

    private Map<String, List<String>> payloads;

    public TestMqttHandler() {
        payloads = new HashMap<>();
    }

    @Override
    public void onMessage(String topic, ByteBuf payload) {
        List<String> payloads = this.payloads.get(topic);
        if (payloads == null) {
            payloads = new ArrayList<>();
            this.payloads.put(topic, payloads);
        }
        payloads.add(payload.toString(StandardCharsets.UTF_8));
    }

    public List<String> getValues(String topic) {
        return payloads.get(topic);
    }
}
