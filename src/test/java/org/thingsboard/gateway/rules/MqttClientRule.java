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
package org.thingsboard.gateway.rules;

import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.extern.slf4j.Slf4j;
import nl.jk5.mqtt.MqttClient;
import nl.jk5.mqtt.MqttHandler;
import org.junit.After;

import java.util.concurrent.TimeUnit;

/**
 * Created by Valerii Sosliuk on 5/9/2018.
 */
@Slf4j
public class MqttClientRule extends AbstractGatewayTestRule {

    private final long timeout;
    private MqttClient mqttClient;

    public MqttClientRule (String host, int port, long timeout) {
        this.timeout = timeout;
        mqttClient = MqttClient.create();
        mqttClient.setEventLoop(new NioEventLoopGroup());
        try {
            mqttClient.connect(host, port).get(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
           log.error(e.getMessage(), e);
        }
    }


    public void subscribe(String topic, MqttHandler handler) {
        try {
            mqttClient.on(topic, handler).get(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void publish(String topic, byte[] payload, int qos) {
        mqttClient.publish(topic, Unpooled.wrappedBuffer(payload), MqttQoS.valueOf(qos));
    }

    @After
    public void tearDown() {
        mqttClient.disconnect();
    }
}
