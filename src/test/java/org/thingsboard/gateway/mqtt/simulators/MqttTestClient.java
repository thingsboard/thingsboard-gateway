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
package org.thingsboard.gateway.mqtt.simulators;

import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import nl.jk5.mqtt.MqttClient;
import nl.jk5.mqtt.MqttHandler;
import org.junit.After;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

/**
 * Created by Valerii Sosliuk on 5/9/2018.
 */
@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
@Data
public class MqttTestClient {

    private MqttClient mqttClient;

    @Value("${test.host:localhost}")
    private String host;
    @Value("${mqtt.broker.external.port:7883}")
    private int port;
    @Value("${mqtt.timeout:10000}")
    private long timeout;

    public MqttTestClient() {

    }

    @PostConstruct
    public void init() {
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

    @PreDestroy
    public void tearDown() {
        mqttClient.disconnect();
    }
}
