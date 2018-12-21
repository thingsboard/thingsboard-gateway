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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.thingsboard.gateway.mqtt.TestMqttHandler;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;

/**
 * Created by Valerii Sosliuk on 6/9/2018.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 2)
public class TbSimulator {

    private MqttBroker brokerRule;
    private MqttTestClient clientRule;


    @Value("${test.host:localhost}")
    private String host;
    @Value("${mqtt.broker.tb.port:7884}")
    private int port;
    @Value("${mqtt.broker.tb.allowAnonymous:true}")
    private boolean allowAnonymous;
    @Value("${mqtt.timeout:10000}")
    private long timeout;


    @Autowired
    private TestMqttHandler mqttHandler;

    public TbSimulator() {

    }

    @PostConstruct
    public void init() {
        // TODO: replace with bean injection
        brokerRule = new MqttBroker();
        brokerRule.setHost(host);
        brokerRule.setPort(port);
        brokerRule.setAllowAnonymous(true);
        brokerRule.init();
        clientRule = new MqttTestClient(mqttHandler);
        clientRule.setHost(host);
        clientRule.setPort(port);
        clientRule.setTimeout(timeout);
        clientRule.init();
        clientRule.subscribe("v1/devices/me/attributes/request/1", mqttHandler);
        clientRule.subscribe("v1/devices/me/telemetry", mqttHandler);
        clientRule.subscribe("v1/gateway/connect", mqttHandler);
        clientRule.subscribe("v1/gateway/telemetry", mqttHandler);
    }

    @PreDestroy
    public void tearDown() {
        clientRule.tearDown();
        brokerRule.tearDown();
    }

    public List<String> getValues(String topic) {
        return mqttHandler.getValues(topic);
    }

    public TestMqttHandler getMqttHandler() {
        return mqttHandler;
    }

    public void setMqttHandler(TestMqttHandler mqttHandler) {
        this.mqttHandler = mqttHandler;
    }
}
