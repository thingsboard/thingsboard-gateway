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

import nl.jk5.mqtt.MqttClient;
import org.junit.After;
import org.thingsboard.gateway.mqtt.TestMqttHandler;

import java.util.List;

/**
 * Created by Valerii Sosliuk on 6/9/2018.
 */
public class TbSimulatorRule extends AbstractGatewayTestRule {

    private MqttBrokerRule brokerRule;
    private MqttClientRule clientRule;
    TestMqttHandler mqttHandler;

    public TbSimulatorRule(String host, int port, long timeout) {
        brokerRule = new MqttBrokerRule(host, port, true);
        clientRule = new MqttClientRule(host, port, timeout);
        mqttHandler = new TestMqttHandler();
        clientRule.subscribe("v1/devices/me/attributes/request/1", mqttHandler);
        clientRule.subscribe("v1/devices/me/telemetry", mqttHandler);
        clientRule.subscribe("v1/gateway/connect", mqttHandler);
        clientRule.subscribe("v1/gateway/telemetry", mqttHandler);
    }

    @After
    public void tearDown() {
        clientRule.tearDown();
        brokerRule.tearDown();
    }

    public List<String> getValues(String topic) {
        return mqttHandler.getValues(topic);
    }
}
