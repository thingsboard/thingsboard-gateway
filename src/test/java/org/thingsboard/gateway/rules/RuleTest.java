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

/**
 * Created by Valerii Sosliuk on 5/7/2018.
 */

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thingsboard.gateway.AbstractGatewayIntegrationTest;
import org.thingsboard.gateway.mqtt.TestMqttHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class RuleTest extends AbstractGatewayIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(RuleTest.class);
    public static final String PAYLOAD = "{\"value\":\"39\"}";
    public static final String TOPIC = "/topic";
    public static final int EXTERNAL_BROKER_PORT = 7882;
    public static final int MQTT_TIMEOUT = 3000;

    @Rule
    public MqttBrokerRule externalBroker = new MqttBrokerRule("0.0.0.0", EXTERNAL_BROKER_PORT, true);

    @Rule
    public MqttClientRule receiverClient = new MqttClientRule("localhost", EXTERNAL_BROKER_PORT, MQTT_TIMEOUT);

    @Rule
    public MqttClientRule senderClient = new MqttClientRule("localhost", EXTERNAL_BROKER_PORT, MQTT_TIMEOUT);

    @Test
    public void testCleanSession_maintainClientSubscriptions() throws Exception {
        LOG.info("*** testCleanSession_maintainClientSubscriptions ***");
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        TestMqttHandler mqttHandler = new TestMqttHandler();
        receiverClient.subscribe("/topic", mqttHandler);

        senderClient.publish(TOPIC, PAYLOAD.getBytes(), 0);
        Thread.sleep(100);
        assertNotNull("No values were found for topic [" + TOPIC + "] when expected", mqttHandler.getValues(TOPIC));
        assertEquals(1, mqttHandler.getValues(TOPIC).size());
        assertEquals(PAYLOAD, mqttHandler.getValues(TOPIC).get(0));
    }

}