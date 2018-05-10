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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.RegularExpressionValueMatcher;
import org.skyscreamer.jsonassert.comparator.CustomComparator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.thingsboard.gateway.AbstractGatewayIntegrationTest;
import org.thingsboard.gateway.extensions.mqtt.client.MqttBrokerMonitor;
import org.thingsboard.gateway.rules.AbstractGatewayTestRule;
import org.thingsboard.gateway.rules.MqttBrokerRule;
import org.thingsboard.gateway.rules.MqttClientRule;
import org.thingsboard.gateway.rules.TbSimulatorRule;
import org.thingsboard.gateway.util.IoUtils;
import org.thingsboard.gateway.util.JsonUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@SpringBootTest()
public class MqttIntegrationTest extends AbstractGatewayIntegrationTest {

    private static final int EXTERNAL_BROKER_PORT = 7883;
    private static final long MQTT_TIMEOUT = 1000;
    private static final int TB_BROKER_PORT = 7884;
    private static final String LOCALHOST = "localhost";

    @Rule
    public MqttBrokerRule externalBroker = new MqttBrokerRule(LOCALHOST, EXTERNAL_BROKER_PORT, true);

    @Rule
    public MqttClientRule deviceSimulator = new MqttClientRule(LOCALHOST, EXTERNAL_BROKER_PORT, MQTT_TIMEOUT);

    @Rule
    public TbSimulatorRule tbSimulator = new TbSimulatorRule(LOCALHOST, TB_BROKER_PORT, MQTT_TIMEOUT);

    @Test
    public void testSendAndReceiveSimpleJson() throws IOException, InterruptedException, JSONException {

        try {
            deviceSimulator.publish("sensor/SN-001/temperature", "{\"value\":\"39\"}".getBytes(), 0);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        Thread.sleep(10000);
        List<String> recievedConnectMessage = tbSimulator.getValues("v1/gateway/connect");
        assertNotNull("recievedConnectMessage was expected to be non-null", recievedConnectMessage);
        assertEquals("Only one connect message was expected", 1, recievedConnectMessage.size());
        assertEquals(IoUtils.getResourceAsString("mqtt/connect-SN-001.json"), recievedConnectMessage.get(0));

        List<String> recievedTelemetryMessage = tbSimulator.getValues("v1/gateway/telemetry");
        assertNotNull("recievedTelemetryMessage was expected to be non-null", recievedTelemetryMessage);
        assertEquals("Only one telemetry message was expected", 1, recievedTelemetryMessage.size());

        String expectedTelemetryJson = IoUtils.getResourceAsString("mqtt/single-telemetry-value.json");
        String actualTelemetryJson = recievedTelemetryMessage.get(0);

        JsonUtils.assertWithoutTimestamp("SN-001", expectedTelemetryJson, actualTelemetryJson);
    }
}
