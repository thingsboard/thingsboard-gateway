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

import org.json.JSONException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.RegularExpressionValueMatcher;
import org.skyscreamer.jsonassert.comparator.CustomComparator;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.thingsboard.gateway.AbstractGatewayIntegrationTest;
import org.thingsboard.gateway.rules.MqttBrokerRule;
import org.thingsboard.gateway.rules.MqttClientRule;
import org.thingsboard.gateway.rules.TbSimulatorRule;
import org.thingsboard.gateway.util.IoUtils;
import org.thingsboard.gateway.util.JsonUtils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@SpringBootTest()
@ActiveProfiles("mqtt-two-filters")
public class MqttTwoFiltersIntegrationTest extends AbstractGatewayIntegrationTest {

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
    public void testMqttTwoFiltersOnSameTopic() throws Exception {
        deviceSimulator.publish("sensor/1110", IoUtils.getResourceAsString("mqtt/mqtt-1110-publish.json").getBytes(), 0);
        deviceSimulator.publish("sensor/1111", IoUtils.getResourceAsString("mqtt/mqtt-1111-publish.json").getBytes(), 0);

        Thread.sleep(10000);
        List<String> receivedConnectMessages = tbSimulator.getValues("v1/gateway/connect");
        assertNotNull("recievedConnectMessage was expected to be non-null", receivedConnectMessages);

        assertEquals("Exactly 2 connect messages were expected", 2, receivedConnectMessages.size());
        assertEquals(IoUtils.getResourceAsString("mqtt/connect-1110.json"), receivedConnectMessages.get(0));
        assertEquals(IoUtils.getResourceAsString("mqtt/connect-1111.json"), receivedConnectMessages.get(1));

        List<String> recievedTelemetryMessages = tbSimulator.getValues("v1/gateway/telemetry");
        assertNotNull("recievedTelemetryMessage was expected to be non-null", recievedTelemetryMessages);
        assertEquals("Exactly 2 telemetry message were expected", 2, recievedTelemetryMessages.size());

        String expectedTelemetry1110 = IoUtils.getResourceAsString("mqtt/mqtt-1110-result.json");
        String actualTelemetry1110 = recievedTelemetryMessages.get(0);

        JsonUtils.assertWithoutTimestamp("Device 1110", expectedTelemetry1110, actualTelemetry1110);

        String expectedTelemetry1111 = IoUtils.getResourceAsString("mqtt/mqtt-1111-result.json");
        String actualTelemetry1111 = recievedTelemetryMessages.get(1);

        JsonUtils.assertWithoutTimestamp("Device 1111", expectedTelemetry1111, actualTelemetry1111);

    }
}
