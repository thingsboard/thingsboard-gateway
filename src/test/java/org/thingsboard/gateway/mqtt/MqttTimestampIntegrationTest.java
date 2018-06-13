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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.thingsboard.gateway.AbstractGatewayMqttIntegrationTest;
import org.thingsboard.gateway.mqtt.simulators.MqttTestClient;
import org.thingsboard.gateway.util.IoUtils;
import org.thingsboard.gateway.util.JsonUtils;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@SpringBootTest()
@ActiveProfiles("mqtt-timestamp")
public class MqttTimestampIntegrationTest extends AbstractGatewayMqttIntegrationTest {

    @Autowired
    private MqttTestClient deviceSimulator;

    @Autowired
    private TestMqttHandler testMqttHandler;

    @Test
    public void testPublishWithTimestamp() throws IOException, InterruptedException, JSONException {
        try {
            deviceSimulator.publish("sensor/SN-001/temperature",
                    IoUtils.getResourceAsString("mqtt/single-value-with-ts-publish.json").getBytes(), 0);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        Thread.sleep(10000);
        List<String> receivedConnectMessage = testMqttHandler.getValues("v1/gateway/connect");
        assertNotNull("receivedConnectMessage was expected to be non-null", receivedConnectMessage);
        assertEquals("Only one connect message was expected", 1, receivedConnectMessage.size());
        assertEquals(IoUtils.getResourceAsString("mqtt/connect-SN-001.json"), receivedConnectMessage.get(0));

        List<String> receivedTelemetryMessage = testMqttHandler.getValues("v1/gateway/telemetry");
        assertNotNull("receivedTelemetryMessage was expected to be non-null", receivedTelemetryMessage);
        assertEquals("Only one telemetry message was expected", 1, receivedTelemetryMessage.size());

        String expectedTelemetryJson = IoUtils.getResourceAsString("mqtt/single-value-with-ts-result.json");
        String actualTelemetryJson = receivedTelemetryMessage.get(0);

        JsonUtils.assertEquals(expectedTelemetryJson, actualTelemetryJson);
    }

}
