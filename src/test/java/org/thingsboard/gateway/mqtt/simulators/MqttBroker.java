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

import io.moquette.BrokerConstants;
import io.moquette.server.Server;
import io.moquette.server.config.MemoryConfig;
import lombok.Data;
import org.junit.After;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Valerii Sosliuk on 5/9/2018.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@Data
public class MqttBroker {


    @Value("${test.host:localhost}")
    private String host;
    @Value("${mqtt.broker.external.port:7883}")
    private int port;
    @Value("${mqtt.broker.external.allowAnonymous:true}")
    private boolean allowAnonymous;

    private Server server;

    public MqttBroker() {

    }

    @PostConstruct
    public void init() {
        server = new Server();
        final Properties configProps = new Properties();
        configProps.put(BrokerConstants.PORT_PROPERTY_NAME, Integer.toString(port));
        configProps.put(BrokerConstants.HOST_PROPERTY_NAME, host);
        configProps.put(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, allowAnonymous);

        try {
            server.startServer(new MemoryConfig(configProps));
        } catch (IOException e) {
            throw new RuntimeException("Unable to initialize MQTT Broker on port [" + port + "]. Reason: " + e.getMessage(), e);
        }
    }

    @PreDestroy
    public void tearDown() {
        server.stopServer();
    }
}
