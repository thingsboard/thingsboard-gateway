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
package org.thingsboard.gateway.extensions.mqtt.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.gateway.extensions.mqtt.client.conf.MqttClientConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;
import org.thingsboard.gateway.util.ConfigurationTools;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by ashvayka on 23.01.17.
 */
@Slf4j
public class DefaultMqttClientService implements MqttClientService {

    private final GatewayService gateway;

    private List<MqttBrokerMonitor> brokers;

    public DefaultMqttClientService(GatewayService gateway) {
        this.gateway = gateway;
    }

    @Override
    public void init(JsonNode configurationNode) throws Exception {
        log.info("[{}] Initializing MQTT client service!", gateway.getTenantLabel());
        MqttClientConfiguration configuration;
        try {
            configuration = ConfigurationTools.readConfiguration(configurationNode, MqttClientConfiguration.class);
        } catch (Exception e) {
            log.error("[{}] MQTT client service configuration failed!", gateway.getTenantLabel(), e);
            throw e;
        }

        try {
            brokers = configuration.getBrokers().stream().map(c -> new MqttBrokerMonitor(gateway, c)).collect(Collectors.toList());
            brokers.forEach(MqttBrokerMonitor::connect);
        } catch (Exception e) {
            log.error("[{}] MQTT client service initialization failed!", gateway.getTenantLabel(), e);
            throw e;
        }
    }

    @Override
    public void update(JsonNode configurationNode) {

    }

    @Override
    public void destroy() {
        if (brokers != null) {
            brokers.forEach(MqttBrokerMonitor::disconnect);
        }
    }


}
