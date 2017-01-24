/**
 * Copyright © ${project.inceptionYear}-2017 The Thingsboard Authors
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

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.thingsboard.gateway.extensions.mqtt.client.conf.MqttBrokerConfiguration;
import org.thingsboard.gateway.extensions.mqtt.client.conf.mapping.MqttTopicMapping;
import org.thingsboard.gateway.service.GatewayService;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by ashvayka on 24.01.17.
 */
@Slf4j
public class MqttBrokerMonitor {

    private final UUID clientId = UUID.randomUUID();
    private final GatewayService gateway;
    private final MqttBrokerConfiguration configuration;

    private MqttAsyncClient client;
    private MqttConnectOptions clientOptions;
    private Object connectLock = new Object();


    public MqttBrokerMonitor(GatewayService gateway, MqttBrokerConfiguration configuration) {
        this.gateway = gateway;
        this.configuration = configuration;
    }

    public void connect() {
        try {
            client = new MqttAsyncClient((configuration.isSsl() ? "ssl" : "tcp") + "://" + configuration.getHost() + ":" + configuration.getPort(),
                    clientId.toString(), new MemoryPersistence());
            clientOptions = new MqttConnectOptions();
            clientOptions.setCleanSession(true);
            configuration.getCredentials().configure(clientOptions);
            checkConnection();
        } catch (MqttException e) {
            log.error("[{}:{}]MQTT client connection failed!", configuration.getHost(), configuration.getPort(), e);
            throw new RuntimeException("MQTT client connection failed!", e);
        }
    }

    public void disconnect() {

    }

    private void checkConnection() {
        if (!client.isConnected()) {
            synchronized (connectLock) {
                while (!client.isConnected()) {
                    log.debug("Attempt to connect to Thingsboard!");
                    try {
                        client.connect(clientOptions, null, new IMqttActionListener() {
                            @Override
                            public void onSuccess(IMqttToken iMqttToken) {
                                log.info("Connected to Thingsboard!");
                            }

                            @Override
                            public void onFailure(IMqttToken iMqttToken, Throwable e) {
                            }
                        }).waitForCompletion();
                        subscribeToTopics();
                    } catch (MqttException e) {
                        log.warn("Failed to connect to Thingsboard!", e);
                        if (!client.isConnected()) {
                            try {
                                Thread.sleep(configuration.getRetryInterval());
                            } catch (InterruptedException e1) {
                                log.trace("Failed to wait for retry interval!", e);
                            }
                        }
                    }
                }
            }

        }
    }

    private void subscribeToTopics() throws MqttException {
        List<IMqttToken> tokens = new ArrayList<>();
        for (MqttTopicMapping mapping : configuration.getMapping()) {
            tokens.add(client.subscribe(mapping.getTopicFilter(), 1, new MqttMessageListener(gateway, mapping.getConverter())));
        }
        for (IMqttToken token : tokens) {
            token.waitForCompletion();
        }
    }
}
