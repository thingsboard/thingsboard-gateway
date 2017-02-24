/**
 * Copyright © 2017 The Thingsboard Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.gateway.extensions.mqtt.client;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.internal.security.SSLSocketFactoryFactory;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.util.StringUtils;
import org.thingsboard.gateway.extensions.mqtt.client.conf.MqttBrokerConfiguration;
import org.thingsboard.gateway.extensions.mqtt.client.conf.mapping.AttributeUpdatesMapping;
import org.thingsboard.gateway.extensions.mqtt.client.conf.mapping.MqttTopicMapping;
import org.thingsboard.gateway.service.AttributesUpdateListener;
import org.thingsboard.gateway.service.RpcCommandListener;
import org.thingsboard.gateway.service.data.AttributesUpdateSubscription;
import org.thingsboard.gateway.service.data.DeviceData;
import org.thingsboard.gateway.service.GatewayService;
import org.thingsboard.gateway.service.data.RpcCommandData;
import org.thingsboard.gateway.service.data.RpcCommandSubscription;
import org.thingsboard.server.common.data.kv.KvEntry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by ashvayka on 24.01.17.
 */
@Slf4j
public class MqttBrokerMonitor implements MqttCallback, AttributesUpdateListener, RpcCommandListener {
    private final UUID clientId = UUID.randomUUID();
    private final GatewayService gateway;
    private final MqttBrokerConfiguration configuration;
    private final Set<String> devices;

    private MqttAsyncClient client;
    private MqttConnectOptions clientOptions;
    private Object connectLock = new Object();

    //TODO: probably use newScheduledThreadPool(int threadSize) to improve performance in heavy load cases
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, ScheduledFuture<?>> deviceKeepAliveTimers = new ConcurrentHashMap<>();

    public MqttBrokerMonitor(GatewayService gateway, MqttBrokerConfiguration configuration) {
        this.gateway = gateway;
        this.configuration = configuration;
        this.devices = new HashSet<>();
    }

    public void connect() {
        try {
            client = new MqttAsyncClient((configuration.isSsl() ? "ssl" : "tcp") + "://" + configuration.getHost() + ":" + configuration.getPort(),
                    clientId.toString(), new MemoryPersistence());
            client.setCallback(this);
            clientOptions = new MqttConnectOptions();
            clientOptions.setCleanSession(true);
            if (configuration.isSsl() && !StringUtils.isEmpty(configuration.getTruststore())) {
                Properties sslProperties = new Properties();
                sslProperties.put(SSLSocketFactoryFactory.TRUSTSTORE, configuration.getTruststore());
                sslProperties.put(SSLSocketFactoryFactory.TRUSTSTOREPWD, configuration.getTruststorePassword());
                sslProperties.put(SSLSocketFactoryFactory.TRUSTSTORETYPE, "JKS");
                sslProperties.put(SSLSocketFactoryFactory.CLIENTAUTH, false);
                clientOptions.setSSLProperties(sslProperties);
            }
            configuration.getCredentials().configure(clientOptions);
            checkConnection();
            if (configuration.getAttributeUpdates() != null) {
                configuration.getAttributeUpdates().forEach(mapping ->
                        gateway.subscribe(new AttributesUpdateSubscription(mapping.getDeviceNameFilter(), this))
                );
            }
            if (configuration.getServerSideRpc() != null) {
                configuration.getServerSideRpc().forEach(mapping ->
                        gateway.subscribe(new RpcCommandSubscription(mapping.getDeviceNameFilter(), this))
                );
            }
        } catch (MqttException e) {
            log.error("[{}:{}] MQTT broker connection failed!", configuration.getHost(), configuration.getPort(), e);
            throw new RuntimeException("MQTT broker connection failed!", e);
        }
    }

    public void disconnect() {
        devices.forEach(gateway::onDeviceDisconnect);
        scheduler.shutdownNow();
    }

    private void checkConnection() {
        if (!client.isConnected()) {
            synchronized (connectLock) {
                while (!client.isConnected()) {
                    log.debug("[{}:{}] MQTT broker connection attempt!", configuration.getHost(), configuration.getPort());
                    try {
                        client.connect(clientOptions, null, new IMqttActionListener() {
                            @Override
                            public void onSuccess(IMqttToken iMqttToken) {
                                log.info("[{}:{}] MQTT broker connection established!", configuration.getHost(), configuration.getPort());
                            }

                            @Override
                            public void onFailure(IMqttToken iMqttToken, Throwable e) {
                            }
                        }).waitForCompletion();
                        subscribeToTopics();
                    } catch (MqttException e) {
                        log.warn("[{}:{}] MQTT broker connection failed!", configuration.getHost(), configuration.getPort(), e);
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
            tokens.add(client.subscribe(mapping.getTopicFilter(), 1, new MqttMessageListener(this::onDeviceData, mapping.getConverter())));
        }
        for (IMqttToken token : tokens) {
            token.waitForCompletion();
        }
    }

    private void onDeviceData(List<DeviceData> data) {
        for (DeviceData dd : data) {
            if (!dd.isDisconnect() && devices.add(dd.getName())) {
                gateway.onDeviceConnect(dd.getName());
            }
            if (!dd.getAttributes().isEmpty()) {
                gateway.onDeviceAttributesUpdate(dd.getName(), dd.getAttributes());
            }
            if (!dd.getTelemetry().isEmpty()) {
                gateway.onDeviceTelemetry(dd.getName(), dd.getTelemetry());
            }
            if (dd.isDisconnect()) {
                log.debug("[{}] Will Topic Msg Received. Disconnecting device...", dd.getName());
                gateway.onDeviceDisconnect(dd.getName());
                cleanUpKeepAliveTimes(dd);
            }
            if (dd.getTimeout() != 0) {
                ScheduledFuture<?> future = deviceKeepAliveTimers.get(dd.getName());
                if (future != null) {
                    log.debug("Re-scheduling keep alive timer for device {} with timeout = {}", dd.getName(), dd.getTimeout());
                    future.cancel(true);
                    deviceKeepAliveTimers.remove(dd.getName());
                    scheduleDeviceKeepAliveTimer(dd);
                } else {
                    log.debug("Scheduling keep alive timer for device {} with timeout = {}", dd.getName(), dd.getTimeout());
                    scheduleDeviceKeepAliveTimer(dd);
                }
            }
        }
    }

    private void cleanUpKeepAliveTimes(DeviceData dd) {
        if (dd.getTimeout() != 0) {
            ScheduledFuture<?> future = deviceKeepAliveTimers.get(dd.getName());
            if (future != null) {
                future.cancel(true);
                deviceKeepAliveTimers.remove(dd.getName());
            }
        }
    }

    private void scheduleDeviceKeepAliveTimer(DeviceData dd) {
        ScheduledFuture<?> f = scheduler.schedule(
                () -> {
                    log.warn("[{}] Device is going to be disconnected because of timeout! timeout = {} milliseconds", dd.getName(), dd.getTimeout());
                    deviceKeepAliveTimers.remove(dd.getName());
                    gateway.onDeviceDisconnect(dd.getName());
                },
                dd.getTimeout(),
                TimeUnit.MILLISECONDS
        );
        deviceKeepAliveTimers.put(dd.getName(), f);
    }

    @Override
    public void onAttributesUpdated(String deviceName, List<KvEntry> attributes) {
        List<AttributeUpdatesMapping> mappings = configuration.getAttributeUpdates().stream()
                .filter(mapping -> deviceName.matches(mapping.getDeviceNameFilter())).collect(Collectors.toList());

        for (AttributeUpdatesMapping mapping : mappings) {
            List<KvEntry> affected = attributes.stream().filter(attribute -> attribute.getKey()
                    .matches(mapping.getAttributeFilter())).collect(Collectors.toList());

        }

        //TODO
    }

    @Override
    public void onRpcCommand(String deviceName, RpcCommandData command) {
        //TODO
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.warn("[{}:{}] MQTT broker connection lost!", configuration.getHost(), configuration.getPort());
        devices.forEach(gateway::onDeviceDisconnect);
        checkConnection();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}
