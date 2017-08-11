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

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.internal.security.SSLSocketFactoryFactory;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.util.StringUtils;
import org.thingsboard.gateway.extensions.mqtt.client.conf.MqttBrokerConfiguration;
import org.thingsboard.gateway.extensions.mqtt.client.conf.mapping.*;
import org.thingsboard.gateway.extensions.mqtt.client.listener.MqttAttributeRequestsMessageListener;
import org.thingsboard.gateway.extensions.mqtt.client.listener.MqttDeviceStateChangeMessageListener;
import org.thingsboard.gateway.extensions.mqtt.client.listener.MqttRpcResponseMessageListener;
import org.thingsboard.gateway.extensions.mqtt.client.listener.MqttTelemetryMessageListener;
import org.thingsboard.gateway.service.AttributesUpdateListener;
import org.thingsboard.gateway.service.RpcCommandListener;
import org.thingsboard.gateway.service.data.*;
import org.thingsboard.gateway.service.GatewayService;
import org.thingsboard.server.common.data.kv.KvEntry;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final AtomicInteger msgIdSeq = new AtomicInteger();

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
                    getClientId(), new MemoryPersistence());
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

    private String getClientId() {
        return StringUtils.isEmpty(configuration.getClientId()) ? clientId.toString() : configuration.getClientId();
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
            tokens.add(client.subscribe(mapping.getTopicFilter(), 1, new MqttTelemetryMessageListener(this::onDeviceData, mapping.getConverter())));
        }
        if (configuration.getConnectRequests() != null) {
            for (DeviceStateChangeMapping mapping : configuration.getConnectRequests()) {
                tokens.add(client.subscribe(mapping.getTopicFilter(), 1, new MqttDeviceStateChangeMessageListener(mapping, this::onDeviceConnect)));
            }
        }
        if (configuration.getDisconnectRequests() != null) {
            for (DeviceStateChangeMapping mapping : configuration.getDisconnectRequests()) {
                tokens.add(client.subscribe(mapping.getTopicFilter(), 1, new MqttDeviceStateChangeMessageListener(mapping, this::onDeviceDisconnect)));
            }
        }
        if (configuration.getAttributeRequests() != null) {
            for (AttributeRequestsMapping mapping : configuration.getAttributeRequests()) {
                tokens.add(client.subscribe(mapping.getTopicFilter(), 1, new MqttAttributeRequestsMessageListener(this::onAttributeRequest, mapping)));
            }
        }
        for (IMqttToken token : tokens) {
            token.waitForCompletion();
        }
    }

    private void onDeviceConnect(String deviceName, String deviceType) {
        log.info("[{}] Device with type {} connected!", deviceName, deviceType);
        gateway.onDeviceConnect(deviceName, deviceType);
    }

    private void onDeviceDisconnect(String deviceName, String deviceType) {
        log.info("[{}] Device disconnected!", deviceName);
        gateway.onDeviceDisconnect(deviceName);
        log.debug("[{}] Will Topic Msg Received. Disconnecting device...", deviceName);
        cleanUpKeepAliveTimes(deviceName);
    }

    private void onDeviceData(List<DeviceData> data) {
        for (DeviceData dd : data) {
            if (devices.add(dd.getName())) {
                gateway.onDeviceConnect(dd.getName(), dd.getType());
            }
            if (!dd.getAttributes().isEmpty()) {
                gateway.onDeviceAttributesUpdate(dd.getName(), dd.getAttributes());
            }
            if (!dd.getTelemetry().isEmpty()) {
                gateway.onDeviceTelemetry(dd.getName(), dd.getTelemetry());
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

    private void onAttributeRequest(AttributeRequest attributeRequest) {
        gateway.onDeviceAttributeRequest(attributeRequest, this::onAttributeResponse);
    }

    private void onAttributeResponse(AttributeResponse response) {
        if (response.getData().isPresent()) {
            KvEntry attribute = response.getData().get();
            String topic = replace(response.getTopicExpression(), Integer.toString(response.getRequestId()), response.getDeviceName(), attribute);
            String body = replace(response.getValueExpression(), Integer.toString(response.getRequestId()), response.getDeviceName(), attribute);
            publish(response.getDeviceName(), topic, new MqttMessage(body.getBytes(StandardCharsets.UTF_8)));
        } else {
            log.warn("[{}] {} attribute [{}] not found", response.getDeviceName(), response.isClientScope() ? "Client" : "Shared", response.getKey());
        }
    }

    private void cleanUpKeepAliveTimes(String deviceName) {
        ScheduledFuture<?> future = deviceKeepAliveTimers.get(deviceName);
        if (future != null) {
            future.cancel(true);
            deviceKeepAliveTimers.remove(deviceName);
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
            for (KvEntry attribute : affected) {
                String topic = replace(mapping.getTopicExpression(), deviceName, attribute);
                String body = replace(mapping.getValueExpression(), deviceName, attribute);
                MqttMessage msg = new MqttMessage(body.getBytes(StandardCharsets.UTF_8));
                publish(deviceName, topic, msg);
            }
        }
    }

    @Override
    public void onRpcCommand(String deviceName, RpcCommandData command) {
        int requestId = command.getRequestId();

        List<ServerSideRpcMapping> mappings = configuration.getServerSideRpc().stream()
                .filter(mapping -> deviceName.matches(mapping.getDeviceNameFilter()))
                .filter(mapping -> command.getMethod().matches(mapping.getMethodFilter())).collect(Collectors.toList());

        mappings.forEach(mapping -> {
            String requestTopic = replace(mapping.getRequestTopicExpression(), deviceName, command);
            String body = replace(mapping.getValueExpression(), deviceName, command);

            boolean oneway = StringUtils.isEmpty(mapping.getResponseTopicExpression());
            if (oneway) {
                publish(deviceName, requestTopic, new MqttMessage(body.getBytes(StandardCharsets.UTF_8)));
            } else {
                String responseTopic = replace(mapping.getResponseTopicExpression(), deviceName, command);
                try {
                    log.info("[{}] Temporary subscribe to RPC response topic [{}]", deviceName, responseTopic);
                    client.subscribe(responseTopic, 1,
                            new MqttRpcResponseMessageListener(requestId, deviceName, this::onRpcCommandResponse)
                    ).waitForCompletion();
                    scheduler.schedule(() -> {
                        unsubscribe(deviceName, requestId, responseTopic);
                    }, mapping.getResponseTimeout(), TimeUnit.MILLISECONDS);
                    publish(deviceName, requestTopic, new MqttMessage(body.getBytes(StandardCharsets.UTF_8)));
                } catch (MqttException e) {
                    log.warn("[{}] Failed to subscribe to response topic and push RPC command [{}]", deviceName, requestId, e);
                }
            }
        });
    }

    private void onRpcCommandResponse(String topic, RpcCommandResponse rpcResponse) {
        log.info("[{}] Un-subscribe from RPC response topic [{}]", rpcResponse.getDeviceName(), topic);
        gateway.onDeviceRpcResponse(rpcResponse);
        unsubscribe(rpcResponse.getDeviceName(), rpcResponse.getRequestId(), topic);
    }

    private void unsubscribe(String deviceName, int requestId, String topic) {
        try {
            client.unsubscribe(topic);
        } catch (MqttException e) {
            log.warn("[{}][{}] Failed to unsubscribe from RPC reply topic [{}]", deviceName, requestId, topic, e);
        }
    }

    private void publish(final String deviceName, String topic, MqttMessage msg) {
        try {
            client.publish(topic, msg, null, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken iMqttToken) {
                    log.info("[{}] Successfully published to topic [{}]", deviceName, topic);
                }

                @Override
                public void onFailure(IMqttToken iMqttToken, Throwable e) {
                    log.warn("[{}] Failed to publish to topic [{}]", deviceName, topic, e);
                }
            });
        } catch (MqttException e) {
            log.warn("[{}] Failed to publish to topic [{}] ", deviceName, topic, e);
        }
    }

    private static String replace(String expression, String deviceName, KvEntry attribute) {
        return replace(expression, "", deviceName, attribute);
    }

    private static String replace(String expression, String deviceName, RpcCommandData command) {
        return expression.replace("${deviceName}", deviceName)
                .replace("${methodName}", command.getMethod())
                .replace("${requestId}", Integer.toString(command.getRequestId()))
                .replace("${params}", command.getParams());
    }

    private static String replace(String expression, String requestId, String deviceName, KvEntry attribute) {
        return expression.replace("${deviceName}", deviceName)
                .replace("${requestId}", requestId)
                .replace("${attributeKey}", attribute.getKey())
                .replace("${attributeValue}", attribute.getValueAsString());
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
