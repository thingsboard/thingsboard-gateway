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
package org.thingsboard.gateway.service.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.springframework.util.StringUtils;
import org.thingsboard.gateway.service.AttributesUpdateListener;
import org.thingsboard.gateway.service.RpcCommandListener;
import org.thingsboard.gateway.service.conf.*;
import org.thingsboard.gateway.service.data.*;
import org.thingsboard.gateway.util.JsonTools;
import org.thingsboard.server.common.data.kv.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.thingsboard.gateway.util.JsonTools.*;

/**
 * Created by ashvayka on 16.01.17.
 */
@Slf4j
public class MqttGatewayService implements GatewayService, MqttCallback, IMqttMessageListener {

    private static final long CLIENT_RECONNECT_CHECK_INTERVAL = 1;
    public static final String DEVICE_TELEMETRY_TOPIC = "v1/devices/me/telemetry";
    public static final String GATEWAY_RPC_TOPIC = "v1/gateway/rpc";
    public static final String GATEWAY_ATTRIBUTES_TOPIC = "v1/gateway/attributes";
    public static final String GATEWAY_TELEMETRY_TOPIC = "v1/gateway/telemetry";
    public static final String GATEWAY_REQUESTS_ATTRIBUTES_TOPIC = "v1/gateway/attributes/request";
    public static final String GATEWAY_RESPONSES_ATTRIBUTES_TOPIC = "v1/gateway/attributes/response";
    public static final String GATEWAY_CONNECT_TOPIC = "v1/gateway/connect";
    public static final String GATEWAY_DISCONNECT_TOPIC = "v1/gateway/disconnect";
    public static final String DEVICE_ATTRIBUTES_TOPIC = "v1/devices/me/attributes";
    public static final String DEVICE_GET_ATTRIBUTES_REQUEST_TOPIC = "v1/devices/me/attributes/request/1";
    public static final String DEVICE_GET_ATTRIBUTES_RESPONSE_TOPIC = "v1/devices/me/attributes/response/1";
    public static final String DEVICE_GET_ATTRIBUTES_RESPONSE_PLUS_TOPIC = "v1/devices/me/attributes/response/+";

    private final ConcurrentMap<String, DeviceInfo> devices = new ConcurrentHashMap<>();
    private final AtomicLong attributesCount = new AtomicLong();
    private final AtomicLong telemetryCount = new AtomicLong();
    private final AtomicInteger msgIdSeq = new AtomicInteger();
    private final Set<AttributesUpdateSubscription> attributeUpdateSubs = ConcurrentHashMap.newKeySet();
    private final Set<RpcCommandSubscription> rpcCommandSubs = ConcurrentHashMap.newKeySet();
    private final Map<AttributeRequestKey, AttributeRequestListener> pendingAttrRequestsMap = new ConcurrentHashMap<>();

    private final String tenantLabel;
    private final TbTenantConfiguration configuration;
    private final TbConnectionConfiguration connection;
    private final TbReportingConfiguration reporting;
    private final TbPersistenceConfiguration persistence;
    private final Consumer<String> extensionsConfigListener;

    private volatile ObjectNode error;

    private MqttAsyncClient tbClient;
    private MqttConnectOptions tbClientOptions;

    private Object connectLock = new Object();

    private ScheduledExecutorService scheduler;
    private ExecutorService callbackExecutor = Executors.newCachedThreadPool();


    public MqttGatewayService(TbTenantConfiguration configuration, Consumer<String> extensionsConfigListener) {
        this.configuration = configuration;
        this.extensionsConfigListener = extensionsConfigListener;
        this.tenantLabel = configuration.getLabel();
        this.connection = configuration.getConnection();
        this.reporting = configuration.getReporting();
        this.persistence = configuration.getPersistence();
    }

    @Override
    public void init() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();

        tbClientOptions = new MqttConnectOptions();
        tbClientOptions.setCleanSession(false);
        tbClientOptions.setMaxInflight(connection.getMaxInFlight());
        tbClientOptions.setAutomaticReconnect(true);

        MqttGatewaySecurityConfiguration security = connection.getSecurity();
        security.setupSecurityOptions(tbClientOptions);

        tbClient = new MqttAsyncClient((security.isSsl() ? "ssl" : "tcp") + "://" + connection.getHost() + ":" + connection.getPort(),
                security.getClientId(), persistence.getPersistence());
        tbClient.setCallback(this);

        if (persistence.getBufferSize() > 0) {
            DisconnectedBufferOptions options = new DisconnectedBufferOptions();
            options.setBufferSize(persistence.getBufferSize());
            options.setBufferEnabled(true);
            options.setPersistBuffer(true);
            tbClient.setBufferOpts(options);
        }
        connect();

        scheduler.scheduleAtFixedRate(this::reportStats, 0, reporting.getInterval(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void destroy() throws Exception {
        scheduler.shutdownNow();
        callbackExecutor.shutdownNow();
        tbClient.disconnect();
    }

    @Override
    public String getTenantLabel() {
        return tenantLabel;
    }

    @Override
    public MqttDeliveryFuture onDeviceConnect(final String deviceName, final String deviceType) {
        final int msgId = msgIdSeq.incrementAndGet();
        ObjectNode node = newNode().put("device", deviceName);
        if (!StringUtils.isEmpty(deviceType)) {
            node.put("type", deviceType);
        }
        MqttMessage msg = new MqttMessage(toBytes(node));
        msg.setId(msgId);
        log.info("[{}] Device Connected!", deviceName);
        devices.putIfAbsent(deviceName, new DeviceInfo(deviceName, deviceType));
        return publishAsync(GATEWAY_CONNECT_TOPIC, msg,
                token -> {
                    log.info("[{}][{}] Device connect event is reported to Thingsboard!", deviceName, msgId);
                },
                error -> log.warn("[{}][{}] Failed to report device connection!", deviceName, msgId, error));
    }

    @Override
    public Optional<MqttDeliveryFuture> onDeviceDisconnect(String deviceName) {
        if (devices.remove(deviceName) != null) {
            final int msgId = msgIdSeq.incrementAndGet();
            byte[] msgData = toBytes(newNode().put("device", deviceName));
            MqttMessage msg = new MqttMessage(msgData);
            msg.setId(msgId);
            log.info("[{}][{}] Device Disconnected!", deviceName, msgId);
            MqttDeliveryFuture future = publishAsync(GATEWAY_DISCONNECT_TOPIC, msg,
                    token -> {
                        log.info("[{}][{}] Device disconnect event is delivered!", deviceName, msgId);
                    },
                    error -> log.warn("[{}][{}] Failed to report device disconnect!", deviceName, msgId, error));
            return Optional.of(future);
        } else {
            log.debug("[{}] Device was disconnected before. Nothing is going to happened.", deviceName);
            return Optional.empty();
        }
    }

    @Override
    public MqttDeliveryFuture onDeviceAttributesUpdate(String deviceName, List<KvEntry> attributes) {
        final int msgId = msgIdSeq.incrementAndGet();
        log.trace("[{}][{}] Updating device attributes: {}", deviceName, msgId, attributes);
        checkDeviceConnected(deviceName);
        ObjectNode node = newNode();
        ObjectNode deviceNode = node.putObject(deviceName);
        attributes.forEach(kv -> putToNode(deviceNode, kv));
        final int packSize = attributes.size();
        MqttMessage msg = new MqttMessage(toBytes(node));
        msg.setId(msgId);
        return publishAsync(GATEWAY_ATTRIBUTES_TOPIC, msg,
                token -> {
                    log.debug("[{}][{}] Device attributes were delivered!", deviceName, msgId);
                    attributesCount.addAndGet(packSize);
                },
                error -> log.warn("[{}][{}] Failed to report device attributes!", deviceName, msgId, error));
    }

    @Override
    public MqttDeliveryFuture onDeviceTelemetry(String deviceName, List<TsKvEntry> telemetry) {
        final int msgId = msgIdSeq.incrementAndGet();
        log.trace("[{}][{}] Updating device telemetry: {}", deviceName, msgId, telemetry);
        checkDeviceConnected(deviceName);
        ObjectNode node = newNode();
        Map<Long, List<TsKvEntry>> tsMap = telemetry.stream().collect(Collectors.groupingBy(v -> v.getTs()));
        ArrayNode deviceNode = node.putArray(deviceName);
        tsMap.entrySet().forEach(kv -> {
            Long ts = kv.getKey();
            ObjectNode tsNode = deviceNode.addObject();
            tsNode.put("ts", ts);
            ObjectNode valuesNode = tsNode.putObject("values");
            kv.getValue().forEach(v -> putToNode(valuesNode, v));
        });
        final int packSize = telemetry.size();
        MqttMessage msg = new MqttMessage(toBytes(node));
        msg.setId(msgId);
        return publishAsync(GATEWAY_TELEMETRY_TOPIC, msg,
                token -> {
                    log.debug("[{}][{}] Device telemetry published to ThingsBoard!", msgId, deviceName);
                    telemetryCount.addAndGet(packSize);
                },
                error -> log.warn("[{}][{}] Failed to publish device telemetry!", deviceName, msgId, error));
    }

    @Override
    public void onDeviceAttributeRequest(AttributeRequest request, Consumer<AttributeResponse> listener) {
        final int msgId = msgIdSeq.incrementAndGet();
        String deviceName = request.getDeviceName();
        AttributeRequestKey requestKey = new AttributeRequestKey(request.getRequestId(), request.getDeviceName());
        log.trace("[{}][{}] Requesting {} attribute: {}", deviceName, msgId, request.isClientScope() ? "client" : "shared", request.getAttributeKey());
        checkDeviceConnected(deviceName);

        ObjectNode node = newNode();
        node.put("id", request.getRequestId());
        node.put("client", request.isClientScope());
        node.put("device", request.getDeviceName());
        node.put("key", request.getAttributeKey());
        MqttMessage msg = new MqttMessage(toBytes(node));

        msg.setId(msgId);
        pendingAttrRequestsMap.put(requestKey, new AttributeRequestListener(request, listener));
        publishAsync(GATEWAY_REQUESTS_ATTRIBUTES_TOPIC, msg,
                token -> {
                    log.debug("[{}][{}] Device attributes request was delivered!", deviceName, msgId);
                },
                error -> {
                    log.warn("[{}][{}] Failed to report device attributes!", deviceName, msgId, error);
                    pendingAttrRequestsMap.remove(requestKey);
                });
    }

    @Override
    public void onDeviceRpcResponse(RpcCommandResponse response) {
        final int msgId = msgIdSeq.incrementAndGet();
        int requestId = response.getRequestId();
        String deviceName = response.getDeviceName();
        String data = response.getData();
        checkDeviceConnected(deviceName);

        ObjectNode node = newNode();
        node.put("id", requestId);
        node.put("device", deviceName);
        node.put("data", data);
        MqttMessage msg = new MqttMessage(toBytes(node));
        msg.setId(msgId);
        publishAsync(GATEWAY_RPC_TOPIC, msg,
                token -> {
                    log.debug("[{}][{}] RPC response from device was delivered!", deviceName, requestId);
                },
                error -> {
                    log.warn("[{}][{}] Failed to report RPC response from device!", deviceName, requestId, error);
                });
    }

    @Override
    public boolean subscribe(AttributesUpdateSubscription subscription) {
        return subscribe(attributeUpdateSubs::add, subscription);
    }

    @Override
    public boolean subscribe(RpcCommandSubscription subscription) {
        return subscribe(rpcCommandSubs::add, subscription);
    }

    @Override
    public boolean unsubscribe(AttributesUpdateSubscription subscription) {
        return unsubscribe(attributeUpdateSubs::remove, subscription);
    }

    @Override
    public boolean unsubscribe(RpcCommandSubscription subscription) {
        return unsubscribe(rpcCommandSubs::remove, subscription);
    }

    @Override
    public void onError(Exception e) {
        onError(null, e);
    }

    @Override
    public void onError(String deviceName, Exception e) {
        ObjectNode node = newNode();
        node.put("ts", System.currentTimeMillis());
        if (deviceName != null) {
            node.put("device", deviceName);
        }
        node.put("error", toString(e));
        error = node;
    }

    private void connect() {
        if (!tbClient.isConnected()) {
            synchronized (connectLock) {
                while (!tbClient.isConnected()) {
                    log.debug("Attempt to connect to Thingsboard!");
                    try {
                        tbClient.connect(tbClientOptions, null, new IMqttActionListener() {
                            @Override
                            public void onSuccess(IMqttToken iMqttToken) {
                                log.info("Connected to Thingsboard!");
                            }

                            @Override
                            public void onFailure(IMqttToken iMqttToken, Throwable e) {
                            }
                        }).waitForCompletion();

                        tbClient.subscribe(DEVICE_ATTRIBUTES_TOPIC, 1, (IMqttMessageListener) this).waitForCompletion();

                        tbClient.subscribe(DEVICE_GET_ATTRIBUTES_RESPONSE_PLUS_TOPIC, 1, (IMqttMessageListener) this).waitForCompletion();
                        ObjectNode node = newNode().put("shared", "configuration");
                        MqttMessage msg = new MqttMessage(toBytes(node));
                        tbClient.publish(DEVICE_GET_ATTRIBUTES_REQUEST_TOPIC, msg).waitForCompletion();

                        devices.forEach((k, v) -> onDeviceConnect(v.getName(), v.getType()));
                    } catch (Exception e) {
                        log.warn("Failed to connect to ThingsBoard!", e);
                        if (!tbClient.isConnected()) {
                            try {
                                Thread.sleep(connection.getRetryInterval());
                            } catch (InterruptedException e1) {
                                log.trace("Failed to wait for retry interval!", e);
                            }
                        }
                    }
                }
            }
        }
    }

    private void checkDeviceConnected(String deviceName) {
        if (!devices.containsKey(deviceName)) {
            onDeviceConnect(deviceName, null);
        }
    }

    private MqttDeliveryFuture publishAsync(final String topic, MqttMessage msg, Consumer<IMqttToken> onSuccess, Consumer<Throwable> onFailure) {
        try {
            IMqttDeliveryToken token = tbClient.publish(topic, msg, null, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    onSuccess.accept(asyncActionToken);
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable e) {
                    onFailure.accept(e);
                }
            });
            return new MqttDeliveryFuture(token);
        } catch (MqttException e) {
            onFailure.accept(e);
            return new MqttDeliveryFuture(e);
        }
    }

    private void reportStats() {
        if (tbClient == null) {
            log.info("Can't report stats because client was not initialized yet!");
            return;
        }
        ObjectNode node = newNode();
        node.put("ts", System.currentTimeMillis());
        ObjectNode valuesNode = node.putObject("values");

        valuesNode.put("devicesOnline", devices.size());
        valuesNode.put("attributesUploaded", attributesCount.getAndSet(0));
        valuesNode.put("telemetryUploaded", telemetryCount.getAndSet(0));
        if (error != null) {
            valuesNode.put("latestError", JsonTools.toString(error));
            error = null;
        }
        MqttMessage msg = new MqttMessage(toBytes(node));
        msg.setId(msgIdSeq.incrementAndGet());
        publishAsync(DEVICE_TELEMETRY_TOPIC, msg,
                token -> log.info("Gateway statistics {} reported!", node),
                error -> log.warn("Failed to report gateway statistics!", error));
    }

    @Override
    public void connectionLost(Throwable cause) {
        //TODO: reply with error
        pendingAttrRequestsMap.clear();
        scheduler.schedule(this::checkClientReconnected, CLIENT_RECONNECT_CHECK_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        log.trace("Message arrived [{}] {}", topic, message.getId());
        callbackExecutor.submit(() -> {
            try {
                if (topic.equals(GATEWAY_ATTRIBUTES_TOPIC)) {
                    onAttributesUpdate(message);
                } else if (topic.equals(GATEWAY_RESPONSES_ATTRIBUTES_TOPIC)) {
                    onDeviceAttributesResponse(message);
                } else if (topic.equals(GATEWAY_RPC_TOPIC)) {
                    onRpcCommand(message);
                } else if (topic.equals(DEVICE_ATTRIBUTES_TOPIC)) {
                    onGatewayAttributesUpdate(message);
                } else if (topic.equals(DEVICE_GET_ATTRIBUTES_RESPONSE_TOPIC)) {
                    onGatewayAttributesGet(message);
                }
            } catch (Exception e) {
                log.warn("Failed to process arrived message", message);
            }
        });
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        log.trace("Delivery complete [{}]", token);
    }

    private void onAttributesUpdate(MqttMessage message) {
        JsonNode payload = fromString(new String(message.getPayload(), StandardCharsets.UTF_8));
        String deviceName = payload.get("device").asText();
        Set<AttributesUpdateListener> listeners = attributeUpdateSubs.stream()
                .filter(sub -> sub.matches(deviceName)).map(sub -> sub.getListener())
                .collect(Collectors.toSet());
        if (!listeners.isEmpty()) {
            JsonNode data = payload.get("data");
            List<KvEntry> attributes = getKvEntries(data);
            listeners.forEach(listener -> callbackExecutor.submit(() -> {
                try {
                    listener.onAttributesUpdated(deviceName, attributes);
                } catch (Exception e) {
                    log.error("[{}] Failed to process attributes update", deviceName, e);
                }
            }));
        }
    }

    private void onRpcCommand(MqttMessage message) {
        JsonNode payload = fromString(new String(message.getPayload(), StandardCharsets.UTF_8));
        String deviceName = payload.get("device").asText();
        Set<RpcCommandListener> listeners = rpcCommandSubs.stream()
                .filter(sub -> sub.matches(deviceName)).map(RpcCommandSubscription::getListener)
                .collect(Collectors.toSet());
        if (!listeners.isEmpty()) {
            JsonNode data = payload.get("data");
            RpcCommandData rpcCommand = new RpcCommandData();
            rpcCommand.setRequestId(data.get("id").asInt());
            rpcCommand.setMethod(data.get("method").asText());
            rpcCommand.setParams(JsonTools.toString(data.get("params")));
            listeners.forEach(listener -> callbackExecutor.submit(() -> {
                try {
                    listener.onRpcCommand(deviceName, rpcCommand);
                } catch (Exception e) {
                    log.error("[{}][{}] Failed to process rpc command", deviceName, rpcCommand.getRequestId(), e);
                }
            }));
        } else {
            log.warn("No listener registered for RPC command to device {}!", deviceName);
        }
    }

    private void onGatewayAttributesGet(MqttMessage message) {
        log.info("Configuration arrived! {}", message.toString());
        JsonNode payload = fromString(new String(message.getPayload(), StandardCharsets.UTF_8));
        if (payload.get("shared").get("configuration") != null) {
            String configuration = payload.get("shared").get("configuration").asText();
            if (!StringUtils.isEmpty(configuration)) {
                updateConfiguration(configuration);
            }
        }
    }

    private void onGatewayAttributesUpdate(MqttMessage message) {
        log.info("Configuration updates arrived! {}", message.toString());
        JsonNode payload = fromString(new String(message.getPayload(), StandardCharsets.UTF_8));
        if (payload.has("configuration")) {
            String configuration = payload.get("configuration").asText();
            if (!StringUtils.isEmpty(configuration)) {
                updateConfiguration(configuration);
            }
        }
    }

    private void updateConfiguration(String configuration) {
        try {
            extensionsConfigListener.accept(configuration);
            onAppliedConfiguration(configuration);
        } catch (Exception e) {
            log.warn("Failed to update extension configurations");
            onConfigurationError(e, configuration);
        }
    }

    @Override
    public void onAppliedConfiguration(String configuration) {
        try {
            ObjectNode node = newNode();
            node.put("appliedConfiguration", configuration);
            MqttMessage msg = new MqttMessage(JsonTools.toString(node).getBytes(StandardCharsets.UTF_8));
            tbClient.publish(DEVICE_ATTRIBUTES_TOPIC, msg).waitForCompletion();
        } catch (Exception e) {
            log.warn("Could not publish applied configuration", e);
        }
    }

    @Override
    public void onConfigurationError(Exception e, String configuration) {
        try {
            ArrayNode fromString = (ArrayNode) fromString(configuration);
            String id = fromString.get(0).get("id").asText();
            ObjectNode node = newNode();
            node.put(id + "ExtensionError", toString(e));
            MqttMessage msg = new MqttMessage(toBytes(node));
            tbClient.publish(DEVICE_TELEMETRY_TOPIC, msg).waitForCompletion();

            node = newNode();
            node.put(id + "ExtensionStatus", "Failure");
            msg = new MqttMessage(toBytes(node));
            tbClient.publish(DEVICE_TELEMETRY_TOPIC, msg).waitForCompletion();
        } catch (Exception e1) {
            log.warn("Could not report telemetry error", e1);
        }
    }

    @Override
    public void onConfigurationStatus(String id, String status) throws MqttException {
        ObjectNode objectNode = newNode();
        reportStatus(objectNode, id, status);
    }

    private void reportStatus(ObjectNode objectNode, String id, String status) throws MqttException {
        objectNode.put(id + "ExtensionStatus", status);
        MqttMessage msg = new MqttMessage(toBytes(objectNode));
        tbClient.publish(DEVICE_TELEMETRY_TOPIC, msg).waitForCompletion();
        log.info("Reported status [{}] of extension [{}]", status, id);
    }

    private void onDeviceAttributesResponse(MqttMessage message) {
        JsonNode payload = fromString(new String(message.getPayload(), StandardCharsets.UTF_8));
        AttributeRequestKey requestKey = new AttributeRequestKey(payload.get("id").asInt(), payload.get("device").asText());

        AttributeRequestListener listener = pendingAttrRequestsMap.get(requestKey);
        if (listener == null) {
            log.warn("[{}][{}] Can't find listener for request", requestKey.getDeviceName(), requestKey.getRequestId());
            return;
        }

        AttributeRequest request = listener.getRequest();
        AttributeResponse.AttributeResponseBuilder response = AttributeResponse.builder();

        response.requestId(request.getRequestId());
        response.deviceName(request.getDeviceName());
        response.key(request.getAttributeKey());
        response.clientScope(request.isClientScope());
        response.topicExpression(request.getTopicExpression());
        response.valueExpression(request.getValueExpression());

        String key = listener.getRequest().getAttributeKey();
        JsonNode value = payload.get("value");
        if (value == null) {
            response.data(Optional.empty());
        } else if (value.isBoolean()) {
            response.data(Optional.of(new BooleanDataEntry(key, value.asBoolean())));
        } else if (value.isLong()) {
            response.data(Optional.of(new LongDataEntry(key, value.asLong())));
        } else if (value.isDouble()) {
            response.data(Optional.of(new DoubleDataEntry(key, value.asDouble())));
        } else {
            response.data(Optional.of(new StringDataEntry(key, value.asText())));
        }

        callbackExecutor.submit(() -> {
            try {
                listener.getListener().accept(response.build());
            } catch (Exception e) {
                log.error("[{}][{}] Failed to process attributes response", requestKey.getDeviceName(), requestKey.getRequestId(), e);
            }
        });
    }

    private void checkClientReconnected() {
        if (tbClient.isConnected()) {
            devices.forEach((k, v) -> onDeviceConnect(v.getName(), v.getType()));
        } else {
            scheduler.schedule(this::checkClientReconnected, CLIENT_RECONNECT_CHECK_INTERVAL, TimeUnit.SECONDS);
        }
    }

    private static String toString(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    private <T> boolean subscribe(Function<T, Boolean> f, T sub) {
        if (f.apply(sub)) {
            log.info("Subscription added: {}", sub);
            return true;
        } else {
            log.warn("Subscription was already added: {}", sub);
            return false;
        }
    }

    private <T> boolean unsubscribe(Function<T, Boolean> f, T sub) {
        if (f.apply(sub)) {
            log.info("Subscription removed: {}", sub);
            return true;
        } else {
            log.warn("Subscription was already removed: {}", sub);
            return false;
        }
    }
}
