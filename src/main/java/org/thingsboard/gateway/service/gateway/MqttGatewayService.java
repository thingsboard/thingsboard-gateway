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
package org.thingsboard.gateway.service.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import nl.jk5.mqtt.*;
import nl.jk5.mqtt.MqttClient;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.thingsboard.gateway.service.*;
import org.thingsboard.gateway.service.conf.*;
import org.thingsboard.gateway.service.data.*;
import org.thingsboard.gateway.util.JsonTools;
import org.thingsboard.server.common.data.kv.*;

import javax.annotation.PostConstruct;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.thingsboard.gateway.util.JsonTools.*;

/**
 * Created by ashvayka on 16.01.17.
 */
@Slf4j
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MqttGatewayService implements GatewayService, MqttCallback, MqttClientCallback, IMqttMessageListener {

    private static final long CLIENT_RECONNECT_CHECK_INTERVAL = 1;
    public static final String DEVICE_TELEMETRY_TOPIC = "v1/devices/me/telemetry";
    public static final String GATEWAY_RPC_TOPIC = "v1/gateway/rpc";
    public static final String GATEWAY_ATTRIBUTES_TOPIC = "v1/gateway/attributes";
    public static final String GATEWAY_TELEMETRY_TOPIC = "v1/gateway/telemetry";
    public static final String GATEWAY_REQUESTS_ATTRIBUTES_TOPIC = "v1/gateway/attributes/request";
    public static final String GATEWAY_RESPONSES_ATTRIBUTES_TOPIC = "v1/gateway/attributes/response";
    public static final String GATEWAY_CONNECT_TOPIC = "v1/gateway/connect";
    public static final String GATEWAY_DISCONNECT_TOPIC = "v1/gateway/disconnect";
    public static final String GATEWAY = "GATEWAY";
    private static final long DEFAULT_CONNECTION_TIMEOUT = 10000;
    private static final String JKS = "JKS";
    private static final long DEFAULT_POLLING_INTERVAL = 1000;
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

    private String tenantLabel;

    private PersistentFileService persistentFileService;

    private Consumer<String> extensionsConfigListener;
    private TbTenantConfiguration configuration;
    private TbConnectionConfiguration connection;
    private TbReportingConfiguration reporting;
    private TbPersistenceConfiguration persistence;


    private volatile ObjectNode error;
    private MqttClient tbClient;

    private ScheduledExecutorService scheduler;
    private ExecutorService mqttSenderExecutor;
    private ExecutorService callbackExecutor = Executors.newCachedThreadPool();


    public MqttGatewayService(TbTenantConfiguration configuration, Consumer<String> extensionsConfigListener) {
        this.configuration = configuration;
        this.extensionsConfigListener = extensionsConfigListener;
    }

    @Override
    @PostConstruct
    public void init() {
        this.tenantLabel = configuration.getLabel();
        this.connection = configuration.getConnection();
        this.reporting = configuration.getReporting();
        this.persistence = configuration.getPersistence();
        this.tenantLabel = configuration.getLabel();
        initTimeouts();
        initMqttClient();
        initMqttSender();
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::reportStats, 0, reporting.getInterval(), TimeUnit.MILLISECONDS);
    }

    private void initTimeouts() {
        // Backwards compatibility with old config file
        if (connection.getConnectionTimeout() == 0) {
            connection.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
        }
        if (persistence.getPollingInterval() == 0) {
            persistence.setPollingInterval(DEFAULT_POLLING_INTERVAL);
        }
    }

    @Override
    public void destroy() throws Exception {
        scheduler.shutdownNow();
        callbackExecutor.shutdownNow();
        mqttSenderExecutor.shutdownNow();
        tbClient.disconnect();
    }

    @Override
    public String getTenantLabel() {
        return tenantLabel;
    }

    @Override
    public MqttDeliveryFuture onDeviceConnect(final String deviceName, final String deviceType) {
        final int msgId = msgIdSeq.incrementAndGet();
        byte[] msgData = toBytes(newNode().put("device", deviceName));
        log.info("[{}] Device Connected!", deviceName);
        devices.putIfAbsent(deviceName, new DeviceInfo(deviceName, deviceType));
        return persistMessage(GATEWAY_CONNECT_TOPIC, msgId, msgData, deviceName,
                message -> {
                    log.info("[{}][{}] Device connect event is reported to Thingsboard!", deviceName, msgId);
                },
                error -> log.warn("[{}][{}] Failed to report device connection!", deviceName, msgId, error));

    }

    @Override
    public Optional<MqttDeliveryFuture> onDeviceDisconnect(String deviceName) {
        if (devices.remove(deviceName) != null) {
            final int msgId = msgIdSeq.incrementAndGet();
            byte[] msgData = toBytes(newNode().put("device", deviceName));
            log.info("[{}][{}] Device Disconnected!", deviceName, msgId);
            return Optional.ofNullable(persistMessage(GATEWAY_DISCONNECT_TOPIC, msgId, msgData, deviceName,
                    message -> {
                        log.info("[{}][{}] Device disconnect event is delivered!", deviceName, msgId);
                    },
                    error -> log.warn("[{}][{}] Failed to report device disconnect!", deviceName, msgId, error)));
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
        return persistMessage(GATEWAY_ATTRIBUTES_TOPIC, msgId, toBytes(node), deviceName,
                message -> {
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
        return persistMessage(GATEWAY_TELEMETRY_TOPIC, msgId, toBytes(node), deviceName,
                message -> {
                    log.debug("[{}][{}] Device telemetry published to ThingsBoard!", msgId, deviceName);
                    telemetryCount.addAndGet(packSize);
                },
                error -> log.warn("[{}][{}] Failed to publish device telemetry!", deviceName, msgId, error));
    }


    private MqttDeliveryFuture persistMessage(String topic,
                                              int msgId,
                                              byte[] payload,
                                              String deviceId,
                                              Consumer<Void> onSuccess,
                                              Consumer<Throwable> onFailure) {
        try {
            return persistentFileService.persistMessage(topic, msgId, payload, deviceId, onSuccess, onFailure);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
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
        persistMessage(GATEWAY_REQUESTS_ATTRIBUTES_TOPIC, msgId, toBytes(node), deviceName,
                message -> {
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
        persistMessage(GATEWAY_RPC_TOPIC, msgId, toBytes(node), deviceName,
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

    /*
                            TODO: fix publish!

                            MqttMessage msg = new MqttMessage(toBytes(node));
                            tbClient.publish(DEVICE_GET_ATTRIBUTES_REQUEST_TOPIC, msg).waitForCompletion();

                            devices.forEach((k, v) -> onDeviceConnect(v.getName(), v.getType()));
                        } catch (Exception e) {
                            log.warn("Failed to connect to ThingsBoard!", e);
                            */
    private void checkDeviceConnected(String deviceName) {
        if (!devices.containsKey(deviceName)) {
            onDeviceConnect(deviceName, null);
        }
    }

    private void reportStats() {
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
        persistMessage(DEVICE_TELEMETRY_TOPIC, msgIdSeq.incrementAndGet(), toBytes(node), GATEWAY,
                token -> log.info("Gateway statistics {} reported!", node),
                error -> log.warn("Failed to report gateway statistics!", error));
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.warn("Lost connection to ThingsBoard. Attempting to reconnect");
        pendingAttrRequestsMap.clear();
        scheduler.schedule(tbClient::reconnect, CLIENT_RECONNECT_CHECK_INTERVAL, TimeUnit.SECONDS);
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
                log.warn("Failed to process arrived message!", message);
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
            if (extensionsConfigListener != null) {
                extensionsConfigListener.accept(configuration);
            }
            onAppliedConfiguration(configuration);
        } catch (Exception e) {
            log.warn("Failed to update extension configurations");
        }
    }

    @Override
    public void onAppliedConfiguration(String configuration) {
        byte[] msgData = toBytes(newNode().put("appliedConfiguration", configuration));
        persistMessage(DEVICE_ATTRIBUTES_TOPIC, msgIdSeq.incrementAndGet(), msgData, null, null,
                error ->
                        log.warn("Could not publish applied configuration", error));
    }

    @Override
    public void onConfigurationError(Exception e, TbExtensionConfiguration configuration) {
        String id = configuration.getId();
        byte[] msgDataError = toBytes(newNode().put(id + "ExtensionError", toString(e)));
        persistMessage(DEVICE_TELEMETRY_TOPIC, msgIdSeq.incrementAndGet(), msgDataError, null, null,
                error -> log.warn("Could not report extension error", error));

        byte[] msgDataStatus = toBytes(newNode().put(id + "ExtensionStatus", "Failure"));
        persistMessage(DEVICE_TELEMETRY_TOPIC, msgIdSeq.incrementAndGet(), msgDataStatus, null, null,
                error -> log.warn("Could not report extension error status", error));
    }

    @Override
    public void onConfigurationStatus(String id, String status) {
        byte[] extentionStatusData = toBytes(newNode().put(id + "ExtensionStatus", status));
        persistMessage(DEVICE_TELEMETRY_TOPIC, msgIdSeq.incrementAndGet(), extentionStatusData, null,
                message -> log.info("Reported status [{}] of extension [{}]", status, id),
                error -> log.warn("Extension status reporting failed", error));


        byte[] extentionErrorData = toBytes(newNode().put(id + "ExtensionError", ""));
        persistMessage(DEVICE_TELEMETRY_TOPIC, msgIdSeq.incrementAndGet(), extentionErrorData, null,
                null, error ->
                        log.warn("Extension error clearing failed", error));
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

    private void initMqttSender() {
        mqttSenderExecutor = Executors.newSingleThreadExecutor();
        mqttSenderExecutor.submit(new MqttMessageSender(persistence, connection, tbClient, persistentFileService));
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


    private MqttHandler getMqttHandler() {
        return new MqttHandlerImpl(extensionsConfigListener, persistentFileService);
    }

    private MqttClient initMqttClient() {
        try {
            MqttClientConfig mqttClientConfig = getMqttClientConfig();
            mqttClientConfig.setUsername(connection.getSecurity().getAccessToken());
            tbClient = MqttClient.create(mqttClientConfig);
            tbClient.setEventLoop(new NioEventLoopGroup(100));
            tbClient.setCallback(this);
            Promise<MqttConnectResult> connectResult = (Promise<MqttConnectResult>) tbClient.connect(connection.getHost(), connection.getPort());
            connectResult.addListener(future -> {
                if (future.isSuccess()) {
                    MqttConnectResult result = (MqttConnectResult) future.getNow();
                    log.debug("Gateway connect result code: [{}]", result.getReturnCode());
                } else {
                    log.error("Unable to connect to mqtt server!");
                    if (future.cause() != null) {
                        log.error(future.cause().getMessage(), future.cause());
                    }
                }
            });
            connectResult.get(connection.getConnectionTimeout(), TimeUnit.MILLISECONDS);

            MqttHandler mqttHandler = getMqttHandler();
            tbClient.on(DEVICE_ATTRIBUTES_TOPIC, mqttHandler).await(connection.getConnectionTimeout(), TimeUnit.MILLISECONDS);
            tbClient.on(DEVICE_GET_ATTRIBUTES_RESPONSE_PLUS_TOPIC, mqttHandler).await(connection.getConnectionTimeout(), TimeUnit.MILLISECONDS);
            tbClient.on(DEVICE_GET_ATTRIBUTES_RESPONSE_PLUS_TOPIC, mqttHandler).await(connection.getConnectionTimeout(), TimeUnit.MILLISECONDS);


            byte[] msgData = toBytes(newNode().put("shared", "configuration"));
            persistMessage(DEVICE_GET_ATTRIBUTES_REQUEST_TOPIC, msgIdSeq.incrementAndGet(), msgData, null,
                    null,
                    error -> log.warn("Error getiing attributes", error));
            return tbClient;
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            String message = "Unable to connect to ThingsBoard. Connection timed out after [" + connection.getConnectionTimeout() + "] milliseconds";
            log.error(message, e);
            throw new RuntimeException(message);
        }
    }

    private MqttClientConfig getMqttClientConfig() {
        MqttClientConfig mqttClientConfig;
        if (!StringUtils.isEmpty(connection.getSecurity().getAccessToken())) {
            mqttClientConfig = new MqttClientConfig();
            mqttClientConfig.setUsername(connection.getSecurity().getAccessToken());
        } else {
            try {
                SslContext sslCtx = initSslContext(connection.getSecurity());
                mqttClientConfig = new MqttClientConfig(sslCtx);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
        return mqttClientConfig;
    }

    private SslContext initSslContext(MqttGatewaySecurityConfiguration configuration) throws Exception {
        URL ksUrl = Resources.getResource(configuration.getKeystore());
        File ksFile = new File(ksUrl.toURI());
        URL tsUrl = Resources.getResource(configuration.getTruststore());
        File tsFile = new File(tsUrl.toURI());

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore trustStore = KeyStore.getInstance(JKS);
        try (InputStream tsFileInputStream = new FileInputStream(tsFile)) {
            trustStore.load(tsFileInputStream, configuration.getTruststorePassword().toCharArray());
        }
        tmf.init(trustStore);

        KeyStore keyStore = KeyStore.getInstance(JKS);
        try (InputStream ksFileInputStream = new FileInputStream(ksFile)) {
            keyStore.load(ksFileInputStream, configuration.getKeystorePassword().toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, configuration.getKeystorePassword().toCharArray());

        return SslContextBuilder.forClient().keyManager(kmf).trustManager(tmf).build();
    }

    public void setPersistentFileService(PersistentFileService persistentFileService) {
        this.persistentFileService = persistentFileService;
    }
}
