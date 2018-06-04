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
import com.google.common.io.Resources;
import io.netty.buffer.ByteBuf;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import nl.jk5.mqtt.MqttClient;
import nl.jk5.mqtt.MqttClientCallback;
import nl.jk5.mqtt.MqttClientConfig;
import nl.jk5.mqtt.MqttConnectResult;
import nl.jk5.mqtt.MqttHandler;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.thingsboard.gateway.service.MessageFuturePair;
import org.thingsboard.gateway.service.MqttDeliveryFuture;
import org.thingsboard.gateway.service.MqttMessageReceiver;
import org.thingsboard.gateway.service.MqttMessageSender;
import org.thingsboard.gateway.service.PersistentFileService;
import org.thingsboard.gateway.service.conf.TbConnectionConfiguration;
import org.thingsboard.gateway.service.conf.TbPersistenceConfiguration;
import org.thingsboard.gateway.service.conf.TbTenantConfiguration;
import org.thingsboard.gateway.service.data.AttributeRequest;
import org.thingsboard.gateway.service.data.AttributeRequestKey;
import org.thingsboard.gateway.service.data.AttributeRequestListener;
import org.thingsboard.gateway.service.data.AttributeResponse;
import org.thingsboard.gateway.service.data.RpcCommandData;
import org.thingsboard.gateway.service.data.RpcCommandResponse;
import org.thingsboard.gateway.util.JsonTools;
import org.thingsboard.server.common.data.kv.BooleanDataEntry;
import org.thingsboard.server.common.data.kv.DoubleDataEntry;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.LongDataEntry;
import org.thingsboard.server.common.data.kv.StringDataEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;

import javax.annotation.PostConstruct;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.thingsboard.gateway.util.JsonTools.fromString;
import static org.thingsboard.gateway.util.JsonTools.getKvEntries;
import static org.thingsboard.gateway.util.JsonTools.newNode;
import static org.thingsboard.gateway.util.JsonTools.putToNode;
import static org.thingsboard.gateway.util.JsonTools.toBytes;

/**
 * Created by ashvayka on 16.01.17.
 */
@Slf4j
public class MqttGatewayClient implements MqttHandler, MqttClientCallback {

    private static final String GATEWAY_RPC_TOPIC = "v1/gateway/rpc";
    private static final String GATEWAY_ATTRIBUTES_TOPIC = "v1/gateway/attributes";
    private static final String GATEWAY_TELEMETRY_TOPIC = "v1/gateway/telemetry";
    private static final String GATEWAY_REQUESTS_ATTRIBUTES_TOPIC = "v1/gateway/attributes/request";
    private static final String GATEWAY_RESPONSES_ATTRIBUTES_TOPIC = "v1/gateway/attributes/response";
    private static final String GATEWAY_CONNECT_TOPIC = "v1/gateway/connect";
    private static final String GATEWAY_DISCONNECT_TOPIC = "v1/gateway/disconnect";
    private static final String GATEWAY = "GATEWAY";

    private static final String DEVICE_RPC_REQUEST_TOPIC = "v1/devices/me/rpc/request";
    private static final String DEVICE_RPC_REQUEST_PLUS_TOPIC = "v1/devices/me/rpc/request/+";
    private static final String DEVICE_RPC_RESPONSE_TOPIC = "v1/devices/me/rpc/response";
    private static final String DEVICE_ATTRIBUTES_TOPIC = "v1/devices/me/attributes";
    private static final String DEVICE_TELEMETRY_TOPIC = "v1/devices/me/telemetry";
    private static final String DEVICE_GET_ATTRIBUTES_REQUEST_TOPIC = "v1/devices/me/attributes/request/";
    private static final String DEVICE_GET_ATTRIBUTES_RESPONSE_TOPIC = "v1/devices/me/attributes/response";
    private static final String DEVICE_GET_ATTRIBUTES_RESPONSE_PLUS_TOPIC = "v1/devices/me/attributes/response/+";

    private static final String JKS = "JKS";
    private static final long DEFAULT_CONNECTION_TIMEOUT = 10000;
    private static final long DEFAULT_POLLING_INTERVAL = 1000;

    private final AtomicLong attributesCount = new AtomicLong();
    private final AtomicLong telemetryCount = new AtomicLong();
    private final AtomicInteger msgIdSeq = new AtomicInteger();
    private final AtomicInteger requestSeq = new AtomicInteger();
    private final Map<AttributeRequestKey, AttributeRequestListener> pendingAttrRequestsMap = new ConcurrentHashMap<>();

    private PersistentFileService persistentFileService;

    private TbTenantConfiguration configuration;
    private TbConnectionConfiguration connection;
    private TbPersistenceConfiguration persistence;


    private volatile ObjectNode error;
    private MqttClient tbClient;
    private GatewayService gatewayService;

    private ExecutorService mqttSenderExecutor;
    private ExecutorService mqttReceiverExecutor;
    public  ExecutorService callbackExecutor = Executors.newCachedThreadPool();

    @Autowired
    private NioEventLoopGroup nioEventLoopGroup;


    public MqttGatewayClient(TbTenantConfiguration configuration, GatewayService service) {
        this.configuration = configuration;
        this.gatewayService = service;
    }

    @PostConstruct
    public void init() {
        BlockingQueue<MessageFuturePair> incomingQueue = new LinkedBlockingQueue<>();

        this.connection = configuration.getConnection();
        this.persistence = configuration.getPersistence();

        initTimeouts();
        initMqttClient();
        initMqttSender(incomingQueue);
        initMqttReceiver(incomingQueue);
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

    public void destroy() {
        callbackExecutor.shutdownNow();
        mqttSenderExecutor.shutdownNow();
        tbClient.disconnect();
    }

    public MqttDeliveryFuture sendDeviceConnect(final String deviceName, final String deviceType) {
        final int msgId = msgIdSeq.incrementAndGet();
        ObjectNode gwMsg = newNode().put("device", deviceName);
        if (deviceType != null) {
            gwMsg.put("type", deviceType);
        }

        byte[] msgData = toBytes(gwMsg);
        return persistMessage(GATEWAY_CONNECT_TOPIC, msgId, msgData, deviceName,
                message -> {
                    log.info("[{}][{}][{}] Device connect event is reported to Thingsboard!", deviceName, deviceType, msgId);
                },
                error -> log.warn("[{}][{}] Failed to report device connection!", deviceName, msgId, error));

    }

    public Optional<MqttDeliveryFuture> sendDeviceDisconnect(String deviceName) {
        final int msgId = msgIdSeq.incrementAndGet();
        byte[] msgData = toBytes(newNode().put("device", deviceName));
        return Optional.ofNullable(persistMessage(GATEWAY_DISCONNECT_TOPIC, msgId, msgData, deviceName,
                message -> {
                    log.info("[{}][{}] Device disconnect event is delivered!", deviceName, msgId);
                },
                error -> log.warn("[{}][{}] Failed to report device disconnect!", deviceName, msgId, error)));
    }

    public MqttDeliveryFuture sendDeviceAttributesUpdate(String deviceName, List<KvEntry> attributes) {
        final int msgId = msgIdSeq.incrementAndGet();
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

    public MqttDeliveryFuture sendDeviceTelemetry(String deviceName, List<TsKvEntry> telemetry) {
        final int msgId = msgIdSeq.incrementAndGet();
        ObjectNode node = newNode();
        Map<Long, List<TsKvEntry>> tsMap = telemetry.stream().collect(Collectors.groupingBy(TsKvEntry::getTs));
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
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public void sendDeviceAttributeRequest(AttributeRequest request, Consumer<AttributeResponse> listener) {
        final int msgId = msgIdSeq.incrementAndGet();
        String deviceName = request.getDeviceName();
        AttributeRequestKey requestKey = new AttributeRequestKey(request.getRequestId(), request.getDeviceName());

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

    public void sendDeviceRpcResponse(RpcCommandResponse response) {
        final int msgId = msgIdSeq.incrementAndGet();
        int requestId = response.getRequestId();
        String deviceName = response.getDeviceName();
        String data = response.getData();

        ObjectNode node = newNode();
        node.put("id", requestId);
        node.put("device", deviceName);
        node.put("data", fromString(data));
        persistMessage(GATEWAY_RPC_TOPIC, msgId, toBytes(node), deviceName,
                token -> {
                    log.debug("[{}][{}] RPC response from device was delivered!", deviceName, requestId);
                },
                error -> {
                    log.warn("[{}][{}] Failed to report RPC response from device!", deviceName, requestId, error);
                });
    }

    public void onError(Exception e) {
        onError(null, e);
    }

    public void onError(String deviceName, Exception e) {
        ObjectNode node = newNode();
        node.put("ts", System.currentTimeMillis());
        if (deviceName != null) {
            node.put("device", deviceName);
        }
        node.put("error", toString(e));
        error = node;
    }

    public void sendStatsReport(ObjectNode values) {
        try {
            ObjectNode node = newNode();
            node.put("ts", System.currentTimeMillis());
            ObjectNode valuesNode = node.putObject("values");

            valuesNode.setAll(values);
            valuesNode.put("attributesUploaded", attributesCount.getAndSet(0));
            valuesNode.put("telemetryUploaded", telemetryCount.getAndSet(0));
            if (error != null) {
                valuesNode.put("latestError", JsonTools.toString(error));
                error = null;
            }
            persistMessage(DEVICE_TELEMETRY_TOPIC, msgIdSeq.incrementAndGet(), toBytes(node), GATEWAY,
                    token -> log.info("Gateway statistics {} reported!", node),
                    error -> log.warn("Failed to report gateway statistics!", error));
        } catch (Throwable e) {
            log.error("Failed to persist statistics message!", e);
        }
    }

    @Override
    public void onMessage(String topic, ByteBuf payload) {
        String message = payload.toString(StandardCharsets.UTF_8);

        log.trace("Message arrived [{}] {}", topic, message);
        callbackExecutor.submit(() -> {
            try {
                if (topic.equals(GATEWAY_ATTRIBUTES_TOPIC)) {
                    onAttributesUpdate(message);
                } else if (topic.equals(GATEWAY_RESPONSES_ATTRIBUTES_TOPIC)) {
                    onDeviceAttributesResponse(message);
                } else if (topic.equals(GATEWAY_RPC_TOPIC)) {
                    onRpcCommand(message);
                } else if (topic.startsWith(DEVICE_RPC_REQUEST_TOPIC)) {
                    // TODO: add RPC command process
                } else if (topic.equals(DEVICE_ATTRIBUTES_TOPIC)) {
                    onGatewayAttributesUpdate(message);
                } else if (topic.startsWith(DEVICE_GET_ATTRIBUTES_RESPONSE_TOPIC)) {
                    onGatewayAttributesUpdate(message);
                }
            } catch (Exception e) {
                log.warn("Failed to process arrived message!", message);
            }
        });

    }

    @Override
    public void connectionLost(Throwable throwable) {
        log.warn("Lost connection to ThingsBoard. Attempting to reconnect..");
    }

    private void onAttributesUpdate(String message) {
        JsonNode payload = fromString(message);
        String deviceName = payload.get("device").asText();
        JsonNode data = payload.get("data");
        List<KvEntry> attributes = getKvEntries(data);

        gatewayService.onAttributesUpdate(deviceName, attributes);
    }

    private void onRpcCommand(String message) {
        JsonNode payload = fromString(message);
        String deviceName = payload.get("device").asText();
        JsonNode data = payload.get("data");
        RpcCommandData rpcCommand = new RpcCommandData();
        rpcCommand.setRequestId(data.get("id").asInt());
        rpcCommand.setMethod(data.get("method").asText());
        rpcCommand.setParams(JsonTools.toString(data.get("params")));

        gatewayService.onRpcCommand(deviceName, rpcCommand);
    }

    private void onGatewayAttributesUpdate(String message) {

        gatewayService.onGatewayAttributesUpdate(message);
    }

    private void onDeviceAttributesResponse(String message) {
        JsonNode payload = fromString(message);
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

    public MqttDeliveryFuture sendGatewayAttributesUpdate(ObjectNode attributes) {
        final int msgId = msgIdSeq.incrementAndGet();
        return persistMessage(DEVICE_ATTRIBUTES_TOPIC, msgId, toBytes(attributes), null,
                message -> {
                    log.debug("[{}] Gateway attributes were delivered! {}", msgId, attributes.toString());
                },
                error -> log.warn("[{}] Failed to report gateway attributes! {}", msgId, error, attributes.toString()));
    }

    public MqttDeliveryFuture sendGatewayTelemetry(ObjectNode telemetry) {
        final int msgId = msgIdSeq.incrementAndGet();
        return persistMessage(DEVICE_TELEMETRY_TOPIC, msgId, toBytes(telemetry), null,
                message -> {
                    log.debug("[{}] Gateway telemetry published to ThingsBoard! {}", msgId, telemetry.toString());
                },
                error -> log.warn("[{}] Failed to publish gateway telemetry! {}", msgId, error, telemetry.toString()));
    }

    public void sendGatewayAttributeRequest(String requestKey, boolean clientScope) {
        final int msgId = msgIdSeq.incrementAndGet();
        final int requestId = requestSeq.incrementAndGet();

        ObjectNode node = newNode();
        if (clientScope) {
            node.put("clientKeys", requestKey);
        } else {
            node.put("sharedKeys", requestKey);
        }

        MqttMessage msg = new MqttMessage(toBytes(node));

        msg.setId(msgId);
        persistMessage(DEVICE_GET_ATTRIBUTES_REQUEST_TOPIC + requestId, msgId, toBytes(node), GATEWAY,
                message -> {
                    log.debug("[{}] Gateway attributes request was delivered!", msgId);
                },
                error -> {
                    log.warn("[{}] Failed to request gateway attributes!", msgId, error);
                });
    }

    public void sendGatewayRpcResponse(RpcCommandResponse response) {
        // TODO
    }

    private void initMqttSender(BlockingQueue<MessageFuturePair> incomingQueue) {
        mqttSenderExecutor = Executors.newSingleThreadExecutor();
        mqttSenderExecutor.submit(new MqttMessageSender(persistence, connection, tbClient, persistentFileService, incomingQueue));
    }

    private void initMqttReceiver(BlockingQueue<MessageFuturePair> incomingQueue) {
        mqttReceiverExecutor = Executors.newSingleThreadExecutor();
        mqttReceiverExecutor.submit(new MqttMessageReceiver(persistentFileService, incomingQueue, connection.getIncomingQueueWarningThreshold()));
    }

    private static String toString(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    private MqttClient initMqttClient() {
        try {
            MqttClientConfig mqttClientConfig = getMqttClientConfig();
            mqttClientConfig.setUsername(connection.getSecurity().getAccessToken());
            tbClient = MqttClient.create(mqttClientConfig);
            tbClient.setCallback(this);
            tbClient.setEventLoop(nioEventLoopGroup);
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

            tbClient.on(DEVICE_RPC_REQUEST_PLUS_TOPIC, this).await(connection.getConnectionTimeout(), TimeUnit.MILLISECONDS);
            tbClient.on(DEVICE_ATTRIBUTES_TOPIC, this).await(connection.getConnectionTimeout(), TimeUnit.MILLISECONDS);
            tbClient.on(DEVICE_GET_ATTRIBUTES_RESPONSE_PLUS_TOPIC, this).await(connection.getConnectionTimeout(), TimeUnit.MILLISECONDS);

            tbClient.on(GATEWAY_RPC_TOPIC, this).await(connection.getConnectionTimeout(), TimeUnit.MILLISECONDS);
            tbClient.on(GATEWAY_ATTRIBUTES_TOPIC, this).await(connection.getConnectionTimeout(), TimeUnit.MILLISECONDS);
            tbClient.on(GATEWAY_RESPONSES_ATTRIBUTES_TOPIC, this).await(connection.getConnectionTimeout(), TimeUnit.MILLISECONDS);

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
