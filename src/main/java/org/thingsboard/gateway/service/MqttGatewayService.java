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
package org.thingsboard.gateway.service;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.thingsboard.gateway.service.conf.TbConnectionConfiguration;
import org.thingsboard.gateway.service.conf.TbPersistenceConfiguration;
import org.thingsboard.gateway.service.conf.TbReportingConfiguration;
import org.thingsboard.gateway.util.JsonTools;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.thingsboard.gateway.util.JsonTools.*;

/**
 * Created by ashvayka on 16.01.17.
 */
@Service
@Slf4j
public class MqttGatewayService implements GatewayService, MqttCallback {

    private static final long CLIENT_RECONNECT_CHECK_INTERVAL = 1;
    private final ConcurrentMap<String, DeviceInfo> devices = new ConcurrentHashMap<>();
    private final AtomicLong attributesCount = new AtomicLong();
    private final AtomicLong telemetryCount = new AtomicLong();
    private final AtomicInteger msgIdSeq = new AtomicInteger();
    private volatile ObjectNode error;

    @Autowired
    private TbConnectionConfiguration connection;

    @Autowired
    private TbReportingConfiguration reporting;

    @Autowired
    private TbPersistenceConfiguration persistence;


    private MqttAsyncClient tbClient;
    private MqttConnectOptions tbClientOptions;

    private Object connectLock = new Object();

    private ScheduledExecutorService scheduler;


    @PostConstruct
    public void init() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(this::reportStats, reporting.getInterval(), reporting.getInterval(), TimeUnit.MILLISECONDS);

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
    }

    @PreDestroy
    public void preDestroy() throws Exception {
        scheduler.shutdownNow();
        tbClient.disconnect();
    }

    @Override
    public void onDeviceConnect(final String deviceName) {
        final int msgId = msgIdSeq.incrementAndGet();
        byte[] msgData = toBytes(newNode().put("device", deviceName));
        MqttMessage msg = new MqttMessage(msgData);
        msg.setId(msgId);
        log.info("[{}][{}] Device Connected!", deviceName, msgId);
        devices.putIfAbsent(deviceName, new DeviceInfo(deviceName));
        publishAsync("v1/gateway/connect", msg,
                token -> {
                    log.info("[{}][{}] Device connect event is reported to Thingsboard!", deviceName, msgId);
                },
                error -> log.warn("[{}][{}] Failed to report device connection!", deviceName, msgId, error));
    }

    @Override
    public void onDeviceDisconnect(String deviceName) {
        final int msgId = msgIdSeq.incrementAndGet();
        byte[] msgData = toBytes(newNode().put("device", deviceName));
        MqttMessage msg = new MqttMessage(msgData);
        msg.setId(msgId);
        log.info("[{}][{}] Device Disconnected!", deviceName, msgId);
        devices.remove(deviceName);
        publishAsync("v1/gateway/disconnect", msg,
                token -> {
                    log.info("[{}][{}] Device disconnect event is delivered!", deviceName, msgId);
                },
                error -> log.warn("[{}][{}] Failed to report device disconnect!", deviceName, msgId, error));
    }

    @Override
    public void onDeviceAttributesUpdate(String deviceName, List<KvEntry> attributes) {
        final int msgId = msgIdSeq.incrementAndGet();
        log.trace("[{}][{}] Updating device attributes: {}", deviceName, msgId, attributes);
        checkDeviceConnected(deviceName);
        ObjectNode node = newNode();
        ObjectNode deviceNode = node.putObject(deviceName);
        attributes.forEach(kv -> putToNode(deviceNode, kv));
        final int packSize = attributes.size();
        MqttMessage msg = new MqttMessage(toBytes(node));
        msg.setId(msgId);
        publishAsync("v1/gateway/attributes", msg,
                token -> {
                    log.debug("[{}][{}] Device attributes were delivered!", deviceName, msgId);
                    attributesCount.addAndGet(packSize);
                },
                error -> log.warn("[{}][{}] Failed to report device attributes!", deviceName, msgId, error));
    }

    @Override
    public void onDeviceTelemetry(String deviceName, List<TsKvEntry> telemetry) {
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
        publishAsync("v1/gateway/telemetry", msg,
                token -> {
                    log.debug("[{}][{}] Device telemetry published to Thingsboard!", msgId, deviceName);
                    telemetryCount.addAndGet(packSize);
                },
                error -> log.warn("[{}][{}] Failed to publish device telemetry!", deviceName, msgId, error));

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
                        devices.forEach((k, v) -> onDeviceConnect(k));
                    } catch (MqttException e) {
                        log.warn("Failed to connect to Thingsboard!", e);
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
            onDeviceConnect(deviceName);
        }
    }

    private void publishAsync(final String topic, MqttMessage msg, Consumer<IMqttToken> onSuccess, Consumer<Throwable> onFailure) {
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
        } catch (MqttException e) {
            onFailure.accept(e);
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
        publishAsync("v1/devices/me/telemetry", msg,
                token -> log.info("Gateway statistics {} reported!", node),
                error -> log.warn("Failed to report gateway statistics!", error));
    }

    @Override
    public void connectionLost(Throwable cause) {
        scheduler.schedule(this::checkClientReconnected, CLIENT_RECONNECT_CHECK_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        log.trace("Message arrived [{}] {}", topic, message.getId());
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        log.trace("Delivery complete [{}]", token);
    }

    private void checkClientReconnected() {
        if (tbClient.isConnected()) {
            devices.forEach((k, v) -> onDeviceConnect(k));
        } else {
            scheduler.schedule(this::checkClientReconnected, CLIENT_RECONNECT_CHECK_INTERVAL, TimeUnit.SECONDS);
        }
    }

    private static String toString(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
