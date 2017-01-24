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
package org.thingsboard.gateway.service;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.internal.security.SSLSocketFactoryFactory;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.thingsboard.gateway.service.conf.TbConnectionConfiguration;
import org.thingsboard.gateway.service.conf.TbReportingConfiguration;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;
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

    private final UUID clientId = UUID.randomUUID();
    private final ConcurrentMap<String, DeviceInfo> devices = new ConcurrentHashMap<>();

    @Autowired
    private TbConnectionConfiguration connection;

    @Autowired
    private TbReportingConfiguration reporting;

    private MqttAsyncClient tbClient;
    private MqttConnectOptions tbClientOptions;

    private Object connectLock = new Object();

    private ScheduledExecutorService scheduler;

    private AtomicLong attributesCount = new AtomicLong();
    private AtomicLong telemetryCount = new AtomicLong();


    @PostConstruct
    public void init() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(this::reportStats, reporting.getInterval(), reporting.getInterval(), TimeUnit.MILLISECONDS);

        tbClientOptions = new MqttConnectOptions();
        tbClientOptions.setCleanSession(true);
        tbClientOptions.setMaxInflight(connection.getMaxInFlight());

        MqttGatewaySecurityConfiguration security = setupSecurityOptions(tbClientOptions);

        tbClient = new MqttAsyncClient((security.isSsl() ? "ssl" : "tcp") + "://" + connection.getHost() + ":" + connection.getPort(),
                clientId.toString(), new MemoryPersistence());
        tbClient.setCallback(this);
        checkConnection();
    }

    private void reportStats() {
        if (tbClient == null) {
            log.info("Can't report stats because client was not initialized yet!");
            return;
        }
        ObjectNode node = newNode();
        node.put("devicesOnline", devices.size());
        node.put("attributesUploaded", attributesCount.getAndSet(0));
        node.put("telemetryUploaded", telemetryCount.getAndSet(0));
        publishAsync("v1/devices/me/telemetry", new MqttMessage(toBytes(node)),
                token -> log.debug("Gateway statistics {} reported!", node),
                error -> log.warn("Failed to report gateway statistics!", error));
    }


    @PreDestroy
    public void preDestroy() throws Exception {
        scheduler.shutdownNow();
        tbClient.disconnect();
    }

    @Override
    public void connect(final String deviceName) {
        byte[] msgData = toBytes(newNode().put("device", deviceName));
        MqttMessage msg = new MqttMessage(msgData);
        publishSync("v1/gateway/connect", msg,
                token -> {
                    log.info("[{}] Device Connected!", deviceName);
                    devices.putIfAbsent(deviceName, new DeviceInfo(deviceName));
                },
                error -> log.warn("[{}] Failed to report device connection!", deviceName, error));
    }

    @Override
    public void disconnect(String deviceName) {
        byte[] msgData = toBytes(newNode().put("device", deviceName));
        MqttMessage msg = new MqttMessage(msgData);
        publishSync("v1/gateway/disconnect", msg,
                token -> {
                    log.info("[{}] Device Disconnected!", deviceName);
                    devices.remove(deviceName);
                },
                error -> log.warn("[{}] Failed to report device disconnect!", deviceName, error));
    }

    @Override
    public void onDeviceAttributesUpdate(String deviceName, List<KvEntry> attributes) {
        log.trace("[{}] Updating device attributes: {}", deviceName, attributes);
        checkDeviceConnected(deviceName);
        ObjectNode node = newNode();
        ObjectNode deviceNode = node.putObject(deviceName);
        attributes.forEach(kv -> putToNode(deviceNode, kv));
        final int packSize = attributes.size();
        publishAsync("v1/gateway/attributes", new MqttMessage(toBytes(node)),
                token -> {
                    log.debug("[{}] Device attributes published!", deviceName);
                    attributesCount.addAndGet(packSize);
                },
                error -> log.warn("[{}] Failed to report device attributes!", deviceName, error));
    }

    @Override
    public void onDeviceTelemetry(String deviceName, List<TsKvEntry> telemetry) {
        log.trace("[{}] Updating device telemetry: {}", deviceName, telemetry);
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
        publishAsync("v1/gateway/telemetry", new MqttMessage(toBytes(node)),
                token -> {
                    log.debug("[{}] Device telemetry published!", deviceName);
                    telemetryCount.addAndGet(packSize);
                },
                error -> log.warn("[{}] Failed to publish device telemetry!", deviceName, error));

    }

    @Override
    public void onError(String deviceName, Exception e) {

    }

    @Override
    public void onError(Exception e) {

    }

    private void checkConnection() {
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
                        devices.forEach((k, v) -> connect(k));
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
            connect(deviceName);
        }
    }

    private void publishAsync(final String topic, MqttMessage msg, Consumer<IMqttToken> onSuccess, Consumer<Throwable> onFailure) {
        publish(topic, msg, false, onSuccess, onFailure);
    }

    private void publishSync(final String topic, MqttMessage msg, Consumer<IMqttToken> onSuccess, Consumer<Throwable> onFailure) {
        publish(topic, msg, true, onSuccess, onFailure);
    }

    private void publish(final String topic, MqttMessage msg, boolean sync, Consumer<IMqttToken> onSuccess, Consumer<Throwable> onFailure) {
        try {
            checkConnection();
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
            if (sync) {
                token.waitForCompletion();
            }
        } catch (MqttException e) {
            onFailure.accept(e);
        }
    }

    private MqttGatewaySecurityConfiguration setupSecurityOptions(MqttConnectOptions options) {
        MqttGatewaySecurityConfiguration security = connection.getSecurity();
        if (security.isTokenBased()) {
            options.setUserName(security.getAccessToken());
            if (!StringUtils.isEmpty(security.getTruststore())) {
                Properties sslProperties = new Properties();
                sslProperties.put(SSLSocketFactoryFactory.TRUSTSTORE, security.getTruststore());
                sslProperties.put(SSLSocketFactoryFactory.TRUSTSTOREPWD, security.getTruststorePassword());
                sslProperties.put(SSLSocketFactoryFactory.TRUSTSTORETYPE, "JKS");
                sslProperties.put(SSLSocketFactoryFactory.CLIENTAUTH, false);
                options.setSSLProperties(sslProperties);
            }
        } else {
            Properties sslProperties = new Properties();
            sslProperties.put(SSLSocketFactoryFactory.KEYSTORE, security.getKeystore());
            sslProperties.put(SSLSocketFactoryFactory.KEYSTOREPWD, security.getKeystorePassword());
            sslProperties.put(SSLSocketFactoryFactory.KEYSTORETYPE, "JKS");
            sslProperties.put(SSLSocketFactoryFactory.TRUSTSTORE, security.getTruststore());
            sslProperties.put(SSLSocketFactoryFactory.TRUSTSTOREPWD, security.getTruststorePassword());
            sslProperties.put(SSLSocketFactoryFactory.TRUSTSTORETYPE, "JKS");
            sslProperties.put(SSLSocketFactoryFactory.CLIENTAUTH, true);
            options.setSSLProperties(sslProperties);
        }
        return security;
    }

    @Override
    public void connectionLost(Throwable cause) {
        checkConnection();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {

    }
}