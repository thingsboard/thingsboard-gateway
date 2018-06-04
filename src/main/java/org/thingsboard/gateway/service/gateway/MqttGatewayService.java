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
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.thingsboard.gateway.service.AttributesUpdateListener;
import org.thingsboard.gateway.service.MqttDeliveryFuture;
import org.thingsboard.gateway.service.PersistentFileService;
import org.thingsboard.gateway.service.RpcCommandListener;
import org.thingsboard.gateway.service.conf.TbExtensionConfiguration;
import org.thingsboard.gateway.service.conf.TbReportingConfiguration;
import org.thingsboard.gateway.service.conf.TbTenantConfiguration;
import org.thingsboard.gateway.service.data.*;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;

import javax.annotation.PostConstruct;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.thingsboard.gateway.util.JsonTools.fromString;
import static org.thingsboard.gateway.util.JsonTools.newNode;

/**
 * Created by ashvayka on 16.01.17.
 */
@Slf4j
public class MqttGatewayService implements GatewayService {
    private final ConcurrentMap<String, DeviceInfo> devices = new ConcurrentHashMap<>();
    private final Set<AttributesUpdateSubscription> attributeUpdateSubs = ConcurrentHashMap.newKeySet();
    private final Set<RpcCommandSubscription> rpcCommandSubs = ConcurrentHashMap.newKeySet();

    private String tenantLabel;

    private Consumer<String> extensionsConfigListener;
    private TbTenantConfiguration configuration;
    private TbReportingConfiguration reporting;

    private MqttGatewayClient mqttClient;

    private ScheduledExecutorService scheduler;

    public MqttGatewayService(TbTenantConfiguration configuration, Consumer<String> extensionsConfigListener) {
        this.configuration = configuration;
        this.extensionsConfigListener = extensionsConfigListener;
        this.mqttClient = new MqttGatewayClient(configuration, this);

    }

    @Override
    @PostConstruct
    public void init() {
        this.tenantLabel = configuration.getLabel();
        this.reporting = configuration.getReporting();

        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::reportStats, 0, reporting.getInterval(), TimeUnit.MILLISECONDS);

        initMqttClient();

        mqttClient.sendGatewayAttributeRequest("configuration", false);
    }

    @Override
    public void destroy() throws Exception {
        mqttClient.destroy();
        scheduler.shutdownNow();
    }

    @Override
    public String getTenantLabel() {
        return tenantLabel;
    }

    @Override
    public MqttDeliveryFuture onDeviceConnect(final String deviceName, final String deviceType) {
        log.info("[{}] Device Connected!", deviceName);

        devices.putIfAbsent(deviceName, new DeviceInfo(deviceName, deviceType));

        return mqttClient.sendDeviceConnect(deviceName, deviceType);
    }

    @Override
    public Optional<MqttDeliveryFuture> onDeviceDisconnect(String deviceName) {
        if (deviceName != null && devices.remove(deviceName) != null) {
            log.info("[{}][{}] Device Disconnected!", deviceName);

            return mqttClient.sendDeviceDisconnect(deviceName);
        } else {
            log.debug("[{}] Device was disconnected before. Nothing is going to happened.", deviceName);

            return Optional.empty();
        }
    }

    @Override
    public MqttDeliveryFuture onDeviceAttributesUpdate(String deviceName, List<KvEntry> attributes) {
        log.trace("[{}] Updating device attributes: {}", deviceName, attributes);

        checkDeviceConnected(deviceName);

        return mqttClient.sendDeviceAttributesUpdate(deviceName, attributes);
    }

    @Override
    public MqttDeliveryFuture onDeviceTelemetry(String deviceName, List<TsKvEntry> telemetry) {
        log.trace("[{}] Updating device telemetry: {}", deviceName, telemetry);

        checkDeviceConnected(deviceName);

        return mqttClient.sendDeviceTelemetry(deviceName, telemetry);
    }

    @Override
    public void onDeviceAttributeRequest(AttributeRequest request, Consumer<AttributeResponse> listener) {
        String deviceName = request.getDeviceName();

        log.trace("[{}] Requesting {} attribute: {}", deviceName, request.isClientScope() ? "client" : "shared", request.getAttributeKey());

        checkDeviceConnected(deviceName);

        mqttClient.sendDeviceAttributeRequest(request, listener);
    }

    @Override
    public void onDeviceRpcResponse(RpcCommandResponse response) {
        int requestId = response.getRequestId();
        String deviceName = response.getDeviceName();
        String data = response.getData();

        log.trace("[{}][{}] RPC response data: {}", deviceName, requestId, data);
        checkDeviceConnected(deviceName);

        mqttClient.sendDeviceRpcResponse(response);
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
        mqttClient.onError(e);
    }

    @Override
    public void onError(String deviceName, Exception e) {
        mqttClient.onError(deviceName, e);
    }

    private void checkDeviceConnected(String deviceName) {
        if (!devices.containsKey(deviceName)) {
            onDeviceConnect(deviceName, null);
        }
    }

    private void reportStats() {
        ObjectNode node = newNode();
        node.put("devicesOnline", devices.size());

        mqttClient.sendStatsReport(node);
    }

    @Override
    public void onAttributesUpdate(String deviceName, List<KvEntry> attributes) {
        Set<AttributesUpdateListener> listeners = attributeUpdateSubs.stream()
                .filter(sub -> sub.matches(deviceName)).map(sub -> sub.getListener())
                .collect(Collectors.toSet());
        if (!listeners.isEmpty()) {
            listeners.forEach(listener -> mqttClient.callbackExecutor.submit(() -> {
                try {
                    listener.onAttributesUpdated(deviceName, attributes);
                } catch (Exception e) {
                    log.error("[{}] Failed to process attributes update", deviceName, e);
                }
            }));
        }
    }

    @Override
    public void onRpcCommand(String deviceName, RpcCommandData command) {
        Set<RpcCommandListener> listeners = rpcCommandSubs.stream()
                .filter(sub -> sub.matches(deviceName)).map(RpcCommandSubscription::getListener)
                .collect(Collectors.toSet());
        if (!listeners.isEmpty()) {
            listeners.forEach(listener -> mqttClient.callbackExecutor.submit(() -> {
                try {
                    listener.onRpcCommand(deviceName, command);
                } catch (Exception e) {
                    log.error("[{}][{}] Failed to process rpc command", deviceName, command.getRequestId(), e);
                }
            }));
        } else {
            log.warn("No listener registered for RPC command to device {}!", deviceName);
        }
    }

    @Override
    public void onGatewayAttributesUpdate(String message) {
        log.info("Configuration arrived! {}", message);
        JsonNode payload = fromString(message);
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
            log.warn("Failed to update extension configurations [[]]", e.getMessage(), e);
        }
    }

    @Override
    public void onAppliedConfiguration(String configuration) {
        mqttClient.sendGatewayAttributesUpdate(newNode().put("appliedConfiguration", configuration));
    }

    @Override
    public void onConfigurationError(Exception e, TbExtensionConfiguration configuration) {
        String id = configuration.getId();
        mqttClient.sendGatewayTelemetry(newNode().put(id + "ExtensionError", toString(e)));
        mqttClient.sendGatewayTelemetry(newNode().put(id + "ExtensionStatus", "Failure"));
    }

    @Override
    public void onConfigurationStatus(String id, String status) {
        mqttClient.sendGatewayTelemetry(newNode().put(id + "ExtensionStatus", status));
        mqttClient.sendGatewayTelemetry(newNode().put(id + "ExtensionError", ""));
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

    private MqttGatewayClient initMqttClient() {
        mqttClient.init();

        return mqttClient;
    }

    public void setPersistentFileService(PersistentFileService persistentFileService) {
        mqttClient.setPersistentFileService(persistentFileService);
    }
}
