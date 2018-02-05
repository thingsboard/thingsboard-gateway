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
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import nl.jk5.mqtt.MqttHandler;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.thingsboard.gateway.service.AttributesUpdateListener;
import org.thingsboard.gateway.service.MqttDeliveryFuture;
import org.thingsboard.gateway.service.PersistentFileService;
import org.thingsboard.gateway.service.RpcCommandListener;
import org.thingsboard.gateway.service.data.*;
import org.thingsboard.gateway.util.JsonTools;
import org.thingsboard.server.common.data.kv.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.thingsboard.gateway.util.JsonTools.*;
import static org.thingsboard.gateway.service.gateway.MqttGatewayService.*;

/**
 * Created by Valerii Sosliuk on 1/23/2018.
 */
@Slf4j
//@Component
public class MqttHandlerImpl implements MqttHandler {

    private final AtomicInteger msgIdSeq = new AtomicInteger();

    private final Set<AttributesUpdateSubscription> attributeUpdateSubs = ConcurrentHashMap.newKeySet();
    private final Set<RpcCommandSubscription> rpcCommandSubs = ConcurrentHashMap.newKeySet();

    private ExecutorService callbackExecutor = Executors.newCachedThreadPool();
    private Consumer<String> extensionsConfigListener;
    private final Map<AttributeRequestKey, AttributeRequestListener> pendingAttrRequestsMap = new ConcurrentHashMap<>();

    private PersistentFileService persistentFileService;

    public MqttHandlerImpl(Consumer<String> extensionsConfigListener, PersistentFileService persistentFileService) {
        this.extensionsConfigListener = extensionsConfigListener;
        this.persistentFileService = persistentFileService;
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

    private void onAttributesUpdate(String message) {
        JsonNode payload = fromString(message);
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

    private void onRpcCommand(String message) {
        JsonNode payload = fromString(message);
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

    private void onGatewayAttributesGet(String message) {
        log.info("Configuration arrived! {}", message);
        JsonNode payload = fromString(message);
        if (payload.get("shared").get("configuration") != null) {
            String configuration = payload.get("shared").get("configuration").asText();
            if (!StringUtils.isEmpty(configuration)) {
                updateConfiguration(configuration);
            }
        }
    }

    private void onGatewayAttributesUpdate(String message) {
        log.info("Configuration updates arrived! {}", message);
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

    public void onAppliedConfiguration(String configuration) {
        byte[] msgData = toBytes(newNode().put("appliedConfiguration", configuration));
        persistMessage(DEVICE_ATTRIBUTES_TOPIC, msgIdSeq.incrementAndGet(), msgData, null, null,
                error ->
                        log.warn("Could not publish applied configuration", error));
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
}
