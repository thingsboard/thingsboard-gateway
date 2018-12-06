/**
 * Copyright © 2016-2018 The Thingsboard Authors
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
package org.thingsboard.gateway.extensions.opc.rpc;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadResponse;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.eclipse.milo.opcua.stack.core.types.structured.WriteResponse;
import org.eclipse.milo.opcua.stack.core.types.structured.WriteValue;
import org.thingsboard.gateway.extensions.opc.OpcUaDevice;
import org.thingsboard.gateway.extensions.opc.OpcUaDeviceAware;
import org.thingsboard.gateway.extensions.opc.scan.OpcUaNode;
import org.thingsboard.gateway.extensions.opc.util.OpcUaUtils;
import org.thingsboard.gateway.service.RpcCommandListener;
import org.thingsboard.gateway.service.data.RpcCommandData;
import org.thingsboard.gateway.service.data.RpcCommandResponse;
import org.thingsboard.gateway.service.gateway.GatewayService;
import org.thingsboard.gateway.util.JsonTools;

import java.util.*;

@Slf4j
public class RpcProcessor implements RpcCommandListener {
    private static final String RESPONSE_STATUS_OK = "ok";
    private static final String RESPONSE_FIELD_ERROR = "error";

    private static final String RPC_WRITE = "write";

    private final GatewayService gateway;
    private final OpcUaClient client;
    private final OpcUaDeviceAware deviceContainer;

    public RpcProcessor(GatewayService gateway, OpcUaClient client, OpcUaDeviceAware deviceContainer) {
        this.gateway = gateway;
        this.client = client;
        this.deviceContainer = deviceContainer;
    }

    @Override
    public void onRpcCommand(String deviceName, RpcCommandData command) {
        log.debug("RPC received: device='{}', command={}", deviceName, command);

        OpcUaDevice device = deviceContainer.getDevice(deviceName);
        if (device == null) {
            log.warn("No device '{}' found for RPC {}", deviceName, command);
            gateway.onDeviceRpcResponse(createErrorResponse(command.getRequestId(),
                                        deviceName,
                                        String.format("No device '%s' found", deviceName)));
            return;
        }

        if (!RPC_WRITE.equals(command.getMethod())) {
            log.warn("Unknown RPC method '{}'", command.getMethod());
            gateway.onDeviceRpcResponse(createErrorResponse(command.getRequestId(),
                                        deviceName,
                                        String.format("Unsupported RPC method '%s'", command.getMethod())));
            return;
        }

        Map<String, String> results = new HashMap<>();
        HashMap<String, Object> values = JsonTools.fromStringToMap(command.getParams());

        Map<String, NodeId> nodeIds = resolveNodeIds(device, values.keySet());
        Set<String> notFoundTags = Sets.difference(values.keySet(), nodeIds.keySet());

        if (!notFoundTags.isEmpty()) {
            log.warn("Failed to find tags {}", notFoundTags);
            results.putAll(createTagsErrorResponse(notFoundTags, "No tag found"));
        }

        Map<String, NodeId> types = requestOpcTypes(nodeIds);
        Set<String> notResolvedTags = Sets.difference(nodeIds.keySet(), types.keySet());

        if (!notResolvedTags.isEmpty()) {
            log.warn("Failed to resolve OPC type for tags {}", notResolvedTags);
            results.putAll(createTagsErrorResponse(notResolvedTags, "Failed to resolve OPC type"));
        }

        try {
            List<WriteValue> request = createWriteRequest(nodeIds, types, values, results);

            log.trace("Writing values to OPC server for tags {}", types.keySet());

            WriteResponse writeResponse = client.write(request).get();
            results.putAll(processWriteResponse(writeResponse, types));
        } catch (Exception e) {
            log.warn("OPC write failed", e);
            results.putAll(createTagsErrorResponse(types.keySet(), "OPC write failed: " + e.getMessage()));
        }

        gateway.onDeviceRpcResponse(createResponse(command.getRequestId(), deviceName, results));
    }

    private List<WriteValue> createWriteRequest(Map<String, NodeId> tags,
                                                Map<String, NodeId> types,
                                                Map<String, Object> values,
                                                Map<String, String> results) {
        List<WriteValue> opcValues = new LinkedList<>();
        Set<String> failedTags= new HashSet<>();

        types.forEach((tag, typeNodeId) -> {
            Object rawValue = values.get(tag);
            try {
                Variant opcValue = OpcUaUtils.convertToOpcValue(typeNodeId, rawValue);
                opcValues.add(new WriteValue(tags.get(tag), AttributeId.Value.uid(), null, DataValue.valueOnly(opcValue)));
            } catch (Exception e) {
                log.warn("Failed to convert '{}' tag's value '{}' to OPC format (OPC type node {})", tag, rawValue, typeNodeId, e);
                results.put(tag, e.getMessage());
                failedTags.add(tag);
            }
        });

        if (!failedTags.isEmpty()) {
            types.keySet().removeAll(failedTags);
        }

        return opcValues;
    }

    private Map<String, String> processWriteResponse(WriteResponse response, Map<String, NodeId> tags) {
        Map<String, String> results = new HashMap<>();

        int i = 0;
        for (String tag : tags.keySet()) {
            StatusCode statusCode = response.getResults()[i++];
            if (statusCode.isGood()) {
                log.debug("OPC write success for tag '{}'", tag);
                results.put(tag, RESPONSE_STATUS_OK);
            } else {
                log.warn("OPC write failed for tag '{}': reason={}", tag, statusCode);
                results.put(tag, "OPC error code: " + statusCode);
            }
        }

        return results;
    }

    private Map<String, NodeId> requestOpcTypes(Map<String, NodeId> tags) {
        Map<String, NodeId> resolvedTypes = new HashMap<>();

        try {
            log.trace("Requesting OPC data type for tags {}", tags.keySet());

            List<ReadValueId> readIds = new LinkedList<>();
            tags.forEach((tag, nodeId) -> readIds.add(
                    new ReadValueId(nodeId, AttributeId.DataType.uid(), null, QualifiedName.NULL_VALUE)));

            ReadResponse readResponse = client.read(0.0, TimestampsToReturn.Neither, readIds).get();

            int readRespIndex = 0;
            for (Map.Entry<String, NodeId> entry : tags.entrySet()) {
                DataValue dv = readResponse.getResults()[readRespIndex++];

                if (dv.getStatusCode().isGood()) {
                    NodeId type = (NodeId) dv.getValue().getValue();
                    log.trace("Got OPC type node for tag '{}': {}", entry.getKey(), type);

                    resolvedTypes.put(entry.getKey(), type);
                } else {
                    log.warn("Failed to request OPC type node for tag '{}': reason={}", entry.getKey(), dv.getStatusCode());
                }
            }
        } catch (Exception e) {
            log.warn("Failed to request OPC data type for tags '{}': ", tags.keySet(), e);
        }

        return resolvedTypes;
    }

    private Map<String, NodeId> resolveNodeIds(OpcUaDevice device, Set<String> tags) {
        Set<String> pendingTags = new HashSet<>();
        Map<String, NodeId> tagsToNodeId = new HashMap<>();

        for (String tag : tags) {
            NodeId tagNodeId = device.getTagNodeId(tag);
            if (tagNodeId == null) {
                pendingTags.add(tag);
            } else {
                tagsToNodeId.put(tag, tagNodeId);
            }
        }

        if (!pendingTags.isEmpty()) {
            log.trace("Looking up OPC server for tags {}", pendingTags);

            OpcUaNode node = device.getOpcNode();
            Map<String, NodeId> resolvedTags = OpcUaUtils.lookupTags(client,
                                                                     node.getNodeId(),
                                                                     node.getName(),
                                                                     pendingTags);
            tagsToNodeId.putAll(resolvedTags);
        }

        return tagsToNodeId;
    }

    private Map<String, String> createTagsErrorResponse(Set<String> tags, String error) {
        Map<String, String> errors = new HashMap<>();
        tags.forEach((tag)->errors.put(tag, error));
        return errors;
    }

    private RpcCommandResponse createErrorResponse(int requestId, String deviceName, String errorDescription) {
        RpcCommandResponse response = new RpcCommandResponse();
        response.setRequestId(requestId);
        response.setDeviceName(deviceName);

        Map<String, String> data = new HashMap<>();
        data.put(RESPONSE_FIELD_ERROR, errorDescription);

        response.setData(JsonTools.toString(data));

        return response;
    }

    private RpcCommandResponse createResponse(int requestId, String deviceName, Map<String, String> data) {
        RpcCommandResponse response = new RpcCommandResponse();
        response.setRequestId(requestId);
        response.setDeviceName(deviceName);
        response.setData(JsonTools.toString(data));

        return response;
    }
}
