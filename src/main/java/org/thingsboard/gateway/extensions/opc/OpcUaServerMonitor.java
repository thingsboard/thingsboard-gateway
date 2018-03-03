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
package org.thingsboard.gateway.extensions.opc;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider;
import org.eclipse.milo.opcua.sdk.client.api.nodes.VariableNode;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.stack.client.UaTcpStackClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.*;
import org.eclipse.milo.opcua.stack.core.types.structured.*;
import org.thingsboard.gateway.extensions.opc.conf.OpcUaServerConfiguration;
import org.thingsboard.gateway.extensions.opc.conf.mapping.DeviceMapping;
import org.thingsboard.gateway.extensions.opc.scan.OpcUaNode;
import org.thingsboard.gateway.service.GatewayService;
import org.thingsboard.gateway.util.CertificateInfo;
import org.thingsboard.gateway.util.ConfigurationTools;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;
import static org.eclipse.milo.opcua.stack.core.util.ConversionUtil.toList;

/**
 * Created by ashvayka on 16.01.17.
 */
@Slf4j
public class OpcUaServerMonitor {

    private final GatewayService gateway;
    private final OpcUaServerConfiguration configuration;

    private OpcUaClient client;
    private UaSubscription subscription;
    private Map<NodeId, OpcUaDevice> devices;
    private Map<NodeId, List<OpcUaDevice>> devicesByTags;
    private Map<Pattern, DeviceMapping> mappings;
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private final AtomicLong clientHandles = new AtomicLong(1L);

    public OpcUaServerMonitor(GatewayService gateway, OpcUaServerConfiguration configuration) {
        this.gateway = gateway;
        this.configuration = configuration;
        this.devices = new HashMap<>();
        this.devicesByTags = new HashMap<>();
        this.mappings = configuration.getMapping().stream().collect(Collectors.toMap(m -> Pattern.compile(m.getDeviceNodePattern()), Function.identity()));
    }

    public void connect() {
        try {
            log.info("Initializing OPC-UA server connection to [{}:{}]!", configuration.getHost(), configuration.getPort());
            CertificateInfo certificate = ConfigurationTools.loadCertificate(configuration.getKeystore());

            SecurityPolicy securityPolicy = SecurityPolicy.valueOf(configuration.getSecurity());
            IdentityProvider identityProvider = configuration.getIdentity().toProvider();

            EndpointDescription[] endpoints = UaTcpStackClient.getEndpoints("opc.tcp://" + configuration.getHost() + ":" + configuration.getPort() + "/").get();

            EndpointDescription endpoint = Arrays.stream(endpoints)
                    .filter(e -> e.getSecurityPolicyUri().equals(securityPolicy.getSecurityPolicyUri()))
                    .findFirst().orElseThrow(() -> new Exception("no desired endpoints returned"));

            OpcUaClientConfig config = OpcUaClientConfig.builder()
                    .setApplicationName(LocalizedText.english(configuration.getApplicationName()))
                    .setApplicationUri(configuration.getApplicationUri())
                    .setCertificate(certificate.getCertificate())
                    .setKeyPair(certificate.getKeyPair())
                    .setEndpoint(endpoint)
                    .setIdentityProvider(identityProvider)
                    .setRequestTimeout(uint(configuration.getTimeoutInMillis()))
                    .build();

            client = new OpcUaClient(config);
            client.connect().get();

            subscription = client.getSubscriptionManager().createSubscription(1000.0).get();

            scanForDevices();
        } catch (Exception e) {
            log.error("OPC-UA server connection failed!", e);
            throw new RuntimeException("OPC-UA server connection failed!", e);
        }
    }

    public void disconnect() {
        if (client != null) {
            log.info("Disconnecting from OPC-UA server!");
            try {
                client.disconnect().get(10, TimeUnit.SECONDS);
                log.info("Disconnected from OPC-UA server!");
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.info("Failed to disconnect from OPC-UA server!");
            }
        }
    }

    public void scanForDevices() {
        try {
            long startTs = System.currentTimeMillis();
            scanForDevices(new OpcUaNode(Identifiers.RootFolder, ""));
            log.info("Device scan cycle completed in {} ms", (System.currentTimeMillis() - startTs));
            List<OpcUaDevice> deleted = devices.entrySet().stream().filter(kv -> kv.getValue().getScanTs() < startTs).map(kv -> kv.getValue()).collect(Collectors.toList());
            if (deleted.size() > 0) {
                log.info("Devices {} are no longer available", deleted);
            }
            deleted.forEach(devices::remove);
            deleted.stream().map(OpcUaDevice::getDeviceName).forEach(gateway::onDeviceDisconnect);
        } catch (Exception e) {
            log.warn("Periodic device scan failed!", e);
        }

        log.info("Scheduling next scan in {} seconds!", configuration.getScanPeriodInSeconds());
        executor.schedule(() -> {
            scanForDevices();
        }, configuration.getScanPeriodInSeconds(), TimeUnit.SECONDS);
    }

    private void scanForDevices(OpcUaNode node) {
        log.trace("Scanning node: {}", node);
        List<DeviceMapping> matchedMappings = mappings.entrySet().stream()
                .filter(mappingEntry -> mappingEntry.getKey().matcher(node.getNodeId().getIdentifier().toString()).matches())
                .map(m -> m.getValue()).collect(Collectors.toList());

        matchedMappings.forEach(m -> {
            try {
                scanDevice(node, m);
            } catch (Exception e) {
                log.error("Failed to scan device: {}", node.getName(), e);
            }
        });

        try {
            BrowseResult browseResult = client.browse(getBrowseDescription(node.getNodeId())).get();
            List<ReferenceDescription> references = toList(browseResult.getReferences());

            for (ReferenceDescription rd : references) {
                NodeId nodeId;
                if (rd.getNodeId().isLocal()) {
                    nodeId = rd.getNodeId().local().get();
                } else {
                    log.trace("Ignoring remote node: {}", rd.getNodeId());
                    continue;
                }
                OpcUaNode childNode = new OpcUaNode(node, nodeId, rd.getBrowseName().getName());

                // recursively browse to children
                scanForDevices(childNode);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Browsing nodeId={} failed: {}", node, e.getMessage(), e);
        }
    }

    private void scanDevice(OpcUaNode node, DeviceMapping m) throws Exception {
        log.debug("Scanning device node: {}", node);
        Set<String> tags = m.getAllTags();
        log.debug("Scanning node hierarchy for tags: {}", tags);
        Map<String, NodeId> tagMap = lookupTags(node.getNodeId(), node.getName(), tags);
        log.debug("Scanned {} tags out of {}", tagMap.size(), tags.size());

        OpcUaDevice device;
        if (devices.containsKey(node.getNodeId())) {
            device = devices.get(node.getNodeId());
        } else {
            device = new OpcUaDevice(node.getNodeId(), m);
            devices.put(node.getNodeId(), device);

            Map<String, NodeId> deviceNameTags = new HashMap<>();
            for (String tag : m.getDeviceNameTags()) {
                NodeId tagNode = tagMap.get(tag);
                if (tagNode == null) {
                    log.error("Not enough info to populate device id for node [{}]. Tag [{}] is missing!", node.getName(), tag);
                    throw new IllegalArgumentException("Not enough info to populate device id. Tag: [" + tag + "] is missing!");
                } else {
                    deviceNameTags.put(tag, tagNode);
                }
            }
            device.calculateDeviceName(readTags(deviceNameTags));
            gateway.onDeviceConnect(device.getDeviceName());
        }

        device.updateScanTs();

        Map<String, NodeId> newTags = device.registerTags(tagMap);
        if (newTags.size() > 0) {
            for (NodeId tagId : newTags.values()) {
                devicesByTags.putIfAbsent(tagId, new ArrayList<>());
                devicesByTags.get(tagId).add(device);
            }
            log.debug("Going to subscribe to tags: {}", newTags);
            subscribeToTags(newTags);
        }
    }

    private void subscribeToTags(Map<String, NodeId> newTags) throws InterruptedException, ExecutionException {
        List<MonitoredItemCreateRequest> requests = new ArrayList<>();
        for (Map.Entry<String, NodeId> kv : newTags.entrySet()) {
            // subscribe to the Value attribute of the server's CurrentTime node
            ReadValueId readValueId = new ReadValueId(
                    kv.getValue(),
                    AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE);
            // important: client handle must be unique per item
            UInteger clientHandle = uint(clientHandles.getAndIncrement());

            MonitoringParameters parameters = new MonitoringParameters(
                    clientHandle,
                    1000.0,     // sampling interval
                    null,       // filter, null means use default
                    uint(10),   // queue size
                    true        // discard oldest
            );

            requests.add(new MonitoredItemCreateRequest(
                    readValueId, MonitoringMode.Reporting, parameters));
        }

        BiConsumer<UaMonitoredItem, Integer> onItemCreated =
                (item, id) -> item.setValueConsumer(this::onSubscriptionValue);

        List<UaMonitoredItem> items = subscription.createMonitoredItems(
                TimestampsToReturn.Both,
                requests,
                onItemCreated
        ).get();

        for (UaMonitoredItem item : items) {
            if (item.getStatusCode().isGood()) {
                log.trace("Monitoring Item created for nodeId={}", item.getReadValueId().getNodeId());
            } else {
                log.warn("Failed to create item for nodeId={} (status={})",
                        item.getReadValueId().getNodeId(), item.getStatusCode());
            }
        }
    }

    private void onSubscriptionValue(UaMonitoredItem item, DataValue dataValue) {
        log.debug("Subscription value received: item={}, value={}",
                item.getReadValueId().getNodeId(), dataValue.getValue());
        NodeId tagId = item.getReadValueId().getNodeId();
        devicesByTags.getOrDefault(tagId, Collections.emptyList()).forEach(
                device -> {
                    device.updateTag(tagId, dataValue);
                    List<KvEntry> attributes = device.getAffectedAttributes(tagId, dataValue);
                    if (attributes.size() > 0) {
                        gateway.onDeviceAttributesUpdate(device.getDeviceName(), attributes);
                    }
                    List<TsKvEntry> timeseries = device.getAffectedTimeseries(tagId, dataValue);
                    if (timeseries.size() > 0) {
                        gateway.onDeviceTelemetry(device.getDeviceName(), timeseries);
                    }
                }
        );
    }

    private Map<String, String> readTags(Map<String, NodeId> tags) throws ExecutionException, InterruptedException {
        Map<String, CompletableFuture<DataValue>> dataFutures = new HashMap<>();
        for (Map.Entry<String, NodeId> kv : tags.entrySet()) {
            VariableNode node = client.getAddressSpace().createVariableNode(kv.getValue());
            dataFutures.put(kv.getKey(), node.readValue());
        }

        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, CompletableFuture<DataValue>> kv : dataFutures.entrySet()) {
            String tag = kv.getKey();
            DataValue value = kv.getValue().get();
            result.put(tag, value.getValue().getValue().toString());
        }
        return result;
    }

    private Map<String, NodeId> lookupTags(NodeId nodeId, String deviceNodeName, Set<String> tags) {
        Map<String, NodeId> values = new HashMap<>();
        try {
            BrowseResult browseResult = client.browse(getBrowseDescription(nodeId)).get();
            List<ReferenceDescription> references = toList(browseResult.getReferences());

            for (ReferenceDescription rd : references) {
                NodeId childId;
                if (rd.getNodeId().isLocal()) {
                    childId = rd.getNodeId().local().get();
                } else {
                    log.trace("Ignoring remote node: {}", rd.getNodeId());
                    continue;
                }

                String browseName = rd.getBrowseName().getName();
                String name;
                String childIdStr = childId.getIdentifier().toString();
                if (childIdStr.contains(deviceNodeName)) {
                    name = childIdStr.substring(childIdStr.indexOf(deviceNodeName) + deviceNodeName.length() + 1, childIdStr.length());
                } else {
                    name = rd.getBrowseName().getName();
                }
                if (tags.contains(name)) {
                    values.put(name, childId);
                }
                // recursively browse to children
                values.putAll(lookupTags(childId, deviceNodeName, tags));
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Browsing nodeId={} failed: {}", nodeId, e.getMessage(), e);
        }
        return values;
    }

    private BrowseDescription getBrowseDescription(NodeId nodeId) {
        return new BrowseDescription(
                nodeId,
                BrowseDirection.Forward,
                Identifiers.References,
                true,
                uint(NodeClass.Object.getValue() | NodeClass.Variable.getValue()),
                uint(BrowseResultMask.All.getValue())
        );
    }
}
