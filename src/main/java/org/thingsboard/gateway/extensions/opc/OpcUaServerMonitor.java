/**
 * Copyright © ${project.inceptionYear}-2017 The Thingsboard Authors
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
package org.thingsboard.gateway.extensions.opc;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider;
import org.eclipse.milo.opcua.stack.client.UaTcpStackClient;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseDirection;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseResultMask;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseResult;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;
import org.thingsboard.gateway.extensions.opc.conf.OpcUaServerConfiguration;
import org.thingsboard.gateway.extensions.opc.conf.mapping.DeviceMapping;
import org.thingsboard.gateway.extensions.opc.scan.OpcUaNode;
import org.thingsboard.gateway.service.GatewayService;
import org.thingsboard.gateway.util.CertificateInfo;
import org.thingsboard.gateway.util.ConfigurationTools;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    private Map<NodeId, OpcUaDevice> devices;
    private Map<Pattern, DeviceMapping> mappings;

    public OpcUaServerMonitor(GatewayService gateway, OpcUaServerConfiguration configuration) {
        this.gateway = gateway;
        this.configuration = configuration;
        this.devices = new HashMap<>();
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
                    .setRequestTimeout(uint(configuration.getTimeoutMs()))
                    .build();

            client = new OpcUaClient(config);

            scanForDevices(new OpcUaNode(Identifiers.RootFolder, ""));

            client.connect().get();
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

    private void scanForDevices(OpcUaNode node) {
        log.trace("Scanning node: {}", node);
        List<DeviceMapping> matchedMappings = mappings.entrySet().stream()
                .filter(mappingEntry -> mappingEntry.getKey().matcher(node.getName()).matches())
                .map(m -> m.getValue()).collect(Collectors.toList());
        matchedMappings.forEach(m -> scanDevice(node, m));

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

    private void scanDevice(OpcUaNode node, DeviceMapping m) {
        log.debug("Scanning device node: {}", node);
        Set<String> tags = m.getTags();
        log.debug("Scanning node hierarchy for tags: {}", tags);
        Map<String, NodeId> tagMap = lookupTags(node.getNodeId(), node.getName(), tags);
        log.debug("Scanned node hierarchy for tags: {}", tagMap);

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
                String name = browseName.substring(deviceNodeName.length() + 1); // 1 is for extra .
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
