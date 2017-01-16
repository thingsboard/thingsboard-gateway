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
package org.thingsboard.gateway.extensions.opc;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider;
import org.eclipse.milo.opcua.sdk.client.api.nodes.VariableNode;
import org.eclipse.milo.opcua.stack.client.UaTcpStackClient;
import org.eclipse.milo.opcua.stack.core.BuiltinDataType;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseDirection;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseResultMask;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowseResult;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;
import org.thingsboard.gateway.extensions.opc.conf.OpcUaServerConfiguration;
import org.thingsboard.gateway.service.GatewayService;
import org.thingsboard.gateway.util.CertificateInfo;
import org.thingsboard.gateway.util.ConfigurationTools;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    public OpcUaServerMonitor(GatewayService gateway, OpcUaServerConfiguration configuration) {
        this.gateway = gateway;
        this.configuration = configuration;
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

            browseNode("", client, Identifiers.RootFolder);

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

    private void browseNode(String indent, OpcUaClient client, NodeId browseRoot) {
        BrowseDescription browse = new BrowseDescription(
                browseRoot,
                BrowseDirection.Forward,
                Identifiers.References,
                true,
                uint(NodeClass.Object.getValue() | NodeClass.Variable.getValue()),
                uint(BrowseResultMask.All.getValue())
        );

        try {
            BrowseResult browseResult = client.browse(browse).get();
            List<ReferenceDescription> references = toList(browseResult.getReferences());

            for (ReferenceDescription rd : references) {
                log.info("{} Node={}", indent, rd.getBrowseName().getName());
                if ("7200.Device1.TestTag".equalsIgnoreCase(rd.getBrowseName().getName())) {
                    if (rd.getNodeClass().equals(NodeClass.Variable)) {
                        if (rd.getNodeId().local().isPresent()) {
                            NodeId nodeId = rd.getNodeId().local().get();
                            VariableNode node = client.getAddressSpace().createVariableNode(nodeId);
                            DataValue value = node.readValue().get();
                            Variant valueVariant = value.getValue();
                            valueVariant.getDataType().ifPresent((NodeId nId) -> {
                                BuiltinDataType.getBackingClass(nId).cast(value.getValue().getValue());
                            });


                            log.info("Value: {}, data type: {}", value.getValue(), value.getValue().getDataType().orElse(null));
                            log.info("OLOLO");
                        }
                    }
                }
                // recursively browse to children
                rd.getNodeId().local().ifPresent(nodeId -> browseNode(indent + "  ", client, nodeId));
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Browsing nodeId={} failed: {}", browseRoot, e.getMessage(), e);
        }
    }
}
