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
package org.thingsboard.opc.service;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider;
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
import org.eclipse.milo.opcua.stack.core.types.structured.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.thingsboard.opc.util.KeyStoreLoader;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;
import static org.eclipse.milo.opcua.stack.core.util.ConversionUtil.toList;

/**
 * Created by ashvayka on 06.01.17.
 */
@Service
@Slf4j
public class OpcUaService {

    @Value("${opc.host}")
    private String host;

    @Value("${opc.port}")
    private Integer port;

    private final KeyStoreLoader loader = new KeyStoreLoader();

    @PostConstruct
    public void init() throws Exception {
        try {
            SecurityPolicy securityPolicy = SecurityPolicy.None;
            IdentityProvider identityProvider = new AnonymousProvider();

            loader.load();

            EndpointDescription[] endpoints = UaTcpStackClient.getEndpoints("opc.tcp://" + host + ":" + port + "/example").get();

            EndpointDescription endpoint = Arrays.stream(endpoints)
                    .filter(e -> e.getSecurityPolicyUri().equals(securityPolicy.getSecurityPolicyUri()))
                    .findFirst().orElseThrow(() -> new Exception("no desired endpoints returned"));

            OpcUaClientConfig config = OpcUaClientConfig.builder()
                    .setApplicationName(LocalizedText.english("eclipse milo opc-ua client"))
                    .setApplicationUri("urn:eclipse:milo:examples:client")
                    .setCertificate(loader.getClientCertificate())
                    .setKeyPair(loader.getClientKeyPair())
                    .setEndpoint(endpoint)
                    .setIdentityProvider(identityProvider)
                    .setRequestTimeout(uint(5000))
                    .build();

            OpcUaClient client = new OpcUaClient(config);

            browseNode("", client, Identifiers.RootFolder);


            client.connect().get();
            log.info("SUCCESS");
        } catch (Exception e) {
            log.error("FAILURE", e);
            throw e;
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
