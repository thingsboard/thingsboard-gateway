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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Service;
import org.thingsboard.gateway.extensions.opc.conf.OpcUaConfiguration;
import org.thingsboard.gateway.service.GatewayService;
import org.thingsboard.gateway.util.ConfigurationTools;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.stream.Collectors;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

/**
 * Created by ashvayka on 06.01.17.
 */
@Service
@ConditionalOnProperty(prefix = "opc", value = "enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class DefaultOpcUaService implements OpcUaService {

    @Autowired
    private GatewayService service;

    @Value("${thingsboard.connection.host}")
    private String configurationFile;

    private List<OpcUaServerMonitor> monitors;

    @PostConstruct
    public void init() throws Exception {
        try {
            log.info("Initializing OPC-UA service!");
            OpcUaConfiguration configurations = ConfigurationTools.readConfiguration(configurationFile, OpcUaConfiguration.class);
            monitors = configurations.getServers().stream().map(c -> new OpcUaServerMonitor(service, c)).collect(Collectors.toList());
            monitors.forEach(OpcUaServerMonitor::connect);
        } catch (Exception e) {
            log.error("OPC-UA service initializaion failed!", e);
            throw e;
        }
    }

    @PreDestroy
    public void preDestroy() {
        monitors.forEach(OpcUaServerMonitor::disconnect);
    }
}
