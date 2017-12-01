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
import org.thingsboard.gateway.extensions.ExtensionUpdate;
import org.thingsboard.gateway.extensions.opc.conf.OpcUaConfiguration;
import org.thingsboard.gateway.service.conf.TbExtensionConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;
import org.thingsboard.gateway.util.ConfigurationTools;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by ashvayka on 06.01.17.
 */
@Slf4j
public class DefaultOpcUaService extends ExtensionUpdate implements OpcUaService {

    private final GatewayService gateway;
    private TbExtensionConfiguration currentConfiguration;
    private List<OpcUaServerMonitor> monitors;

    public DefaultOpcUaService(GatewayService gateway) {
        this.gateway = gateway;
    }

    @Override
    public TbExtensionConfiguration getCurrentConfiguration() {
        return currentConfiguration;
    }

    @Override
    public void init(TbExtensionConfiguration configurationNode) throws Exception {
        currentConfiguration = configurationNode;
        log.info("Initializing OPC-UA service!", gateway.getTenantLabel());
        OpcUaConfiguration configuration;
        try {
            configuration = ConfigurationTools.readConfiguration(configurationNode.getConfiguration(), OpcUaConfiguration.class);
        } catch (Exception e) {
            log.error("OPC-UA service configuration failed!", gateway.getTenantLabel(), e);
            gateway.onConfigurationError(e, currentConfiguration);
            throw e;
        }

        try {
            monitors = configuration.getServers().stream().map(c -> new OpcUaServerMonitor(gateway, c)).collect(Collectors.toList());
            monitors.forEach(OpcUaServerMonitor::connect);
        } catch (Exception e) {
            log.error("OPC-UA service initialization failed!", gateway.getTenantLabel(), e);
            gateway.onConfigurationError(e, currentConfiguration);
            throw e;
        }
    }

    @Override
    public void destroy() {
        if (monitors != null) {
            monitors.forEach(OpcUaServerMonitor::disconnect);
        }
    }
}
