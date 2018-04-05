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
package org.thingsboard.gateway.extensions.modbus;

import java.util.List;
import java.util.stream.Collectors;

import org.thingsboard.gateway.extensions.ExtensionUpdate;
import org.thingsboard.gateway.service.conf.TbExtensionConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;
import org.thingsboard.gateway.util.ConfigurationTools;

import lombok.extern.slf4j.Slf4j;
import org.thingsboard.gateway.extensions.modbus.conf.ModbusConfiguration;

@Slf4j
public class DefaultModbusService extends ExtensionUpdate implements ModbusService {

    private final GatewayService gateway;
    private TbExtensionConfiguration currentConfiguration;
    private List<ModbusClient> clients;

    public DefaultModbusService(GatewayService gateway) {
        this.gateway = gateway;
    }

    public TbExtensionConfiguration getCurrentConfiguration() {
        return currentConfiguration;
    }

    public void init(TbExtensionConfiguration configurationNode, Boolean isRemote) throws Exception {
        currentConfiguration = configurationNode;
        log.info("[{}] Initializing Modbus service", gateway.getTenantLabel());
        ModbusConfiguration configuration;
        try {
            if(isRemote) {
                configuration = ConfigurationTools.readConfiguration(configurationNode.getConfiguration(), ModbusConfiguration.class);
            } else {
                configuration = ConfigurationTools.readFileConfiguration(configurationNode.getExtensionConfiguration(), ModbusConfiguration.class);
            }
        } catch (Exception e) {
            log.error("[{}] Modbus service configuration failed", gateway.getTenantLabel(), e);
            gateway.onConfigurationError(e, currentConfiguration);
            throw e;
        }

        log.debug("[{}] Modbus service configuration [{}]", gateway.getTenantLabel(), configuration);

        try {
            clients = configuration.getServers().stream().map(c -> new ModbusClient(gateway, c)).collect(Collectors.toList());
            clients.forEach(ModbusClient::connect);
        } catch (Exception e) {
            log.error("[{}] Modbus service initialization failed", gateway.getTenantLabel(), e);
            gateway.onConfigurationError(e, currentConfiguration);
            throw e;
        }
    }

    public void destroy() {
        if (clients != null) {
            clients.forEach(ModbusClient::disconnect);
        }
    }
}
