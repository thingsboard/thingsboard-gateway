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
package org.thingsboard.gateway.extensions.file;

import lombok.extern.slf4j.Slf4j;
import org.thingsboard.gateway.extensions.ExtensionUpdate;
import org.thingsboard.gateway.extensions.file.conf.FileTailConfiguration;
import org.thingsboard.gateway.service.conf.TbExtensionConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;
import org.thingsboard.gateway.util.ConfigurationTools;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by ashvayka on 15.05.17.
 */
@Slf4j
public class DefaultFileTailService extends ExtensionUpdate {

    private final GatewayService gateway;
    private TbExtensionConfiguration currentConfiguration;
    private List<FileMonitor> brokers;

    public DefaultFileTailService(GatewayService gateway) {
        this.gateway = gateway;
    }

    @Override
    public TbExtensionConfiguration getCurrentConfiguration() {
        return currentConfiguration;
    }

    @Override
    public void init(TbExtensionConfiguration configurationNode) throws Exception {
        currentConfiguration = configurationNode;
        log.info("[{}] Initializing File Tail service!", gateway.getTenantLabel());
        FileTailConfiguration configuration;
        try {
            configuration = ConfigurationTools.readConfiguration(configurationNode.getConfiguration(), FileTailConfiguration.class);
        } catch (Exception e) {
            log.error("[{}] File Tail service configuration failed!", gateway.getTenantLabel(), e);
            gateway.onConfigurationError(e, currentConfiguration);
            throw e;
        }

        try {
            brokers = configuration.getFileMonitorConfigurations().stream().map(c -> new FileMonitor(gateway, c)).collect(Collectors.toList());
            brokers.forEach(FileMonitor::init);
        } catch (Exception e) {
            log.error("[{}] File Tail service initialization failed!", gateway.getTenantLabel(), e);
            gateway.onConfigurationError(e, currentConfiguration);
            throw e;
        }
    }

    @Override
    public void destroy() {
        if (brokers != null) {
            brokers.forEach(FileMonitor::stop);
        }
    }

}
