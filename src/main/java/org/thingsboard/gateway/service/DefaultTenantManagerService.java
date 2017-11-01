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
package org.thingsboard.gateway.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.thingsboard.gateway.extensions.ExtensionService;
import org.thingsboard.gateway.service.conf.TbGatewayConfiguration;
import org.thingsboard.gateway.service.conf.TbTenantConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;
import org.thingsboard.gateway.service.gateway.MqttGatewayService;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ashvayka on 29.09.17.
 */
@Service
@Slf4j
public class DefaultTenantManagerService implements TenantManagerService {

    @Autowired
    private TbGatewayConfiguration configuration;

    private Map<String, TenantServicesRegistry> gateways;

    @PostConstruct
    public void init() {
        gateways = new HashMap<>();
        for (TbTenantConfiguration configuration : configuration.getTenants()) {
            String label = configuration.getLabel();
            log.info("[{}] Initializing gateway", configuration.getLabel());
            GatewayService service = new MqttGatewayService(configuration, c -> onExtensionConfigurationUpdate(label, c));
            try {
                service.init();
                gateways.put(label, new TenantServicesRegistry(service));
            } catch (Exception e) {
                log.info("[{}] Failed to initialize the service ", label, e);
                try {
                    service.destroy();
                } catch (Exception e1) {
                    log.info("[{}] Failed to stop the service ", label, e1);
                }
            }
        }
    }

    private void onExtensionConfigurationUpdate(String label, String config) {
        TenantServicesRegistry registry = gateways.get(label);
        log.info("[{}] Updating extension configuration", label);
        registry.updateExtensionConfiguration(config);
    }

    @Override
    public void processRequest(String converterId, String token, String body) throws Exception {
        for (TenantServicesRegistry tenant : gateways.values()) {
            tenant.processRequest(converterId, token, body);
        }
    }

    @PreDestroy
    public void stop() {
        for (String label : gateways.keySet()) {
            try {
                TenantServicesRegistry registry = gateways.get(label);
                for (ExtensionService extension : registry.getExtensions().values()) {
                    try {
                        extension.destroy();
                    } catch (Exception e) {
                        log.info("[{}] Failed to stop the extension ", label, e);
                    }
                }
                registry.getService().destroy();
            } catch (Exception e) {
                log.info("[{}] Failed to stop the service ", label, e);
            }
        }
    }
}
