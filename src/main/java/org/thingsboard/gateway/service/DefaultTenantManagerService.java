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
import org.thingsboard.gateway.extensions.ExtensionService;
import org.thingsboard.gateway.extensions.http.HttpService;
import org.thingsboard.gateway.service.conf.TbExtensionConfiguration;
import org.thingsboard.gateway.service.conf.TbGatewayConfiguration;
import org.thingsboard.gateway.service.conf.TbTenantConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by ashvayka on 29.09.17.
 */
@Slf4j
public abstract class DefaultTenantManagerService implements TenantManagerService {

    @Autowired
    private TbGatewayConfiguration configuration;

    private Map<String, TenantServiceRegistry> gateways;
    private List<HttpService> httpServices;
    private Boolean isRemoteConfiguration;

    private static final String STATUS_STOP = "Stopped";

    public abstract GatewayService getGatewayService(TbTenantConfiguration configuration, Consumer<String> extensionsConfigListener);

    @PostConstruct
    public void init() {
        gateways = new HashMap<>();
        httpServices = new ArrayList<>();
        for (TbTenantConfiguration configuration : configuration.getTenants()) {
            isRemoteConfiguration = configuration.getRemoteConfiguration();
            if (isRemoteConfiguration) {
                String label = configuration.getLabel();
                log.info("[{}] Initializing gateway", configuration.getLabel());
                TenantServiceRegistry tenantServiceRegistry = new TenantServiceRegistry();
                GatewayService service = null;
                try {
                    service = getGatewayService(configuration, c -> tenantServiceRegistry.updateExtensionConfiguration(c));
                    tenantServiceRegistry.setService(service);
                    gateways.put(label, tenantServiceRegistry);
                } catch (Exception e) {
                    log.info("[{}] Failed to initialize the service ", label, e);
                    try {
                        if (service != null) {
                            service.destroy();
                        }
                    } catch (Exception exc) {
                        log.info("[{}] Failed to stop the service ", label, exc);
                    }
                }
            } else {
                String label = configuration.getLabel();
                log.info("[{}] Initializing gateway", configuration.getLabel());
                GatewayService service = null;
                try {
                    TenantServiceRegistry tenantServiceRegistry = new TenantServiceRegistry();
                    service = getGatewayService(configuration, c -> {});
                    tenantServiceRegistry.setService(service);
                    for (TbExtensionConfiguration extensionConfiguration : configuration.getExtensions()) {
                        log.info("[{}] Initializing extension: [{}]", configuration.getLabel(), extensionConfiguration.getType());
                        ExtensionService extension = tenantServiceRegistry.createExtensionServiceByType(service, extensionConfiguration.getType());
                        extension.init(extensionConfiguration, isRemoteConfiguration);
                        if (extensionConfiguration.getType().equals("HTTP")) {
                            httpServices.add((HttpService) extension);
                        }
                    }
                    gateways.put(label, (TenantServiceRegistry) tenantServiceRegistry);
                } catch (Exception e) {
                    log.info("[{}] Failed to initialize the service ", label, e);
                    try {
                        if (service != null) {
                            service.destroy();
                        }
                    } catch (Exception exc) {
                        log.info("[{}] Failed to stop the service ", label, exc);
                    }
                }
            }
        }
    }

    @Override
    public void processRequest(String converterId, String token, String body) throws Exception {
        if (isRemoteConfiguration) {
            for (TenantServiceRegistry tenant : gateways.values()) {
                tenant.processRequest(converterId, token, body);
            }
        } else {
            for (HttpService service : httpServices) {
                service.processRequest(converterId, token, body);
            }
        }
    }

    @PreDestroy
    public void stop() {
        for (String label : gateways.keySet()) {
            try {
                TenantServiceRegistry registry = gateways.get(label);
                for (ExtensionService extension : registry.getExtensions().values()) {
                    try {
                        if (isRemoteConfiguration) {
                            registry.getService().onConfigurationStatus(extension.getCurrentConfiguration().getId(), STATUS_STOP);
                        }
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
